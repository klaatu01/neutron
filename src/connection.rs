use futures::Future;
use futures::{lock::Mutex, FutureExt};
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    message::{
        proto::pulsar::MessageIdData, ClientInbound, ClientOutbound, ConnectionInbound,
        ConnectionOutbound, EngineInbound, EngineOutbound, Inbound, Message, Outbound, SendReceipt,
    },
    resolver_manager::{Resolvable, ResolverManager},
    PulsarConfig,
};

#[derive(Debug, Clone)]
pub enum PulsarConnectionError {
    Disconnected,
    Timeout,
    UnsupportedCommand,
}

impl std::fmt::Display for PulsarConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PulsarConnectionError::Disconnected => write!(f, "Disconnected"),
            PulsarConnectionError::Timeout => write!(f, "Timeout"),
            PulsarConnectionError::UnsupportedCommand => write!(f, "Unsupported command"),
        }
    }
}

impl std::error::Error for PulsarConnectionError {}

pub struct PulsarConnection {
    send_tx: async_channel::Sender<Outbound>,
    recv_rx: async_channel::Receiver<Inbound>,
}

impl PulsarConnection {
    pub async fn connect(host: &String, port: &u16) -> Self {
        let addr = format!("{}:{}", host, port);
        println!("Connecting to {}", addr);
        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        let (mut stream, mut sink) = tokio::io::split(tcp_stream);

        let (send_tx, send_rx) = async_channel::unbounded::<Outbound>();
        let (recv_tx, recv_rx) = async_channel::unbounded::<Inbound>();

        let stream_send_tx = send_tx.clone();
        tokio::spawn(async move {
            let mut buf = Vec::new();
            loop {
                buf.clear();
                match stream.read_buf(&mut buf).await {
                    Ok(0) => {
                        println!("Socket closed");
                        break;
                    }
                    Ok(_) => match Message::try_from(&buf) {
                        Ok(msg) => {
                            let inbound = Inbound::try_from(&msg);
                            match inbound {
                                Ok(Inbound::Connection(e)) => {
                                    println!("<- {:?}", e.base_command());
                                    Self::handle_connection_inbound(e, &stream_send_tx)
                                        .await
                                        .unwrap();
                                }
                                Ok(cmd) => {
                                    println!("{}", cmd.to_string());
                                    recv_tx.send(cmd).await.unwrap();
                                }
                                Err(e) => {
                                    println!("Error while parsing message: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error while parsing message: {}", e);
                        }
                    },
                    Err(e) => {
                        println!("Error while reading from socket: {}", e);
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            while let Ok(msg) = send_rx.recv().await {
                println!("{}", msg.to_string());
                let msg: Message = msg.into();
                let bytes: Vec<u8> = msg.into();
                if let Err(e) = sink.write_all(&bytes).await {
                    println!("Error while writing to socket: {}", e);
                    break;
                }
            }
        });

        PulsarConnection { send_tx, recv_rx }
    }

    async fn handle_connection_inbound(
        message: ConnectionInbound,
        send_tx: &async_channel::Sender<Outbound>,
    ) -> Result<(), PulsarConnectionError> {
        match message {
            ConnectionInbound::Ping => {
                send_tx.send(ConnectionOutbound::Pong.into()).await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn send(&self, msg: Outbound) -> Result<(), PulsarConnectionError> {
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| PulsarConnectionError::Disconnected)
    }

    pub async fn recv(&self) -> Result<Inbound, PulsarConnectionError> {
        self.recv_rx
            .recv()
            .await
            .map_err(|_| PulsarConnectionError::Disconnected)
    }
}

pub struct PulsarConnectionManager {
    config: PulsarConfig,
    connection: Option<PulsarConnection>,
    resolver_manager: ResolverManager<Inbound>,
}

impl PulsarConnectionManager {
    pub fn new(config: &PulsarConfig) -> Self {
        PulsarConnectionManager {
            config: config.clone(),
            connection: None,
            resolver_manager: ResolverManager::new(),
        }
    }

    pub async fn connect(&mut self) -> Result<(), PulsarConnectionError> {
        let connection =
            PulsarConnection::connect(&self.config.endpoint_url, &self.config.endpoint_port).await;
        self.connection = Some(connection);

        let start = std::time::Instant::now();
        let inbound = self
            .send_and_resolve(EngineOutbound::Connect { auth_data: None }.into())
            .await?;

        if let Inbound::Engine(EngineInbound::Connected) = inbound {
            println!("Connected in {}ms", start.elapsed().as_millis());
            return Ok(());
        } else {
            return Err(PulsarConnectionError::UnsupportedCommand);
        }
    }

    pub async fn send(&self, message: Outbound) -> Result<(), PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            return connection
                .send(message.into())
                .await
                .map_err(|_| PulsarConnectionError::Disconnected)
                .map(|_| ());
        }
        panic!("Not connected");
    }

    pub async fn send_and_resolve(
        &self,
        outbound: Outbound,
    ) -> Result<Inbound, PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            return if let Some(resolver) = self.resolver_manager.put_resolver(&outbound).await {
                connection
                    .send(outbound.into())
                    .await
                    .map_err(|_| PulsarConnectionError::Disconnected)?;
                let inbound = resolver.await.map_err(|_| PulsarConnectionError::Timeout)?;
                Ok(inbound)
            } else {
                Err(PulsarConnectionError::Timeout)
            };
        }
        Err(PulsarConnectionError::Disconnected)
    }

    async fn handle_engine_inbound(
        &self,
        message: EngineInbound,
    ) -> Result<(), PulsarConnectionError> {
        match message {
            _ => {}
        }
        Ok(())
    }

    async fn next_inbound(&self) -> Result<Inbound, PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            let inbound: Inbound = connection
                .recv()
                .await
                .map_err(|_| PulsarConnectionError::Disconnected)?;
            Ok(inbound)
        } else {
            panic!("Not connected");
        }
    }

    pub async fn recv(&self) -> Result<ClientInbound, PulsarConnectionError> {
        loop {
            if let Some(connection) = &self.connection {
                let inbound: Inbound = connection
                    .recv()
                    .await
                    .map_err(|_| PulsarConnectionError::Disconnected)?;

                match inbound {
                    Inbound::Engine(engine_inbound) => {
                        self.handle_engine_inbound(engine_inbound.clone()).await?;
                    }
                    Inbound::Client(client_inbound) => {
                        return Ok(client_inbound);
                    }
                    _ => {}
                }
            } else {
                panic!("Not connected");
            }
        }
    }
}
