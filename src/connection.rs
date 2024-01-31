use futures::Future;
use futures::{lock::Mutex, FutureExt};
use std::collections::HashMap;
use std::f32::consts::E;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::{
    message::{
        proto::pulsar::MessageIdData, ClientInbound, ClientOutbound, ConnectionInbound,
        ConnectionOutbound, EngineInbound, EngineOutbound, Inbound, Message, Outbound, SendReceipt,
    },
    resolver_manager::{Resolvable, ResolverManager},
    PulsarConfig,
};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub enum PulsarConnectionError {
    Disconnected,
    Timeout,
    UnsupportedCommand,
    Decode,
}

impl std::fmt::Display for PulsarConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PulsarConnectionError::Disconnected => write!(f, "Disconnected"),
            PulsarConnectionError::Timeout => write!(f, "Timeout"),
            PulsarConnectionError::UnsupportedCommand => write!(f, "Unsupported command"),
            PulsarConnectionError::Decode => write!(f, "Decode error"),
        }
    }
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

impl std::error::Error for PulsarConnectionError {}

pub struct PulsarConnection {
    sink: WriteHalf<TcpStream>,
    stream: ReadHalf<TcpStream>,
}

impl PulsarConnection {
    pub async fn connect(host: &String, port: &u16) -> Self {
        let addr = format!("{}:{}", host, port);
        log::debug!("Opening connection to {}", addr);
        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        log::debug!("Connection opened");
        let (stream, sink) = tokio::io::split(tcp_stream);

        Self { sink, stream }
    }

    pub async fn start_connection(
        &mut self,
        client_connection: EngineConnection<Inbound, Outbound>,
    ) {
        let mut buf = Vec::new();
        loop {
            buf.clear();
            tokio::select! {
                outbound = client_connection.recv() => {
                    match outbound {
                        Ok(outbound) => {
                            let msg: Message = outbound.into();
                            let bytes: Vec<u8> = msg.into();
                            let _ = self
                                .sink
                                .write_all(&bytes).await;
                        }
                        Err(e) => {
                            log::warn!("Error: {}", &e);
                        }
                    }
                },
                bytes = self.stream.read_buf(&mut buf) => {
                    let inbound = match bytes {
                        Ok(0) => {
                            log::warn!("Connection closed");
                            Err(NeutronError::Disconnected)
                        }
                        Ok(_) => Message::try_from(&buf)
                            .map_err(|_| NeutronError::DecodeFailed)
                            .and_then(|msg| {
                                Inbound::try_from(&msg)
                                    .map_err(|_| NeutronError::UnsupportedCommand)
                            }),
                        Err(e) => {
                            log::warn!("Error: {}", e);
                            Err(NeutronError::DecodeFailed)
                        }
                    };

                    if let Err(_) = client_connection.send(inbound.clone()).await {
                        break;
                    }

                    match &inbound {
                        Ok(inbound) => {
                            log::debug!("{}", inbound.to_string());
                        }
                        Err(NeutronError::Disconnected) => {
                            log::warn!("Disconnected");
                            break;
                        }
                        Err(e) => {
                            log::warn!("Error: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Engine<Inbound, Outbound> for PulsarConnection {
    async fn run(mut self) -> EngineConnection<Outbound, Inbound> {
        let (tx, _rx) = async_channel::unbounded::<ResultInbound>();
        let (_tx, rx) = async_channel::unbounded::<ResultOutbound>();

        let client_connection = EngineConnection::new(tx, rx);

        tokio::task::spawn(async move {
            self.start_connection(client_connection).await;
        });

        EngineConnection::new(_tx, _rx)
    }
}
