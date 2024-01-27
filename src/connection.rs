use futures::lock::Mutex;
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    message::{
        proto::pulsar::{base_command, MessageIdData},
        ClientCommand, Message, SendReceipt, ServerMessage,
    },
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
    send_tx: async_channel::Sender<Message>,
    recv_rx: async_channel::Receiver<Message>,
}

impl PulsarConnection {
    pub async fn connect(host: &String, port: &u16) -> Self {
        let addr = format!("{}:{}", host, port);
        println!("Connecting to {}", addr);
        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        let (mut stream, mut sink) = tokio::io::split(tcp_stream);

        let (send_tx, send_rx) = async_channel::unbounded::<Message>();
        let (recv_tx, recv_rx) = async_channel::unbounded::<Message>();

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
                            if msg.command.type_() == base_command::Type::PING {
                                stream_send_tx
                                    .send(ClientCommand::Pong.into())
                                    .await
                                    .unwrap();
                                continue;
                            };
                            recv_tx.send(msg).await.unwrap();
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
                println!("-> {:?}", msg.command.type_());
                let bytes: Vec<u8> = msg.into();
                if let Err(e) = sink.write_all(&bytes).await {
                    println!("Error while writing to socket: {}", e);
                    break;
                }
            }
        });

        PulsarConnection { send_tx, recv_rx }
    }

    pub async fn send(&self, msg: Message) -> Result<(), PulsarConnectionError> {
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| PulsarConnectionError::Disconnected)
    }

    pub async fn recv(&self) -> Result<Message, PulsarConnectionError> {
        self.recv_rx
            .recv()
            .await
            .map_err(|_| PulsarConnectionError::Disconnected)
    }
}

pub struct ReceiptManager {
    map: Mutex<HashMap<MessageIdData, futures::channel::oneshot::Sender<()>>>,
}

impl ReceiptManager {
    pub fn new() -> Self {
        ReceiptManager {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn put_receipt(
        &self,
        message_id: &MessageIdData,
        tx: futures::channel::oneshot::Sender<()>,
    ) {
        self.map
            .lock()
            .await
            .insert(message_id.clone(), tx)
            .unwrap();
    }

    pub async fn get_receipt(
        &self,
        message_id: &MessageIdData,
    ) -> Option<futures::channel::oneshot::Sender<()>> {
        self.map.lock().await.remove(&message_id)
    }
}

pub struct PulsarConnectionManager {
    config: PulsarConfig,
    connection: Option<PulsarConnection>,
    receipt_manager: ReceiptManager,
}

impl PulsarConnectionManager {
    pub fn new(config: &PulsarConfig) -> Self {
        PulsarConnectionManager {
            config: config.clone(),
            connection: None,
            receipt_manager: ReceiptManager::new(),
        }
    }

    pub async fn connect(&mut self) -> Result<(), PulsarConnectionError> {
        let connection =
            PulsarConnection::connect(&self.config.endpoint_url, &self.config.endpoint_port).await;
        self.connection = Some(connection);

        let start = std::time::Instant::now();
        self.send(ClientCommand::Connect { auth_data: None }.into())
            .await
            .map_err(|_| PulsarConnectionError::Timeout)?;

        let message = self
            .recv()
            .await
            .map_err(|_| PulsarConnectionError::Timeout)?;

        if let ServerMessage::Connected = message {
            println!("Connected in {}ms", start.elapsed().as_millis());
            return Ok(());
        } else {
            return Err(PulsarConnectionError::UnsupportedCommand);
        }
    }

    pub async fn send(&self, message: ClientCommand) -> Result<(), PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            return connection
                .send(message.into())
                .await
                .map_err(|_| PulsarConnectionError::Disconnected)
                .map(|_| ());
        }
        panic!("Not connected");
    }

    pub async fn send_with_receipt(
        &self,
        message: Message,
    ) -> Result<SendReceipt, PulsarConnectionError> {
        if let Some(send_cmd) = message.command.send.clone().into_option() {
            let message_id = send_cmd.message_id.clone().unwrap();
            let (send_receipt, receipt_handle) = SendReceipt::create_pair(&message_id);

            self.receipt_manager
                .put_receipt(&message_id, receipt_handle)
                .await;

            if let Some(connection) = &self.connection {
                connection
                    .send(message)
                    .await
                    .map_err(|_| PulsarConnectionError::Disconnected)?;
            }

            return Ok(send_receipt);
        }
        panic!("Not a send command");
    }

    pub async fn recv(&self) -> Result<ServerMessage, PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            let message: ServerMessage = connection
                .recv()
                .await
                .map_err(|_| PulsarConnectionError::Disconnected)?
                .try_into()
                .map_err(|_| PulsarConnectionError::UnsupportedCommand)?;

            match &message {
                ServerMessage::SendReceipt { message_id } => {
                    let receipt_handle = self.receipt_manager.get_receipt(&message_id).await;
                    if let Some(handle) = receipt_handle {
                        let _ = handle.send(());
                    };
                }
                ServerMessage::Ping => {
                    self.send(ClientCommand::Pong.into())
                        .await
                        .map_err(|_| PulsarConnectionError::Timeout)?;
                }
                _ => {}
            }
            Ok(message)
        } else {
            panic!("Not connected");
        }
    }
}
