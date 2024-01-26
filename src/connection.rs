use std::collections::HashMap;

use futures::{lock::Mutex, SinkExt, StreamExt};
use protobuf::Message;
use url::Url;

use crate::{
    message::{
        proto::pulsar::{self, BaseCommand, CommandSendReceipt, MessageIdData},
        SendReceipt,
    },
    PulsarConfig,
};

pub enum PulsarConnectionError {
    Disconnected,
    Timeout,
}

pub struct PulsarConnection {
    send_tx: async_channel::Sender<pulsar::BaseCommand>,
    recv_rx: async_channel::Receiver<pulsar::BaseCommand>,
}

impl PulsarConnection {
    pub async fn connect(host: &String, port: &u16) -> Self {
        let url = Url::parse(&format!("ws://{}:{}", host, port)).unwrap();

        let (send_tx, send_rx) = async_channel::unbounded::<pulsar::BaseCommand>();
        let (recv_tx, recv_rx) = async_channel::unbounded::<pulsar::BaseCommand>();

        // let (terminate_sink_tx, terminate_sink_rx) = futures::channel::oneshot::channel();
        // let (terminate_stream_tx, terminate_stream_rx) = futures::channel::oneshot::channel();

        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        let (mut sink, stream) = ws_stream.split();

        tokio::spawn(async move {
            stream
                .for_each(|msg| async {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes)) => {
                            recv_tx
                                .send(
                                    pulsar::BaseCommand::parse_from_bytes(bytes.as_slice())
                                        .unwrap(),
                                )
                                .await
                                .unwrap();
                        }
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                })
                .await;
        });

        tokio::spawn(async move {
            while let Ok(msg) = send_rx.recv().await {
                sink.send(tokio_tungstenite::tungstenite::Message::binary(
                    msg.write_to_bytes().unwrap(),
                ))
                .await
                .unwrap();
            }
        });

        PulsarConnection { send_tx, recv_rx }
    }

    pub async fn send(&self, msg: pulsar::BaseCommand) -> Result<(), PulsarConnectionError> {
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| PulsarConnectionError::Disconnected)
    }

    pub async fn recv(&self) -> Result<pulsar::BaseCommand, PulsarConnectionError> {
        self.recv_rx
            .recv()
            .await
            .map_err(|_| PulsarConnectionError::Disconnected)
    }
}

pub struct ReceiptManager {
    map: Mutex<HashMap<MessageIdData, futures::channel::oneshot::Sender<CommandSendReceipt>>>,
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
        tx: futures::channel::oneshot::Sender<CommandSendReceipt>,
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
    ) -> Option<futures::channel::oneshot::Sender<CommandSendReceipt>> {
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
        Ok(())
    }

    pub async fn send(&self, base_command: BaseCommand) -> Result<(), PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            return connection
                .send(base_command)
                .await
                .map_err(|_| PulsarConnectionError::Disconnected)
                .map(|_| ());
        }
        panic!("Not connected");
    }

    pub async fn send_with_receipt(
        &self,
        base_command: BaseCommand,
    ) -> Result<SendReceipt, PulsarConnectionError> {
        if let Some(send_cmd) = base_command.send.clone().into_option() {
            let message_id = send_cmd.message_id.clone().unwrap();
            let (send_receipt, receipt_handle) = SendReceipt::create_pair(&message_id);

            self.receipt_manager
                .put_receipt(&message_id, receipt_handle)
                .await;

            if let Some(connection) = &self.connection {
                connection
                    .send(base_command)
                    .await
                    .map_err(|_| PulsarConnectionError::Disconnected)?;
            }

            return Ok(send_receipt);
        }
        panic!("Not a send command");
    }

    pub async fn recv(&self) -> Result<pulsar::BaseCommand, PulsarConnectionError> {
        if let Some(connection) = &self.connection {
            return match connection.recv().await {
                Ok(cmd) => {
                    if let Some(send_receipt_cmd) = cmd.send_receipt.clone().into_option() {
                        let message_id = send_receipt_cmd.message_id.clone().unwrap();
                        let receipt_handle = self.receipt_manager.get_receipt(&message_id).await;
                        if let Some(handle) = receipt_handle {
                            let _ = handle.send(cmd.send_receipt.clone().unwrap());
                        };
                    }
                    Ok(cmd)
                }
                Err(_) => Err(PulsarConnectionError::Disconnected),
            };
        }
        panic!("Not connected");
    }
}
