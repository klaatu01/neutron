use crate::{
    connection,
    message::{proto::pulsar::MessageIdData, ClientInbound, ClientOutbound},
};
use async_trait::async_trait;

#[derive(Debug)]
pub enum PulsarClientError {
    PulsarError(String),
}

impl std::fmt::Display for PulsarClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PulsarClientError::PulsarError(msg) => write!(f, "Pulsar Error: {}", msg),
        }
    }
}

impl std::error::Error for PulsarClientError {}

#[async_trait]
pub trait PulsarClient {
    async fn run(&self) -> Result<(), PulsarClientError>;
}

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
    // pub authentication: Option<Box<dyn PulsarAuthentication>>,
}

pub struct Pulsar {
    #[allow(dead_code)]
    pub(crate) config: PulsarConfig,
    pub(crate) connection_manager: connection::PulsarConnectionManager,
}

pub enum CloseClient {
    Consumer,
    Producer,
}

#[allow(dead_code)]
impl Pulsar {
    pub fn new(config: PulsarConfig) -> Self {
        let connection_manager = connection::PulsarConnectionManager::new(&config);

        Pulsar {
            config,
            connection_manager,
        }
    }

    pub(crate) async fn connect(&mut self) -> Result<(), PulsarClientError> {
        self.connection_manager
            .connect()
            .await
            .map_err(|_| PulsarClientError::PulsarError("Failed to connect".to_string()))?;
        Ok(())
    }

    async fn close(&self, _client_type: CloseClient) -> Result<(), PulsarClientError> {
        // match on client type and send close message accordingly
        Ok(())
    }

    pub(crate) fn next_message_id(&self) -> MessageIdData {
        unimplemented!();
    }

    pub(crate) async fn send_message(
        &self,
        producer_id: u64,
        sequence_id: u64,
        payload: Vec<u8>,
    ) -> Result<(), PulsarClientError> {
        let message_id = self.next_message_id();
        let message = ClientOutbound::Send {
            message_id,
            payload,
            producer_id,
            sequence_id,
        };
        self.connection_manager
            .send(message.into())
            .await
            .map_err(|_| PulsarClientError::PulsarError("Failed to send message".to_string()))?;

        Ok(())
    }

    pub async fn next_message(&self) -> Result<ClientInbound, PulsarClientError> {
        let message: ClientInbound =
            self.connection_manager.recv().await.map_err(|_| {
                PulsarClientError::PulsarError("Failed to receive message".to_string())
            })?;
        Ok(message)
    }

    pub async fn ack(&self, message_id: &MessageIdData) -> Result<(), PulsarClientError> {
        let message = ClientOutbound::Ack(vec![message_id.clone()]);
        self.connection_manager
            .send(message.into())
            .await
            .map_err(|_| PulsarClientError::PulsarError("Failed to send ack".to_string()))?;
        Ok(())
    }

    // perform a ping
    async fn check_connection(&self) -> Result<(), PulsarClientError> {
        //        self.connection_manager
        //            .send_with_receipt()
        //            .await?
        //            .wait_for_receipt()
        //            .await?;
        Ok(())
    }
}
