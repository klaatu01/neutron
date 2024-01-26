use crate::{auth::PulsarAuthentication, connection};
use async_trait::async_trait;

pub enum PulsarClientError {
    PulsarError(String),
}

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
    pub(crate) config: PulsarConfig,
    pub(crate) connection_manager: connection::PulsarConnectionManager,
}

impl Pulsar {
    pub fn new(config: PulsarConfig) -> Self {
        let connection_manager = connection::PulsarConnectionManager::new(&config);

        Pulsar {
            config,
            connection_manager,
        }
    }

    async fn connect(&mut self) -> Result<(), PulsarClientError> {
        self.connection_manager
            .connect()
            .await
            .map_err(|_| PulsarClientError::PulsarError("Failed to connect".to_string()))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), PulsarClientError> {
        Ok(())
    }

    async fn send_message(&self) -> Result<(), PulsarClientError> {
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
