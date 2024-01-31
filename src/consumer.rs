use crate::{
    engine::{Engine, EngineConnection},
    message::{Inbound, Outbound},
    PulsarConfig,
};
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::error::NeutronError;

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerConfig {
    pub consumer_name: String,
    pub consumer_id: u64,
    pub topic: String,
    pub subscription: String,
}

#[allow(dead_code)]
pub struct Consumer {
    config: ConsumerConfig,
    pulsar_config: PulsarConfig,
    client_engine_connection: Option<EngineConnection<Outbound, Inbound>>,
}

impl Consumer {
    pub fn new(pulsar_config: PulsarConfig, consumer_config: ConsumerConfig) -> Self {
        Self {
            config: consumer_config,
            pulsar_config,
            client_engine_connection: None,
        }
    }

    pub async fn connect(mut self) -> Result<Self, NeutronError> {
        let client_engine_connection = crate::Pulsar::new(self.pulsar_config.clone()).run().await;
        Ok(Consumer {
            config: self.config,
            pulsar_config: self.pulsar_config,
            client_engine_connection: Some(client_engine_connection),
        })
    }

    pub async fn next(&self) -> Result<Inbound, NeutronError> {
        match &self.client_engine_connection {
            Some(engine_connection) => {
                let inbound = engine_connection.recv().await;
                match inbound {
                    Ok(inbound) => {
                        log::debug!("Received inbound: {:?}", inbound);
                        return Ok(inbound);
                    }
                    Err(e) => {
                        log::warn!("{}", e);
                        return Err(e);
                    }
                }
            }
            None => {
                log::error!("Disconnected");
                Err(NeutronError::Disconnected)
            }
        }
    }
}
