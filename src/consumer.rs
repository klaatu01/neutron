use std::fmt::{Display, Formatter};

use crate::message::ClientInbound;
use async_trait::async_trait;

#[cfg(feature = "json")]
use serde::de::DeserializeOwned;

use crate::message::proto::pulsar::MessageIdData;

#[derive(Debug)]
pub enum PulsarConsumerError {
    PulsarError(String),
    DeserializationError(String),
}

impl std::error::Error for PulsarConsumerError {}

impl Display for PulsarConsumerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PulsarConsumerError::PulsarError(msg) => write!(f, "PulsarError: {}", msg),
            PulsarConsumerError::DeserializationError(msg) => {
                write!(f, "DeserializationError: {}", msg)
            }
        }
    }
}

#[async_trait]
pub trait PulsarConsumer {
    async fn close(&self) -> Result<(), PulsarConsumerError>;
    async fn ack(&self, message_id: &MessageIdData) -> Result<(), PulsarConsumerError>;
}

#[cfg(not(feature = "json"))]
#[async_trait]
pub trait PulsarConsumerNext<T: TryFrom<Vec<u8>>> {
    async fn next(&self) -> Result<Message<T>, PulsarConsumerError>;
}

#[cfg(feature = "json")]
#[async_trait]
pub trait PulsarConsumerNext<T: DeserializeOwned> {
    async fn next(&self) -> Result<Message<T>, PulsarConsumerError>;
}

pub struct ConsumerConfig {
    pub consumer_name: String,
    pub consumer_id: String,
    pub topic: String,
    pub subscription: String,
}

#[allow(dead_code)]
pub struct Consumer {
    client: crate::Pulsar,
    config: ConsumerConfig,
}

impl Consumer {
    pub fn new(client: crate::Pulsar, config: ConsumerConfig) -> Self {
        Consumer { client, config }
    }

    pub async fn connect(mut self) -> Result<Self, PulsarConsumerError> {
        self.client
            .connect()
            .await
            .map_err(|_| PulsarConsumerError::PulsarError("Failed to connect".to_string()))?;
        Ok(self)
    }
}

pub struct Message<T> {
    message_id: MessageIdData,
    payload: T,
}

impl<T> Message<T> {
    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn message_id(&self) -> &MessageIdData {
        &self.message_id
    }
}

#[async_trait]
impl PulsarConsumer for Consumer {
    async fn close(&self) -> Result<(), PulsarConsumerError> {
        Ok(())
    }

    async fn ack(&self, message_id: &MessageIdData) -> Result<(), PulsarConsumerError> {
        self.client
            .ack(message_id)
            .await
            .map_err(|_| PulsarConsumerError::PulsarError("Failed to ack message".to_string()))?;
        Ok(())
    }
}

#[cfg(not(feature = "json"))]
#[async_trait]
impl<T: TryFrom<Vec<u8>>> PulsarConsumerNext<T> for Consumer {
    async fn next(&self) -> Result<Message<T>, PulsarConsumerError> {
        loop {
            let msg = self.client.next_message().await.map_err(|_| {
                PulsarConsumerError::PulsarError("Failed to receive message".to_string())
            })?;

            #[allow(irrefutable_let_patterns)]
            if let ClientInbound::Message {
                message_id,
                payload,
            } = msg
            {
                let payload = T::try_from(payload).map_err(|_| {
                    PulsarConsumerError::DeserializationError(
                        "Failed to deserialize message".to_string(),
                    )
                })?;
                return Ok(Message::<T> {
                    message_id,
                    payload,
                });
            }
        }
    }
}

#[cfg(feature = "json")]
#[async_trait]
impl<T: DeserializeOwned> PulsarConsumerNext<T> for Consumer {
    async fn next(&self) -> Result<Message<T>, PulsarConsumerError> {
        loop {
            let msg = self.client.next_message().await.map_err(|_| {
                PulsarConsumerError::PulsarError("Failed to receive message".to_string())
            })?;
            if let ClientInbound::Message {
                message_id,
                payload,
            } = msg
            {
                let payload = serde_json::from_slice(&payload).map_err(|_| {
                    PulsarConsumerError::DeserializationError(
                        "Failed to deserialize message".to_string(),
                    )
                })?;
                return Ok(Message::<T> {
                    message_id,
                    payload,
                });
            }
        }
    }
}
