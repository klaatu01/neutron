use crate::message::ServerMessage;
use async_trait::async_trait;

#[cfg(feature = "json")]
use serde::de::DeserializeOwned;

use crate::message::proto::pulsar::MessageIdData;

pub enum PulsarConsumerError {
    PulsarError(String),
    DeserializationError(String),
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
            if let ServerMessage::Message {
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
            if let ServerMessage::Message {
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
