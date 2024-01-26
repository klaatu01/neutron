use async_trait::async_trait;
use serde::de::DeserializeOwned;

pub enum PulsarConsumerError {
    PulsarError(String),
    DeserializationError(String),
}

#[async_trait]
pub trait PulsarConsumer<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    async fn close(&self) -> Result<(), PulsarConsumerError>;
    async fn next(&self) -> Result<T, PulsarConsumerError>;
}
