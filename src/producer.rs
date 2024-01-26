use async_trait::async_trait;
use serde::Serialize;

pub enum PulsarProducerError {
    PulsarError(String),
    SerializationError(String),
}

#[async_trait]
pub trait PulsarProducer<T>
where
    T: Serialize + Send + Sync + 'static,
{
    async fn close(&self) -> Result<(), PulsarProducerError>;
    async fn send(&self, msg: T) -> Result<(), PulsarProducerError>;
}
