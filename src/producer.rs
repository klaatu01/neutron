use crate::{
    client::Client,
    message::{Inbound, Outbound},
    PulsarManager,
};
#[cfg(feature = "json")]
use serde::ser::Serialize;
use serde::Deserialize;

use crate::error::NeutronError;

#[cfg(feature = "json")]
pub trait ProducerDataTrait: Serialize + Send + Sync + Clone {}

#[cfg(not(feature = "json"))]
pub trait ProducerDataTrait: Into<Vec<u8>> + Send + Sync + Clone {}

// Implement the trait for all types that satisfy the trait bounds.
// This is a simplified example; for real-world usage, you might need to adjust it.
#[cfg(feature = "json")]
impl<T: Serialize + Send + Sync + Clone> ProducerDataTrait for T {}

#[cfg(not(feature = "json"))]
impl<T: Into<Vec<u8>> + Send + Sync + Clone> ProducerDataTrait for T {}

#[derive(Debug, Clone, Deserialize)]
pub struct ProducerConfig {
    pub topic: String,
}

#[allow(dead_code)]
pub struct Producer<T>
where
    T: ProducerDataTrait,
{
    pub(crate) config: ProducerConfig,
    _phantom: std::marker::PhantomData<T>,
    pub(crate) client: Client,
}

impl<T> Producer<T>
where
    T: ProducerDataTrait,
{
    async fn connect(&self) -> Result<(), NeutronError> {
        self.client.lookup_topic(&self.config.topic).await?;
        self.client.producer(&self.config.topic).await
    }

    fn producer_id(&self) -> u64 {
        self.client.client_id
    }

    fn producer_name(&self) -> &str {
        &self.client.client_name
    }

    pub async fn send(&self, message: T) -> Result<(), NeutronError> {
        #[cfg(feature = "json")]
        let payload =
            serde_json::to_vec(&message).map_err(|_| NeutronError::SerializationFailed)?;
        #[cfg(not(feature = "json"))]
        let payload = message.into();
        self.client.send_message(payload).await?.await.map(|_| ())
    }

    pub async fn send_all(&self, messages: Vec<T>) -> Result<(), NeutronError> {
        let responses = messages
            .into_iter()
            .map(|m| async move {
                #[cfg(feature = "json")]
                let payload =
                    serde_json::to_vec(&m).map_err(|_| NeutronError::SerializationFailed)?;
                #[cfg(not(feature = "json"))]
                let payload = m.into();
                self.client.send_message(payload).await
            })
            .collect::<Vec<_>>();

        futures::future::join_all(responses)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }
}

pub struct ProducerBuilder<T>
where
    T: ProducerDataTrait,
{
    producer_name: Option<String>,
    topic: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> ProducerBuilder<T>
where
    T: ProducerDataTrait,
{
    pub fn new() -> Self {
        Self {
            producer_name: None,
            topic: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_producer_name(mut self, producer_name: &str) -> Self {
        self.producer_name = Some(producer_name.to_string());
        self
    }

    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    pub async fn connect(
        self,
        pulsar_manager: &PulsarManager,
    ) -> Result<Producer<T>, NeutronError> {
        let name = self.producer_name.expect("Producer name is required");
        let topic = self.topic.unwrap();
        let id = pulsar_manager.new_client_id();

        let config = ProducerConfig {
            topic: topic.clone(),
        };

        let pulsar_engine_connection = pulsar_manager.register(topic, id).await?;

        let client = Client::new(pulsar_engine_connection, id, name);

        let producer = Producer {
            config,
            client,
            _phantom: std::marker::PhantomData,
        };

        log::info!("Created producer, producer_id: {}", producer.producer_id());

        producer.connect().await?;
        Ok(producer)
    }
}
