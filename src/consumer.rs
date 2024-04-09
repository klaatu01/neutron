use crate::client::{Client, PulsarClient};
use crate::error::NeutronError;
use crate::{message::proto::pulsar::MessageIdData, PulsarManager};
use async_trait::async_trait;
use futures::lock::Mutex;
use itertools::Itertools;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::sync::atomic::AtomicU32;

#[cfg(feature = "json")]
pub trait ConsumerDataTrait: DeserializeOwned + Send + Sync + Clone {}

#[cfg(not(feature = "json"))]
pub trait ConsumerDataTrait: TryFrom<Vec<u8>> + Send + Sync + Clone {}

// Implement the trait for all types that satisfy the trait bounds.
// This is a simplified example; for real-world usage, you might need to adjust it.
#[cfg(feature = "json")]
impl<T: DeserializeOwned + Send + Sync + Clone> ConsumerDataTrait for T {}

#[cfg(not(feature = "json"))]
impl<T: TryFrom<Vec<u8>> + Send + Sync + Clone> ConsumerDataTrait for T {}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerConfig {
    pub topic: String,
    pub subscription: String,
}

#[derive(Debug, Clone)]
pub enum Message<T>
where
    T: ConsumerDataTrait,
{
    Single(SingleMessage<T>),
    Batch(BatchMessage<T>),
}

#[derive(Debug, Clone)]
pub struct SingleMessage<T>
where
    T: ConsumerDataTrait,
{
    pub payload: T,
    pub message_id: MessageIdData,
}

#[derive(Debug, Clone)]
pub struct BatchMessage<T>
where
    T: ConsumerDataTrait,
{
    pub payloads: Vec<T>,
    pub message_id: MessageIdData,
}

#[allow(dead_code)]
pub struct Consumer<C, T>
where
    T: ConsumerDataTrait,
    C: PulsarClient,
{
    pub(crate) config: ConsumerConfig,
    pub(crate) client: C,
    pub(crate) message_permits: u32,
    pub(crate) current_message_permits: AtomicU32,
    pub(crate) plugins: Mutex<Vec<Box<dyn ConsumerPlugin<C, T> + Send + Sync>>>,
}

impl<C, T> Consumer<C, T>
where
    T: ConsumerDataTrait,
    C: PulsarClient,
{
    pub async fn next_message(&self) -> Result<Message<T>, NeutronError> {
        self.check_and_flow().await?;

        let next_message = self.client.next_message().await?;

        let message = match next_message {
            crate::message::Message::Single(crate::message::SingleMessage {
                payload,
                message_id,
                ..
            }) => {
                #[cfg(feature = "json")]
                let message: Message<T> = Message::Single(SingleMessage {
                    payload: serde_json::from_slice(&payload)
                        .map_err(|_| NeutronError::DeserializationFailed)?,
                    message_id: message_id.clone(),
                });

                #[cfg(not(feature = "json"))]
                let message: Message<T> = Message::Single(SingleMessage {
                    payload: payload
                        .try_into()
                        .map_err(|_| NeutronError::DeserializationFailed)?,
                    message_id: message_id.clone(),
                });

                message
            }
            crate::message::Message::Batch(batch) => {
                let messages = batch
                    .payloads
                    .into_iter()
                    .map(|m| {
                        #[cfg(feature = "json")]
                        let message: T = serde_json::from_slice(&m.payload)
                            .map_err(|_| NeutronError::DeserializationFailed)
                            .unwrap();

                        #[cfg(not(feature = "json"))]
                        let message: T = m
                            .payload
                            .try_into()
                            .map_err(|_| NeutronError::DeserializationFailed)
                            .unwrap();

                        message
                    })
                    .collect::<Vec<_>>();

                Message::Batch(BatchMessage {
                    payloads: messages,
                    message_id: batch.message_id.clone(),
                })
            }
        };

        // increase message permit count
        self.current_message_permits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(message)
    }

    pub async fn ack(&self, message_id: &MessageIdData) -> Result<(), NeutronError> {
        self.client.ack(message_id).await?.await?;
        Ok(())
    }

    pub async fn ack_all(&self, message_ids: Vec<MessageIdData>) -> Result<(), NeutronError> {
        let responses = message_ids
            .iter()
            .map(|m| self.client.ack(m))
            .collect::<Vec<_>>();

        let (receipts, _errors): (Vec<_>, Vec<NeutronError>) = futures::future::join_all(responses)
            .await
            .into_iter()
            .partition_result();

        futures::future::join_all(receipts).await;

        Ok(())
    }

    async fn check_and_flow(&self) -> Result<(), NeutronError> {
        {
            let current_message_permits = self
                .current_message_permits
                .load(std::sync::atomic::Ordering::Relaxed);
            if current_message_permits >= self.message_permits {
                self.client.flow(self.message_permits * 2).await?;
                self.current_message_permits
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Ok(())
    }

    async fn connect(&self) -> Result<(), NeutronError> {
        log::debug!("Connecting consumer: {}", self.client.client_name());
        self.client.connect().await?;
        log::debug!("Looking up topic: {}", self.config.topic);
        self.client.lookup_topic(&self.config.topic).await?;
        log::debug!(
            "Subscribing to topic: {} with subscription: {}",
            self.config.topic,
            self.config.subscription
        );
        self.client
            .subscribe(&self.config.topic, &self.config.subscription)
            .await?;
        log::debug!("Flowing consumer: {}", self.client.client_name());
        self.client.flow(self.message_permits * 2).await?;
        log::debug!("Connected consumer: {}", self.client.client_name());
        Ok(())
    }

    pub fn consumer_id(&self) -> u64 {
        self.client.client_id()
    }

    pub fn consumer_name(&self) -> &str {
        self.client.client_name()
    }
}

#[async_trait]
pub trait ConsumerEngine {
    async fn consume(&self) -> Result<(), NeutronError>;
}

#[async_trait]
impl<C, T> ConsumerEngine for Consumer<C, T>
where
    T: ConsumerDataTrait,
    C: PulsarClient + std::marker::Sync,
{
    async fn consume(&self) -> Result<(), NeutronError> {
        loop {
            let message = self.next_message().await?;
            let mut plugins = self.plugins.lock().await;
            for plugin in plugins.iter_mut() {
                plugin.on_message(self, message.clone()).await?;
            }
        }
    }
}

#[allow(unused_variables)]
#[async_trait]
pub trait ConsumerPlugin<C, T>
where
    C: PulsarClient,
    T: ConsumerDataTrait,
{
    async fn on_connect(&mut self, consumer: &Consumer<C, T>) -> Result<(), NeutronError> {
        Ok(())
    }
    async fn on_disconnect(&mut self, consumer: &Consumer<C, T>) -> Result<(), NeutronError> {
        Ok(())
    }
    async fn on_message(
        &mut self,
        consumer: &Consumer<C, T>,
        message: Message<T>,
    ) -> Result<(), NeutronError> {
        Ok(())
    }
}

pub struct ConsumerBuilder<C, T>
where
    T: ConsumerDataTrait,
    C: PulsarClient,
{
    plugins: Vec<Box<dyn ConsumerPlugin<C, T> + Send + Sync>>,
    consumer_name: Option<String>,
    topic: Option<String>,
    subscription: Option<String>,
}

impl<T> Default for ConsumerBuilder<Client, T>
where
    T: ConsumerDataTrait,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConsumerBuilder<Client, T>
where
    T: ConsumerDataTrait,
{
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            consumer_name: None,
            topic: None,
            subscription: None,
        }
    }

    pub fn with_consumer_name(mut self, consumer_name: &str) -> Self {
        self.consumer_name = Some(consumer_name.to_string());
        self
    }

    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    pub fn with_subscription(mut self, subscription: &str) -> Self {
        self.subscription = Some(subscription.to_string());
        self
    }

    pub fn add_plugin<Plugin>(mut self, plugin: Plugin) -> Self
    where
        Plugin: ConsumerPlugin<Client, T> + Send + Sync + 'static,
    {
        self.plugins.push(Box::new(plugin));
        self
    }

    pub async fn connect(
        self,
        pulsar_manager: &PulsarManager,
    ) -> Result<Consumer<Client, T>, NeutronError> {
        let consumer_name = self.consumer_name.unwrap();
        let topic = self.topic.unwrap();
        let subscription = self.subscription.unwrap();
        let consumer_id = pulsar_manager.new_client_id();

        let consumer_config = ConsumerConfig {
            topic: topic.clone(),
            subscription,
        };

        let pulsar_engine_connection = pulsar_manager.register(topic, consumer_id).await?;

        let client = Client::new(pulsar_engine_connection, consumer_id, consumer_name);

        let consumer = Consumer {
            config: consumer_config,
            message_permits: 250,
            current_message_permits: AtomicU32::new(0),
            plugins: self.plugins.into(),
            client,
        };

        log::info!("Created consumer, consumer_id: {}", consumer.consumer_id());

        consumer.connect().await?;
        Ok(consumer)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        client::MockPulsarClient,
        message::{proto::pulsar::MessageIdData, AckReciept, Connected, Inbound},
        Consumer, NeutronError,
    };

    fn get_mock_client(next: Option<Vec<Inbound>>) -> MockPulsarClient {
        let mut client = MockPulsarClient::new();
        client
            .expect_client_name()
            .return_const("test_client".into());
        client.expect_client_id().return_const(0u64);

        if let Some(next) = next {
            for message in next {
                client.expect_next().return_once(|| Ok(message));
            }
        }

        client
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Data {
        pub data: String,
    }

    impl TryFrom<Vec<u8>> for Data {
        type Error = NeutronError;

        fn try_from(value: Vec<u8>) -> Result<Self, NeutronError> {
            Ok(Data {
                data: String::from_utf8(value).map_err(|_| NeutronError::DeserializationFailed)?,
            })
        }
    }

    #[tokio::test]
    async fn test_base_consumer() {
        let client = get_mock_client(None);
        let consumer: Consumer<MockPulsarClient, Data> = Consumer {
            config: crate::consumer::ConsumerConfig {
                topic: "test_topic".into(),
                subscription: "test_subscription".into(),
            },
            client,
            message_permits: 250,
            current_message_permits: std::sync::atomic::AtomicU32::new(0),
            plugins: futures::lock::Mutex::new(Vec::new()),
        };

        assert_eq!(consumer.consumer_name(), "test_client");
        assert_eq!(consumer.consumer_id(), 0);
    }

    #[tokio::test]
    async fn test_connect_flow() {
        let mut client = get_mock_client(None);
        client
            .expect_connect()
            .times(1)
            .return_once(|| Ok(Connected));
        client
            .expect_lookup_topic()
            .times(1)
            .return_once(|_| Ok(()));
        client
            .expect_subscribe()
            .times(1)
            .return_once(|_, _| Ok(()));
        client.expect_flow().times(1).return_once(|_| Ok(()));
        let consumer: Consumer<MockPulsarClient, Data> = Consumer {
            config: crate::consumer::ConsumerConfig {
                topic: "test_topic".into(),
                subscription: "test_subscription".into(),
            },
            client,
            message_permits: 250,
            current_message_permits: std::sync::atomic::AtomicU32::new(0),
            plugins: futures::lock::Mutex::new(Vec::new()),
        };

        consumer.connect().await.unwrap();
    }

    #[tokio::test]
    async fn test_ack() {
        let mut client = get_mock_client(None);
        client.expect_ack().times(1).return_once(|_| {
            let future = async {
                Ok(AckReciept {
                    consumer_id: 0,
                    request_id: 0,
                })
            };
            Ok(Box::pin(future))
        });
        let consumer: Consumer<MockPulsarClient, Data> = Consumer {
            config: crate::consumer::ConsumerConfig {
                topic: "test_topic".into(),
                subscription: "test_subscription".into(),
            },
            client,
            message_permits: 250,
            current_message_permits: std::sync::atomic::AtomicU32::new(0),
            plugins: futures::lock::Mutex::new(Vec::new()),
        };

        consumer.ack(&MessageIdData::new()).await.unwrap();
    }

    #[tokio::test]
    async fn test_message_recieve() {
        let mut client = get_mock_client(None);
        client.expect_next_message().times(3).returning(|| {
            Ok(crate::message::Message {
                payload: vec![104, 101, 108, 108, 111, 95, 119, 111, 114, 108, 100],
                message_id: MessageIdData::new(),
                consumer_id: 0,
            })
        });
        client.expect_flow().times(1).return_once(|_| Ok(()));
        let consumer: Consumer<MockPulsarClient, Data> = Consumer {
            config: crate::consumer::ConsumerConfig {
                topic: "test_topic".into(),
                subscription: "test_subscription".into(),
            },
            client,
            message_permits: 2,
            current_message_permits: std::sync::atomic::AtomicU32::new(0),
            plugins: futures::lock::Mutex::new(Vec::new()),
        };

        let message: Data = consumer.next_message().await.unwrap().payload;

        assert_eq!(
            message,
            Data {
                data: "hello_world".into()
            }
        );

        assert_eq!(
            consumer
                .current_message_permits
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let _: Data = consumer.next_message().await.unwrap().payload;
        assert_eq!(
            consumer
                .current_message_permits
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        let _: Data = consumer.next_message().await.unwrap().payload;
        assert_eq!(
            consumer
                .current_message_permits
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}
