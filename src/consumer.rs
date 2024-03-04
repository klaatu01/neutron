use crate::client::Client;
use crate::error::NeutronError;
use crate::{
    message::{proto::pulsar::MessageIdData, Inbound, Outbound},
    PulsarManager,
};
use async_trait::async_trait;
use futures::lock::Mutex;
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

pub(crate) type ResultInbound = Result<Inbound, NeutronError>;
pub(crate) type ResultOutbound = Result<Outbound, NeutronError>;

#[derive(Debug, Clone)]
pub struct Message<T>
where
    T: ConsumerDataTrait,
{
    pub payload: T,
    pub message_id: MessageIdData,
}

#[allow(dead_code)]
pub struct Consumer<T>
where
    T: ConsumerDataTrait,
{
    pub(crate) config: ConsumerConfig,
    pub(crate) client: Client,
    pub(crate) message_permits: u32,
    pub(crate) current_message_permits: AtomicU32,
    pub(crate) plugins: Mutex<Vec<Box<dyn ConsumerPlugin<T> + Send + Sync>>>,
}

impl<T> Consumer<T>
where
    T: ConsumerDataTrait,
{
    pub async fn next_message(&self) -> Result<Message<T>, NeutronError> {
        self.check_and_flow().await?;

        let crate::message::Message {
            payload,
            message_id,
            ..
        } = self.client.next_message().await?;

        #[cfg(feature = "json")]
        let message: Message<T> = Message {
            payload: serde_json::from_slice(&payload)
                .map_err(|_| NeutronError::DeserializationFailed)?,
            message_id: message_id.clone(),
        };

        #[cfg(not(feature = "json"))]
        let message: Message<T> = Message {
            payload: payload
                .try_into()
                .map_err(|_| NeutronError::DeserializationFailed)?,
            message_id: message_id.clone(),
        };

        // increase message permit count
        self.current_message_permits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(message)
    }

    pub async fn ack(&self, message_id: &MessageIdData) -> Result<(), NeutronError> {
        self.client.ack(message_id).await?;
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
        self.client.lookup_topic(&self.config.topic).await?;
        self.client
            .subscribe(&self.config.topic, &self.config.subscription)
            .await?;
        self.client.flow(self.message_permits * 2).await?;
        Ok(())
    }

    pub fn consumer_id(&self) -> u64 {
        self.client.client_id
    }

    pub fn consumer_name(&self) -> &str {
        &self.client.client_name
    }
}

#[async_trait]
pub trait ConsumerEngine {
    async fn consume(&self) -> Result<(), NeutronError>;
}

#[async_trait]
impl<T> ConsumerEngine for Consumer<T>
where
    T: ConsumerDataTrait,
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
pub trait ConsumerPlugin<T>
where
    T: ConsumerDataTrait,
{
    async fn on_connect(&mut self, consumer: &Consumer<T>) -> Result<(), NeutronError> {
        Ok(())
    }
    async fn on_disconnect(&mut self, consumer: &Consumer<T>) -> Result<(), NeutronError> {
        Ok(())
    }
    async fn on_message(
        &mut self,
        consumer: &Consumer<T>,
        message: Message<T>,
    ) -> Result<(), NeutronError> {
        Ok(())
    }
}

pub struct ConsumerBuilder<T>
where
    T: ConsumerDataTrait,
{
    plugins: Vec<Box<dyn ConsumerPlugin<T> + Send + Sync>>,
    consumer_name: Option<String>,
    topic: Option<String>,
    subscription: Option<String>,
}

impl<T> ConsumerBuilder<T>
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
        Plugin: ConsumerPlugin<T> + Send + Sync + 'static,
    {
        self.plugins.push(Box::new(plugin));
        self
    }

    pub async fn connect(
        self,
        pulsar_manager: &PulsarManager,
    ) -> Result<Consumer<T>, NeutronError> {
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
    use std::sync::atomic::AtomicU64;

    use async_trait::async_trait;
    use futures::lock::Mutex;
    use tokio::sync::RwLock;

    use crate::{
        engine::EngineConnection,
        message::{proto::pulsar::MessageIdData, ClientInbound, ClientOutbound, Inbound, Outbound},
        resolver_manager::ResolverManager,
        Consumer, ConsumerPlugin, NeutronError,
    };

    #[derive(Debug, Clone)]
    struct TestConsumerData {
        pub data: String,
    }

    impl TryFrom<Vec<u8>> for TestConsumerData {
        type Error = NeutronError;

        fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
            Ok(TestConsumerData {
                data: String::from_utf8(value).map_err(|_| NeutronError::DeserializationFailed)?,
            })
        }
    }

    impl Into<Vec<u8>> for TestConsumerData {
        fn into(self) -> Vec<u8> {
            self.data.into_bytes()
        }
    }

    #[tokio::test]
    async fn consumer_connect_test() {
        let (pulsar_engine_connection, consumer_connection) =
            EngineConnection::<Outbound, Inbound>::pair();
        let consumer_message_permits = 250;
        let consumer: Consumer<TestConsumerData> = Consumer {
            config: crate::ConsumerConfig {
                consumer_name: "test".to_string(),
                consumer_id: 0,
                topic: "test".to_string(),
                subscription: "test".to_string(),
            },
            pulsar_engine_connection,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            message_permits: consumer_message_permits.clone(),
            current_message_permits: RwLock::new(0),
            plugins: Mutex::new(Vec::new()),
            request_id: AtomicU64::new(0),
        };
        let mock_server = async move {
            loop {
                let outbound = consumer_connection.recv().await.unwrap();
                match outbound {
                    Outbound::Client(crate::message::ClientOutbound::LookupTopic {
                        request_id,
                        ..
                    }) => {
                        let _ = consumer_connection
                            .send(Ok(Inbound::Client(
                                crate::message::ClientInbound::LookupTopic {
                                    request_id,
                                    broker_service_url_tls: "test".to_string(),
                                    broker_service_url: "test".to_string(),
                                    response: crate::message::proto::pulsar::command_lookup_topic_response::LookupType::Connect,
                                    authoritative: true
                                },
                            )))
                            .await;
                    }
                    Outbound::Client(crate::message::ClientOutbound::Subscribe {
                        request_id,
                        ..
                    }) => {
                        let _ = consumer_connection
                            .send(Ok(Inbound::Client(
                                crate::message::ClientInbound::Success { request_id },
                            )))
                            .await;
                    }
                    Outbound::Client(crate::message::ClientOutbound::Flow {
                        message_permits,
                        ..
                    }) => {
                        assert!(message_permits == consumer_message_permits * 2);
                        return Ok(());
                    }
                    _ => return Err(NeutronError::UnsupportedCommand),
                }
            }
        };
        let (consumer_connect_result, mock_server_result) =
            tokio::join!(consumer.connect(), mock_server);
        assert!(consumer_connect_result.is_ok());
        assert!(mock_server_result.is_ok());
    }

    #[tokio::test]
    async fn can_handle_disconnect() {
        let (pulsar_engine_connection, consumer_connection) =
            EngineConnection::<Outbound, Inbound>::pair();
        let consumer_message_permits = 250;
        let consumer: Consumer<TestConsumerData> = Consumer {
            config: crate::ConsumerConfig {
                consumer_name: "test".to_string(),
                consumer_id: 0,
                topic: "test".to_string(),
                subscription: "test".to_string(),
            },
            pulsar_engine_connection,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            message_permits: consumer_message_permits.clone(),
            current_message_permits: RwLock::new(0),
            plugins: Mutex::new(Vec::new()),
            request_id: AtomicU64::new(0),
        };
        consumer_connection
            .send(Err(NeutronError::Disconnected))
            .await
            .unwrap();
        let output = consumer.consume().await;
        assert!(output.is_err());
        match output {
            Err(NeutronError::Disconnected) => assert!(true),
            _ => assert!(false),
        }
    }

    struct AutoAckPlugin;

    #[async_trait]
    impl ConsumerPlugin<TestConsumerData> for AutoAckPlugin {
        async fn on_message(
            &mut self,
            consumer: &Consumer<TestConsumerData>,
            message: super::Message<TestConsumerData>,
        ) -> Result<(), NeutronError> {
            consumer.ack(&message.message_id).await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn consumer_test() {
        let (pulsar_engine_connection, consumer_connection) =
            EngineConnection::<Outbound, Inbound>::pair();
        let consumer_message_permits = 500;
        let consumer: Consumer<TestConsumerData> = Consumer {
            config: crate::ConsumerConfig {
                consumer_name: "test".to_string(),
                consumer_id: 0,
                topic: "test".to_string(),
                subscription: "test".to_string(),
            },
            pulsar_engine_connection,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            message_permits: consumer_message_permits.clone(),
            current_message_permits: RwLock::new(0),
            plugins: Mutex::new(vec![Box::new(AutoAckPlugin)]),
            request_id: AtomicU64::new(0),
        };
        let mock_server = async move {
            loop {
                let outbound = consumer_connection.recv().await.unwrap();
                match outbound {
                    Outbound::Client(crate::message::ClientOutbound::LookupTopic {
                        request_id,
                        ..
                    }) => {
                        let _ = consumer_connection
                            .send(Ok(Inbound::Client(
                                crate::message::ClientInbound::LookupTopic {
                                    request_id,
                                    broker_service_url_tls: "test".to_string(),
                                    broker_service_url: "test".to_string(),
                                    response: crate::message::proto::pulsar::command_lookup_topic_response::LookupType::Connect,
                                    authoritative: true
                                },
                            )))
                            .await;
                    }
                    Outbound::Client(crate::message::ClientOutbound::Subscribe {
                        request_id,
                        ..
                    }) => {
                        let _ = consumer_connection
                            .send(Ok(Inbound::Client(
                                crate::message::ClientInbound::Success { request_id },
                            )))
                            .await;
                    }
                    Outbound::Client(crate::message::ClientOutbound::Flow {
                        message_permits,
                        ..
                    }) => {
                        assert!(message_permits == consumer_message_permits * 2);
                        break;
                    }
                    _ => return Err(NeutronError::UnsupportedCommand),
                }
            }
            for x in 0..10000 {
                let mut message_id = MessageIdData::default();
                message_id.set_entryId(x);
                message_id.set_ledgerId(x);
                let _ = consumer_connection
                    .send(Ok(Inbound::Client(
                        crate::message::ClientInbound::Message {
                            consumer_id: 0,
                            message_id: message_id.clone(),
                            payload: TestConsumerData {
                                data: "hello_world".to_string(),
                            }
                            .into(),
                        },
                    )))
                    .await;
            }
            let mut count = 0;
            loop {
                let outbound = consumer_connection.recv().await.unwrap();
                match outbound {
                    Outbound::Client(ClientOutbound::Ack(ack)) => {
                        let first = ack.first().cloned().unwrap();
                        consumer_connection
                            .send(Ok(Inbound::Client(ClientInbound::AckResponse {
                                consumer_id: first.consumer_id,
                                request_id: first.request_id,
                            })))
                            .await
                            .unwrap();
                        count += 1;
                        if count == 10000 {
                            consumer_connection
                                .send(Err(NeutronError::Disconnected))
                                .await
                                .unwrap();
                            return Ok(());
                        }
                    }
                    Outbound::Client(crate::message::ClientOutbound::Flow {
                        message_permits,
                        ..
                    }) => {
                        continue;
                    }
                    _ => {
                        return Err(NeutronError::UnsupportedCommand);
                    }
                }
            }
        };
        let (consumer_start_result, mock_server_result) = tokio::join!(
            async {
                let result = consumer.connect().await;
                assert!(result.is_ok());
                consumer.consume().await
            },
            mock_server
        );
        match consumer_start_result {
            Err(e) if e.is_disconnect() => assert!(true),
            _ => assert!(false),
        }
        assert!(mock_server_result.is_ok());
    }
}
