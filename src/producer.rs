use crate::{
    client::{Client, PulsarClient},
    PulsarManager,
};

use itertools::Itertools;
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
pub struct Producer<C, T>
where
    T: ProducerDataTrait,
    C: PulsarClient,
{
    pub(crate) config: ProducerConfig,
    _phantom: std::marker::PhantomData<T>,
    pub(crate) client: C,
}

impl<C, T> Producer<C, T>
where
    T: ProducerDataTrait,
    C: PulsarClient,
{
    async fn connect(&self) -> Result<(), NeutronError> {
        self.client.connect().await?;
        self.client.lookup_topic(&self.config.topic).await?;
        self.client.producer(&self.config.topic).await
    }

    fn producer_id(&self) -> u64 {
        self.client.client_id()
    }

    #[allow(dead_code)]
    fn producer_name(&self) -> &str {
        self.client.client_name()
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

        let (receipts, _errors): (Vec<_>, Vec<NeutronError>) = futures::future::join_all(responses)
            .await
            .into_iter()
            .partition_result();

        futures::future::join_all(receipts).await;

        Ok(())
    }
}

pub struct ProducerBuilder<C, T>
where
    T: ProducerDataTrait,
    C: PulsarClient,
{
    producer_name: Option<String>,
    topic: Option<String>,
    _phantom: std::marker::PhantomData<T>,
    _phantom_c: std::marker::PhantomData<C>,
}

impl<T> Default for ProducerBuilder<Client, T>
where
    T: ProducerDataTrait,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ProducerBuilder<Client, T>
where
    T: ProducerDataTrait,
{
    pub fn new() -> Self {
        Self {
            producer_name: None,
            topic: None,
            _phantom: std::marker::PhantomData,
            _phantom_c: std::marker::PhantomData,
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
    ) -> Result<Producer<Client, T>, NeutronError> {
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

#[cfg(test)]
mod tests {
    use crate::{
        client::MockPulsarClient,
        message::{proto::pulsar::MessageIdData, Connected, Inbound, SendReceipt},
        Producer,
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

    #[derive(Debug, Clone)]
    struct Data {
        pub data: String,
    }

    impl From<Data> for Vec<u8> {
        fn from(val: Data) -> Self {
            val.data.into_bytes()
        }
    }

    #[tokio::test]
    async fn test_base() {
        let client = get_mock_client(None);
        let producer: Producer<MockPulsarClient, Data> = Producer {
            config: super::ProducerConfig {
                topic: "test_topic".into(),
            },
            client,
            _phantom: std::marker::PhantomData,
        };

        assert_eq!(producer.producer_name(), "test_client");
        assert_eq!(producer.producer_id(), 0);
    }

    #[tokio::test]
    async fn test_connect_flow() {
        let mut client = get_mock_client(None);
        client.expect_connect().return_once(|| Ok(Connected));
        client.expect_lookup_topic().return_once(|_| Ok(()));
        client.expect_producer().return_once(|_| Ok(()));
        let producer: Producer<MockPulsarClient, Data> = Producer {
            config: super::ProducerConfig {
                topic: "test_topic".into(),
            },
            client,
            _phantom: std::marker::PhantomData,
        };

        producer.connect().await.unwrap();
    }

    #[tokio::test]
    async fn test_send() {
        let mut client = get_mock_client(None);
        client
            .expect_send_message()
            .withf(|send| {
                send.clone() == vec![104, 101, 108, 108, 111, 95, 119, 111, 114, 108, 100]
            })
            .return_once(|_| {
                Ok(Box::pin(async {
                    Ok(SendReceipt {
                        message_id: MessageIdData::new(),
                        producer_id: 0,
                        sequence_id: 0,
                    })
                }))
            });

        let producer: Producer<MockPulsarClient, Data> = Producer {
            config: super::ProducerConfig {
                topic: "test_topic".into(),
            },
            client,
            _phantom: std::marker::PhantomData,
        };

        producer
            .send(Data {
                data: "hello_world".into(),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_send_all() {
        let mut client = get_mock_client(None);
        client
            .expect_send_message()
            .withf(|send| {
                send.clone() == vec![104, 101, 108, 108, 111, 95, 119, 111, 114, 108, 100]
            })
            .times(100)
            .returning(|_| {
                Ok(Box::pin(async {
                    Ok(SendReceipt {
                        message_id: MessageIdData::new(),
                        producer_id: 0,
                        sequence_id: 0,
                    })
                }))
            });

        let producer: Producer<MockPulsarClient, Data> = Producer {
            config: super::ProducerConfig {
                topic: "test_topic".into(),
            },
            client,
            _phantom: std::marker::PhantomData,
        };

        // generate list of data 100 long
        let data = vec![
            Data {
                data: "hello_world".into(),
            };
            100
        ];

        producer.send_all(data).await.unwrap()
    }
}
