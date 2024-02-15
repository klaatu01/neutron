use std::sync::atomic::AtomicU64;

use crate::{
    engine::{Engine, EngineConnection},
    message::{
        proto::pulsar::MessageIdData, Ack, ClientInbound, ClientOutbound, Inbound, Outbound,
    },
    resolver_manager::{Resolver, ResolverManager},
    PulsarConfig, PulsarManager,
};
use futures::lock::Mutex;
#[cfg(feature = "json")]
use serde::ser::Serialize;
use serde::Deserialize;
use tokio::sync::RwLock;

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
    pub producer_name: Option<String>,
    pub producer_id: u64,
    pub topic: String,
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

#[allow(dead_code)]
pub struct Producer<T>
where
    T: ProducerDataTrait,
{
    pub(crate) config: RwLock<ProducerConfig>,
    pulsar_engine_connection: EngineConnection<Outbound, Inbound>,
    resolver_manager: ResolverManager<Inbound>,
    inbound_buffer: Mutex<Vec<Inbound>>,
    sequence_id: AtomicU64,
    request_id: AtomicU64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Producer<T>
where
    T: ProducerDataTrait,
{
    pub async fn resolve(
        &self,
        socket_connection: &EngineConnection<Outbound, Inbound>,
        resolver: Resolver<Inbound>,
    ) -> Result<Inbound, NeutronError> {
        loop {
            tokio::select! {
                inbound = socket_connection.recv() => {
                    match inbound {
                        Ok(inbound) => {
                            if !self.resolver_manager.try_resolve(&inbound).await {
                                self.inbound_buffer.lock().await.push(inbound);
                            }
                        }
                        Err(e) => {
                            return Err(e)
                        }
                    }
                }
                inbound = resolver.recv() => {
                    return inbound.map_err(|_| NeutronError::OperationTimeout);
                }
            }
        }
    }

    pub async fn send_and_resolve(
        &self,
        pulsar_engine_connection: &EngineConnection<Outbound, Inbound>,
        outbound: &ResultOutbound,
    ) -> ResultInbound {
        match outbound {
            Ok(outbound) => match self.resolver_manager.put_resolver(outbound).await {
                Some(resolver) => {
                    pulsar_engine_connection
                        .send(Ok(outbound.clone()))
                        .await
                        .map_err(|_| NeutronError::ChannelTerminated)?;
                    self.resolve(pulsar_engine_connection, resolver).await
                }
                _ => Err(NeutronError::Unresolvable),
            },
            Err(e) => Err(e.clone()),
        }
    }

    pub async fn lookup_topic(
        &self,
        pulsar_engine_connection: &EngineConnection<Outbound, Inbound>,
    ) -> Result<(), NeutronError> {
        let outbound = {
            let config = self.config.read().await;
            Outbound::Client(ClientOutbound::LookupTopic {
                topic: config.topic.clone(),
                request_id: self
                    .request_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            })
        };
        let _ = self
            .send_and_resolve(pulsar_engine_connection, &Ok(outbound))
            .await?;
        Ok(())
    }

    pub async fn register(
        &self,
        pulsar_engine_connection: &EngineConnection<Outbound, Inbound>,
    ) -> Result<(), NeutronError> {
        let mut config = self.config.write().await;
        let outbound = Outbound::Client(ClientOutbound::Producer {
            producer_id: config.producer_id,
            producer_name: config.producer_name.clone(),
            topic: config.topic.clone(),
            request_id: self
                .request_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        });
        let inbound = self
            .send_and_resolve(pulsar_engine_connection, &Ok(outbound))
            .await?;
        match inbound {
            Inbound::Client(ClientInbound::ProducerSuccess { producer_name, .. }) => {
                config.producer_name = Some(producer_name);
                Ok(())
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }

    pub async fn connect(&self) -> Result<(), NeutronError> {
        self.lookup_topic(&self.pulsar_engine_connection).await?;
        self.register(&self.pulsar_engine_connection).await?;
        Ok(())
    }

    async fn next_inbound(&self) -> Result<Inbound, NeutronError> {
        let mut inbound_buffer = self.inbound_buffer.lock().await;
        if let Some(inbound) = inbound_buffer.pop() {
            Ok(inbound)
        } else {
            let inbound = self.pulsar_engine_connection.recv().await?;
            Ok(inbound)
        }
    }

    pub async fn send(&self, message: T) -> Result<(), NeutronError> {
        let outbound = {
            let config = self.config.read().await;
            Outbound::Client(ClientOutbound::Send {
                producer_name: config.producer_name.clone().unwrap(),
                producer_id: config.producer_id,
                sequence_id: self
                    .sequence_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                #[cfg(feature = "json")]
                payload: serde_json::to_vec(&message)
                    .map_err(|_| NeutronError::SerializationFailed)?,
                #[cfg(not(feature = "json"))]
                payload: message.into(),
            })
        };
        self.send_and_resolve(&self.pulsar_engine_connection, &Ok(outbound))
            .await?;
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
        let producer_name = self.producer_name.unwrap();
        let topic = self.topic.unwrap();
        let producer_id = pulsar_manager.producer_id();
        let producer_config = ProducerConfig {
            producer_name: Some(producer_name),
            producer_id,
            topic,
        };

        let pulsar_engine_connection = pulsar_manager.register_producer(&producer_config).await?;

        let producer = Producer {
            config: RwLock::new(producer_config),
            pulsar_engine_connection,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            sequence_id: AtomicU64::new(0),
            request_id: AtomicU64::new(0),
            _phantom: std::marker::PhantomData,
        };

        producer.connect().await?;

        Ok(producer)
    }
}
