use std::sync::atomic::AtomicU64;

use crate::{
    engine::{Engine, EngineConnection},
    message::{
        proto::pulsar::MessageIdData, Ack, ClientInbound, ClientOutbound, Inbound, Outbound,
    },
    resolver_manager::ResolverManager,
    Pulsar, PulsarConfig, PulsarManager,
};
use async_trait::async_trait;
use futures::lock::Mutex;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::error::NeutronError;

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerConfig {
    pub consumer_name: String,
    pub consumer_id: u64,
    pub topic: String,
    pub subscription: String,
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

#[derive(Debug, Clone)]
pub struct Message<T>
where
    T: TryFrom<Vec<u8>>,
{
    pub payload: T,
    pub message_id: MessageIdData,
}

#[allow(dead_code)]
pub struct Consumer {
    pub(crate) config: ConsumerConfig,
    client_engine_connection: EngineConnection<Outbound, Inbound>,
    resolver_manager: ResolverManager<Inbound>,
    inbound_buffer: Mutex<Vec<Inbound>>,
    message_permits: u32,
    current_message_permits: RwLock<u32>,
    plugins: Mutex<Vec<Box<dyn ConsumerPlugin + Send + Sync>>>,
}

impl Consumer {
    pub async fn send_and_resolve(
        &self,
        socket_connection: &EngineConnection<Outbound, Inbound>,
        outbound: &ResultOutbound,
    ) -> ResultInbound {
        match outbound {
            Ok(outbound) => match self.resolver_manager.put_resolver(outbound).await {
                Some(resolver) => {
                    socket_connection
                        .send(Ok(outbound.clone()))
                        .await
                        .map_err(|_| NeutronError::ChannelTerminated)?;

                    loop {
                        tokio::select! {
                            inbound = socket_connection.recv() => {
                                match inbound {
                                    Ok(inbound) => {
                                        if self.resolver_manager.try_resolve(&inbound).await {
                                            return Ok(inbound);
                                        } else {
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
                _ => Err(NeutronError::Unresolvable),
            },
            Err(e) => Err(e.clone()),
        }
    }

    async fn flow(
        &self,
        client_engine_connection: &EngineConnection<Outbound, Inbound>,
    ) -> Result<(), NeutronError> {
        let outbound = Outbound::Client(ClientOutbound::Flow {
            consumer_id: self.config.consumer_id,
            message_permits: self.message_permits,
        });
        let _ = client_engine_connection.send(Ok(outbound)).await?;
        Ok(())
    }

    pub async fn subscribe(
        &self,
        client_engine_connection: &EngineConnection<Outbound, Inbound>,
    ) -> Result<(), NeutronError> {
        let outbound = Outbound::Client(ClientOutbound::Subscribe {
            topic: self.config.topic.clone(),
            subscription: self.config.subscription.clone(),
            consumer_id: self.config.consumer_id,
            sub_type: crate::message::proto::pulsar::command_subscribe::SubType::Exclusive,
            request_id: 1,
        });
        let _ = self
            .send_and_resolve(client_engine_connection, &Ok(outbound))
            .await?;
        Ok(())
    }

    pub async fn lookup_topic(
        &self,
        client_engine_connection: &EngineConnection<Outbound, Inbound>,
    ) -> Result<(), NeutronError> {
        let outbound = Outbound::Client(ClientOutbound::LookupTopic {
            topic: self.config.topic.clone(),
            request_id: 0,
        });
        let _ = self
            .send_and_resolve(client_engine_connection, &Ok(outbound))
            .await?;
        Ok(())
    }

    pub async fn connect(&self) -> Result<(), NeutronError> {
        self.lookup_topic(&self.client_engine_connection).await?;
        self.subscribe(&self.client_engine_connection).await?;
        self.flow(&self.client_engine_connection).await?;
        Ok(())
    }

    async fn check_and_flow(&self) -> Result<(), NeutronError> {
        {
            let mut current_message_permits = self.current_message_permits.write().await;
            if *current_message_permits >= self.message_permits {
                self.flow(&self.client_engine_connection).await?;
                *current_message_permits = 0;
            }
        }
        Ok(())
    }

    async fn next_inbound(&self) -> Result<Inbound, NeutronError> {
        let mut inbound_buffer = self.inbound_buffer.lock().await;
        if let Some(inbound) = inbound_buffer.pop() {
            Ok(inbound)
        } else {
            let inbound = self.client_engine_connection.recv().await?;
            Ok(inbound)
        }
    }

    async fn increment_message_permit_count(&self) {
        let mut current_message_permits = self.current_message_permits.write().await;
        *current_message_permits += 1;
    }

    pub async fn next<T>(&self) -> Result<Message<T>, NeutronError>
    where
        T: TryFrom<Vec<u8>>,
    {
        loop {
            self.check_and_flow().await?;
            match self.next_inbound().await {
                Ok(inbound) => {
                    if let Inbound::Client(ClientInbound::Message {
                        payload,
                        message_id,
                        ..
                    }) = &inbound
                    {
                        self.increment_message_permit_count().await;
                        self.on_message(Message {
                            payload: payload.clone(),
                            message_id: message_id.clone(),
                        })
                        .await?;
                        return T::try_from(payload.clone())
                            .map(|payload| Message {
                                payload,
                                message_id: message_id.clone(),
                            })
                            .map_err(|_| NeutronError::DeserializationFailed);
                    };
                    continue;
                }
                Err(e) => {
                    log::warn!("{}", e);
                    return Err(e);
                }
            }
        }
    }

    pub async fn on_message(&self, message: Message<Vec<u8>>) -> Result<(), NeutronError> {
        let plugins = self.plugins.lock().await;
        for plugin in plugins.iter() {
            plugin.on_message(self, message.clone()).await?;
        }
        Ok(())
    }

    pub async fn register_plugin<T>(&self, plugin: T)
    where
        T: ConsumerPlugin + Send + Sync + 'static,
    {
        let mut plugins = self.plugins.lock().await;
        plugins.push(Box::new(plugin));
        log::info!("Plugin registered");
    }

    pub async fn ack(&self, message_id: &MessageIdData) -> Result<(), NeutronError> {
        let outbound = Outbound::Client(ClientOutbound::Ack(vec![Ack {
            consumer_id: self.config.consumer_id,
            request_id: message_id.ledgerId() + message_id.entryId(),
            message_id: message_id.clone(),
        }]));
        let _ = &self.client_engine_connection.send(Ok(outbound)).await?;
        Ok(())
    }

    pub async fn start(&self) -> Result<(), NeutronError> {
        loop {
            let inbound = &self.client_engine_connection.recv().await?;
            match inbound {
                Inbound::Client(ClientInbound::Message {
                    payload,
                    message_id,
                    ..
                }) => {
                    self.on_message(Message {
                        payload: payload.clone(),
                        message_id: message_id.clone(),
                    })
                    .await?;
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
pub trait ConsumerPlugin {
    async fn on_connect(&self, consumer: &Consumer) -> Result<(), NeutronError> {
        Ok(())
    }
    async fn on_disconnect(&self, consumer: &Consumer) -> Result<(), NeutronError> {
        Ok(())
    }
    async fn on_message(
        &self,
        consumer: &Consumer,
        message: Message<Vec<u8>>,
    ) -> Result<(), NeutronError> {
        Ok(())
    }
}

pub struct ConsumerBuilder {
    plugins: Vec<Box<dyn ConsumerPlugin + Send + Sync>>,
    consumer_name: Option<String>,
    topic: Option<String>,
    subscription: Option<String>,
    client_connection: Option<EngineConnection<Outbound, Inbound>>,
}

impl ConsumerBuilder {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            consumer_name: None,
            topic: None,
            subscription: None,
            client_connection: None,
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

    pub fn add_plugin<T>(mut self, plugin: T) -> Self
    where
        T: ConsumerPlugin + Send + Sync + 'static,
    {
        self.plugins.push(Box::new(plugin));
        self
    }

    pub async fn connect(self, pulsar_manager: &PulsarManager) -> Consumer {
        let consumer_name = self.consumer_name.unwrap();
        let topic = self.topic.unwrap();
        let subscription = self.subscription.unwrap();
        let consumer_config = ConsumerConfig {
            consumer_name,
            consumer_id: 0,
            topic,
            subscription,
        };

        let client_engine_connection = pulsar_manager
            .register_consumer(&consumer_config)
            .await
            .unwrap();

        let consumer = Consumer {
            config: consumer_config,
            client_engine_connection,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            message_permits: 20,
            current_message_permits: RwLock::new(0),
            plugins: self.plugins.into(),
        };

        consumer.connect().await.unwrap();

        consumer
    }
}
