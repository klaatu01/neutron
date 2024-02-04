use std::sync::atomic::AtomicU64;

use crate::{
    engine::{Engine, EngineConnection},
    message::{
        proto::pulsar::MessageIdData, Ack, ClientInbound, ClientOutbound, Inbound, Outbound,
    },
    resolver_manager::ResolverManager,
    PulsarConfig,
};
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

pub struct Message<T>
where
    T: TryFrom<Vec<u8>>,
{
    pub payload: T,
    pub message_id: MessageIdData,
}

#[allow(dead_code)]
pub struct Consumer {
    config: ConsumerConfig,
    pulsar_config: PulsarConfig,
    client_engine_connection: Option<EngineConnection<Outbound, Inbound>>,
    resolver_manager: ResolverManager<Inbound>,
    inbound_buffer: Mutex<Vec<Inbound>>,
    message_permits: u32,
    current_message_permits: RwLock<u32>,
}

impl Consumer {
    pub fn new(pulsar_config: PulsarConfig, consumer_config: ConsumerConfig) -> Self {
        Self {
            config: consumer_config,
            pulsar_config,
            client_engine_connection: None,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            message_permits: 20,
            current_message_permits: RwLock::new(0),
        }
    }

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

    pub async fn connect(self) -> Result<Self, NeutronError> {
        let client_engine_connection = crate::Pulsar::new(self.pulsar_config.clone()).run().await;

        self.lookup_topic(&client_engine_connection).await?;
        self.subscribe(&client_engine_connection).await?;
        self.flow(&client_engine_connection).await?;

        Ok(Consumer {
            config: self.config,
            pulsar_config: self.pulsar_config,
            client_engine_connection: Some(client_engine_connection),
            resolver_manager: self.resolver_manager,
            inbound_buffer: Mutex::new(Vec::new()),
            message_permits: self.message_permits,
            current_message_permits: RwLock::new(0),
        })
    }

    // if the count of message permits is half the max off of the message permits, then send a flow command and reset current message permits to 0
    async fn check_and_flow(&self) -> Result<(), NeutronError> {
        {
            let mut current_message_permits = self.current_message_permits.write().await;
            if *current_message_permits >= self.message_permits {
                let client_engine_connection = self.client_engine_connection.as_ref().unwrap();
                self.flow(client_engine_connection).await?;
                *current_message_permits = 0;
            }
        }
        Ok(())
    }

    pub async fn next<T>(&self) -> Result<Message<T>, NeutronError>
    where
        T: TryFrom<Vec<u8>>,
    {
        match &self.client_engine_connection {
            Some(engine_connection) => loop {
                self.check_and_flow().await?;
                let mut inbound_buffer = self.inbound_buffer.lock().await;
                let inbound = if let Some(inbound) = inbound_buffer.pop() {
                    Ok(inbound)
                } else {
                    engine_connection.recv().await
                };
                match inbound {
                    Ok(inbound) => {
                        if let Inbound::Client(ClientInbound::Message {
                            payload,
                            message_id,
                            ..
                        }) = &inbound
                        {
                            {
                                let mut current_message_permits =
                                    self.current_message_permits.write().await;
                                *current_message_permits += 1;
                            }
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
            },
            None => {
                log::error!("Disconnected");
                Err(NeutronError::Disconnected)
            }
        }
    }

    pub async fn ack(&self, message_id: &MessageIdData) -> Result<(), NeutronError> {
        let outbound = Outbound::Client(ClientOutbound::Ack(vec![Ack {
            consumer_id: self.config.consumer_id,
            request_id: message_id.ledgerId() + message_id.entryId(),
            message_id: message_id.clone(),
        }]));
        if let Some(client_engine_connection) = &self.client_engine_connection {
            let _ = client_engine_connection.send(Ok(outbound)).await?;
        }
        Ok(())
    }
}
