use crate::{
    engine::{Engine, EngineConnection},
    message::{ClientInbound, ClientOutbound, Inbound, Outbound},
    resolver_manager::ResolverManager,
    PulsarConfig,
};
use futures::lock::Mutex;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
use serde::Deserialize;

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

#[allow(dead_code)]
pub struct Consumer {
    config: ConsumerConfig,
    pulsar_config: PulsarConfig,
    client_engine_connection: Option<EngineConnection<Outbound, Inbound>>,
    resolver_manager: ResolverManager<Inbound>,
    inbound_buffer: Mutex<Vec<Inbound>>,
}

impl Consumer {
    pub fn new(pulsar_config: PulsarConfig, consumer_config: ConsumerConfig) -> Self {
        Self {
            config: consumer_config,
            pulsar_config,
            client_engine_connection: None,
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
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
            message_permits: 1000,
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
        })
    }

    pub async fn next(&self) -> Result<Inbound, NeutronError> {
        match &self.client_engine_connection {
            Some(engine_connection) => {
                let inbound = engine_connection.recv().await;
                match inbound {
                    Ok(inbound) => {
                        if let Inbound::Client(ClientInbound::Message { payload, .. }) = &inbound {
                        };
                        return Ok(inbound);
                    }

                    Err(e) => {
                        log::warn!("{}", e);
                        return Err(e);
                    }
                }
            }
            None => {
                log::error!("Disconnected");
                Err(NeutronError::Disconnected)
            }
        }
    }
}
