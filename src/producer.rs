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
pub struct ProducerConfig {
    pub producer_name: Option<String>,
    pub producer_id: u64,
    pub topic: String,
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

#[allow(dead_code)]
pub struct Producer {
    pub(crate) config: RwLock<ProducerConfig>,
    pulsar_config: PulsarConfig,
    client_engine_connection: EngineConnection<Outbound, Inbound>,
    resolver_manager: ResolverManager<Inbound>,
    inbound_buffer: Mutex<Vec<Inbound>>,
    sequence_id: AtomicU64,
    request_id: AtomicU64,
}

impl Producer {
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

    pub async fn lookup_topic(
        &self,
        client_engine_connection: &EngineConnection<Outbound, Inbound>,
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
            .send_and_resolve(client_engine_connection, &Ok(outbound))
            .await?;
        Ok(())
    }

    pub async fn register(
        &self,
        client_engine_connection: &EngineConnection<Outbound, Inbound>,
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
            .send_and_resolve(client_engine_connection, &Ok(outbound))
            .await?;
        match inbound {
            Inbound::Client(ClientInbound::ProducerSuccess { producer_name, .. }) => {
                config.producer_name = Some(producer_name);
                Ok(())
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }

    pub async fn connect(self) -> Result<(), NeutronError> {
        self.lookup_topic(&self.client_engine_connection).await?;
        self.register(&self.client_engine_connection).await?;
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

    pub async fn send<T>(&self, message: T) -> Result<(), NeutronError>
    where
        T: Into<Vec<u8>>,
    {
        let outbound = {
            let config = self.config.read().await;
            Outbound::Client(ClientOutbound::Send {
                producer_name: config.producer_name.clone().unwrap(),
                producer_id: config.producer_id,
                sequence_id: self
                    .sequence_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                payload: message.into(),
            })
        };
        self.send_and_resolve(&self.client_engine_connection, &Ok(outbound))
            .await?;
        Ok(())
    }
}
