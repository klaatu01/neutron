use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use async_trait::async_trait;
use futures::Future;
use tokio::sync::{mpsc, Mutex};

use crate::{
    broker_address::BrokerAddress,
    message::{
        self, proto::pulsar::MessageIdData, AckReciept, Connected, Inbound, LookupResponseType,
        LookupTopic, LookupTopicResponse, Outbound, ProducerSuccess, SendReceipt, Subscribe,
        Success,
    },
    pulsar::PulsarInner,
    registry::{ClientSession, ConnectionSlot},
    NeutronError,
};

/// How many broker hops a topic lookup may take (base -> proxy or owner
/// -> authoritative answer) before giving up.
const MAX_LOOKUP_HOPS: usize = 3;

/// One consumer or producer's face onto the cluster. The client holds a
/// connection *slot*, not a connection: the handle inside the slot can be
/// swapped underneath it, which is what lets the topic move brokers and
/// the connection be replaced without the client noticing.
pub struct Client {
    pub(crate) inner: Arc<PulsarInner>,
    pub(crate) slot: RwLock<Arc<ConnectionSlot>>,
    pub(crate) inbox: Mutex<mpsc::Receiver<Inbound>>,
    pub(crate) client_id: u64,
    pub(crate) client_name: String,
    /// Shared with every other client: responses are correlated by
    /// request id, so ids must be unique across the whole connection.
    pub(crate) request_id: Arc<AtomicU64>,
    pub(crate) sequence_id: AtomicU64,
}

type RecieptFuture<T> = Pin<Box<dyn Future<Output = Result<T, NeutronError>> + Send>>;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait PulsarClient {
    async fn next(&self) -> Result<Inbound, NeutronError>;

    async fn connect(&self) -> Result<Connected, NeutronError>;

    async fn lookup_topic(&self, topic: &str) -> Result<(), NeutronError>;

    async fn producer(&self, topic: &str) -> Result<(), NeutronError>;

    async fn subscribe(&self, topic: &str, subscription: &str) -> Result<(), NeutronError>;

    async fn ack(
        &self,
        message_id: &MessageIdData,
    ) -> Result<RecieptFuture<AckReciept>, NeutronError>;

    async fn send_message(
        &self,
        payload: Vec<u8>,
    ) -> Result<RecieptFuture<SendReceipt>, NeutronError>;

    async fn send_batch_message(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<RecieptFuture<SendReceipt>, NeutronError>;

    async fn next_message(&self) -> Result<message::Message, NeutronError>;

    async fn flow(&self, message_permits: u32) -> Result<(), NeutronError>;

    fn client_id(&self) -> u64;

    fn client_name(&self) -> &str;
}

impl Client {
    pub(crate) fn new(
        inner: Arc<PulsarInner>,
        slot: Arc<ConnectionSlot>,
        inbox: mpsc::Receiver<Inbound>,
        client_id: u64,
        client_name: String,
        request_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner,
            slot: RwLock::new(slot),
            inbox: Mutex::new(inbox),
            client_id,
            client_name,
            request_id,
            sequence_id: AtomicU64::new(0),
        }
    }

    fn current_slot(&self) -> Arc<ConnectionSlot> {
        self.slot.read().unwrap().clone()
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) async fn send_command<Request>(&self, command: Request) -> Result<(), NeutronError>
    where
        Request: Into<Outbound>,
    {
        self.current_slot().handle()?.send(command.into()).await
    }

    pub(crate) async fn send_command_and_resolve<Request, Response>(
        &self,
        command: Request,
    ) -> Result<RecieptFuture<Response>, NeutronError>
    where
        Request: Into<Outbound>,
        Response: TryFrom<Inbound> + Send + 'static,
    {
        let rx = self
            .current_slot()
            .handle()?
            .request_deferred(command.into())
            .await?;

        Ok(Box::pin(async move {
            match rx.await {
                Ok(Ok(inbound)) => Response::try_from(inbound).map_err(|_| {
                    log::error!("Received a response of the wrong type");
                    NeutronError::Unresolvable
                }),
                Ok(Err(err)) => {
                    log::error!("Error resolving command: {}", err);
                    Err(err)
                }
                Err(_) => Err(NeutronError::ChannelTerminated),
            }
        }))
    }

    /// Move this client's binding onto the connection for
    /// `broker_address`, dialing it if necessary.
    async fn move_to_broker(&self, broker_address: BrokerAddress) -> Result<(), NeutronError> {
        let new_slot = self.inner.ensure_connection(&broker_address).await?;

        let inbox_tx = self
            .inner
            .registry
            .client_inbox(self.client_id)
            .ok_or(NeutronError::Disconnected)?;
        new_slot.handle()?.bind(self.client_id, inbox_tx).await?;

        let old_slot = self.current_slot();
        if old_slot.broker_address != broker_address {
            if let Ok(old) = old_slot.handle() {
                old.unbind(self.client_id).await;
            }
        }

        self.inner
            .registry
            .set_client_broker(self.client_id, broker_address);
        *self.slot.write().unwrap() = new_slot;
        Ok(())
    }

    /// Where the broker's lookup answer says this client should connect,
    /// or `None` to stay where it is.
    fn resolve_lookup(
        &self,
        current: &BrokerAddress,
        response: &LookupTopicResponse,
    ) -> Option<BrokerAddress> {
        let target = response.get_broker_service_url();
        if response.proxy {
            // Reach the named broker through the configured endpoint.
            let base = self.inner.config.broker_address();
            let address = BrokerAddress::Proxy {
                url: base.base_url().to_string(),
                proxy: target,
            };
            (&address != current).then_some(address)
        } else if target.is_empty() || target == current.to_string() {
            None
        } else {
            let address = BrokerAddress::Direct { url: target };
            (&address != current).then_some(address)
        }
    }
}

#[async_trait]
impl PulsarClient for Client {
    async fn next(&self) -> Result<Inbound, NeutronError> {
        self.inbox
            .lock()
            .await
            .recv()
            .await
            .ok_or(NeutronError::Disconnected)
    }

    async fn connect(&self) -> Result<Connected, NeutronError> {
        // The CONNECT handshake already happened when the connection was
        // established; hand back its cached response.
        Ok(self.current_slot().handle()?.connected())
    }

    async fn lookup_topic(&self, topic: &str) -> Result<(), NeutronError> {
        log::info!("Looking up topic {}", topic);
        for _ in 0..MAX_LOOKUP_HOPS {
            let lookup: LookupTopicResponse = self
                .send_command_and_resolve::<_, LookupTopicResponse>(LookupTopic {
                    request_id: self.next_request_id(),
                    topic: topic.to_string(),
                })
                .await?
                .await?;

            match lookup.response_type {
                LookupResponseType::Connect => {
                    let current = self.current_slot().broker_address.clone();
                    match self.resolve_lookup(&current, &lookup) {
                        None => return Ok(()),
                        Some(address) => {
                            log::info!("Topic {} served via {}", topic, address);
                            self.move_to_broker(address).await?;
                            // Ask again on the new connection for the
                            // authoritative answer.
                            continue;
                        }
                    }
                }
                LookupResponseType::Redirect => {
                    return Err(NeutronError::PulsarError(
                        "lookup redirects are not supported yet".to_string(),
                    ));
                }
                LookupResponseType::Failed => return Err(NeutronError::ConnectionFailed),
            }
        }
        Err(NeutronError::PulsarError(
            "topic lookup did not settle within the hop limit".to_string(),
        ))
    }

    async fn producer(&self, topic: &str) -> Result<(), NeutronError> {
        self.send_command_and_resolve::<_, ProducerSuccess>(message::Producer {
            producer_id: self.client_id,
            producer_name: Some(self.client_name.clone()),
            topic: topic.to_string(),
            request_id: self.next_request_id(),
        })
        .await?
        .await?;
        self.inner.registry.set_session(
            self.client_id,
            ClientSession::Producer {
                topic: topic.to_string(),
                producer_name: self.client_name.clone(),
            },
        );
        Ok(())
    }

    async fn subscribe(&self, topic: &str, subscription: &str) -> Result<(), NeutronError> {
        let sub_type = message::SubType::Shared;
        self.send_command_and_resolve::<_, Success>(Subscribe {
            topic: topic.to_string(),
            consumer_id: self.client_id,
            subscription: subscription.to_string(),
            request_id: self.next_request_id(),
            sub_type: sub_type.clone(),
        })
        .await?
        .await?;
        self.inner.registry.set_session(
            self.client_id,
            ClientSession::Consumer {
                topic: topic.to_string(),
                subscription: subscription.to_string(),
                sub_type,
                permits: 0,
            },
        );
        Ok(())
    }

    async fn ack(
        &self,
        message_id: &MessageIdData,
    ) -> Result<RecieptFuture<AckReciept>, NeutronError> {
        self.send_command_and_resolve::<_, AckReciept>(message::Ack {
            consumer_id: self.client_id,
            message_id: message_id.clone(),
            request_id: self.next_request_id(),
        })
        .await
    }

    async fn send_message(
        &self,
        payload: Vec<u8>,
    ) -> Result<RecieptFuture<SendReceipt>, NeutronError> {
        self.send_command_and_resolve::<_, SendReceipt>(message::Send::Single {
            producer_name: self.client_name.clone(),
            producer_id: self.client_id,
            sequence_id: self.sequence_id.fetch_add(1, Ordering::SeqCst),
            payload: payload.into(),
        })
        .await
    }

    async fn send_batch_message(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<RecieptFuture<SendReceipt>, NeutronError> {
        self.send_command_and_resolve::<_, SendReceipt>(message::Send::Batch {
            producer_name: self.client_name.clone(),
            producer_id: self.client_id,
            sequence_id: self.sequence_id.fetch_add(1, Ordering::SeqCst),
            payloads: payloads.into_iter().map(Into::into).collect(),
        })
        .await
    }

    async fn next_message(&self) -> Result<message::Message, NeutronError> {
        loop {
            let inbound = self.next().await?;
            match message::Message::try_from(inbound) {
                Ok(message) => return Ok(message),
                Err(_) => continue,
            }
        }
    }

    async fn flow(&self, message_permits: u32) -> Result<(), NeutronError> {
        self.send_command(message::Flow {
            message_permits,
            consumer_id: self.client_id,
        })
        .await?;
        self.inner
            .registry
            .set_flow_permits(self.client_id, message_permits);
        Ok(())
    }

    fn client_id(&self) -> u64 {
        self.client_id
    }

    fn client_name(&self) -> &str {
        &self.client_name
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.inner.registry.remove_client(self.client_id);
    }
}
