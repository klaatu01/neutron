use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::broker_address::BrokerAddress;
use crate::client::Client;
use crate::connection::{ConnectionHandle, PulsarConnection};
use crate::error::NeutronError;
use crate::message::{Connect, Flow, Subscribe};
use crate::registry::{ClientSession, ConnectionSlot, Registry, INBOX_CAPACITY};
use crate::AuthenticationPlugin;

/// How long a client operation waits for a reconnecting slot before its
/// transient failure is surfaced.
pub(crate) const RECONNECT_WAIT: Duration = Duration::from_secs(15);

/// Reconnect backoff: exponential from BASE to CAP, jittered by ±50% so a
/// fleet of clients does not stampede a recovering broker.
const BACKOFF_BASE: Duration = Duration::from_millis(200);
const BACKOFF_CAP: Duration = Duration::from_secs(30);

fn jittered(duration: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos())
        .unwrap_or(0);
    duration.mul_f64(0.5 + (nanos % 1000) as f64 / 1000.0)
}

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
}

impl PulsarConfig {
    pub fn broker_address(&self) -> BrokerAddress {
        BrokerAddress::Direct {
            url: format!("{}:{}", self.endpoint_url, self.endpoint_port),
        }
    }

    pub fn is_tls(&self) -> bool {
        self.endpoint_url.starts_with("pulsar+ssl://")
    }
}

/// The shared core behind every client: configuration, authentication,
/// and the registry of connections and clients. It is a table plus
/// methods — no dispatch task sits between a client and its socket.
pub(crate) struct PulsarInner {
    pub(crate) config: PulsarConfig,
    auth_plugin: Option<Arc<dyn AuthenticationPlugin + Send + Sync + 'static>>,
    pub(crate) registry: Registry,
    /// Serializes dialing so concurrent registrations to the same broker
    /// produce one connection, not two.
    dial_lock: tokio::sync::Mutex<()>,
    /// Request ids must be unique across every client sharing a
    /// connection — responses are correlated by them.
    request_id_generator: Arc<AtomicU64>,
}

impl PulsarInner {
    pub(crate) fn auth_plugin(&self) -> Option<Arc<dyn AuthenticationPlugin + Send + Sync>> {
        self.auth_plugin.clone()
    }

    pub(crate) fn next_request_id(&self) -> u64 {
        self.request_id_generator.fetch_add(1, Ordering::SeqCst)
    }

    async fn connect_command(
        &self,
        broker_address: &BrokerAddress,
    ) -> Result<Connect, NeutronError> {
        let mut connect = Connect {
            auth_data: None,
            auth_method_name: None,
            broker_address: Some(broker_address.clone()),
        };
        if let Some(auth) = &self.auth_plugin {
            connect.auth_data = Some(auth.auth_data().await?);
            connect.auth_method_name = Some(auth.auth_method_name());
        }
        Ok(connect)
    }

    /// Dial, handshake, and spawn the connection actor for one broker.
    pub(crate) async fn establish(
        self: &Arc<Self>,
        broker_address: &BrokerAddress,
    ) -> Result<Arc<ConnectionHandle>, NeutronError> {
        let connect = self.connect_command(broker_address).await?;
        let connection = PulsarConnection::connect(broker_address).await?;
        let (connected, connection) = connection.handshake(connect).await?;
        log::info!("Connected to {}", broker_address);
        Ok(connection.spawn(broker_address.clone(), connected, Arc::downgrade(self)))
    }

    /// The slot for `broker_address`, dialing a fresh connection if none
    /// exists yet. Clients keep the slot; the handle inside it changes as
    /// the supervisor replaces dead connections. A slot that is currently
    /// down is returned as-is — `wait_ready` is how callers ride out a
    /// reconnect in progress.
    pub(crate) async fn ensure_connection(
        self: &Arc<Self>,
        broker_address: &BrokerAddress,
    ) -> Result<Arc<ConnectionSlot>, NeutronError> {
        if let Some(slot) = self.registry.get_slot(broker_address) {
            return Ok(slot);
        }

        let _dialing = self.dial_lock.lock().await;
        // Someone else may have dialed while this task waited its turn.
        if let Some(slot) = self.registry.get_slot(broker_address) {
            return Ok(slot);
        }

        // The initial dial fails fast to the caller; only established
        // slots get a supervisor and reconnect semantics.
        let handle = self.establish(broker_address).await?;
        let slot = ConnectionSlot::new(broker_address.clone(), handle);
        self.registry.insert_slot(slot.clone());
        tokio::spawn(supervise(Arc::downgrade(self), slot.clone()));
        Ok(slot)
    }

    /// Called by a connection actor as it dies. Marks the slot down
    /// (unless a replacement already took it over), which wakes the
    /// supervisor to reconnect and replay.
    pub(crate) async fn connection_closed(
        &self,
        broker_address: &BrokerAddress,
        dead: Weak<ConnectionHandle>,
    ) {
        if let Some(slot) = self.registry.get_slot(broker_address) {
            if slot.set_down_if_current(&dead) {
                log::warn!(
                    "Connection to {} lost; supervisor is reconnecting",
                    broker_address
                );
            }
        }
    }

    /// Rebuild every session bound to `broker_address` on a fresh
    /// connection: rebind each client's existing inbox, re-subscribe
    /// consumers and re-issue their flow credit, re-register producers.
    /// Consumers blocked in `next_message` never notice beyond latency —
    /// their inbox channel is the same one, now fed by the new reader.
    async fn replay_sessions(&self, broker_address: &BrokerAddress, handle: &ConnectionHandle) {
        for (id, inbox_tx, session) in self.registry.clients_on(broker_address) {
            if let Err(e) = self.replay_client(id, inbox_tx, session, handle).await {
                log::error!("Failed to replay client {} after reconnect: {}", id, e);
            }
        }
    }

    async fn replay_client(
        &self,
        id: u64,
        inbox_tx: mpsc::Sender<crate::message::Inbound>,
        session: ClientSession,
        handle: &ConnectionHandle,
    ) -> Result<(), NeutronError> {
        handle.bind(id, inbox_tx).await?;
        match session {
            ClientSession::Unregistered => {}
            ClientSession::Consumer {
                topic,
                subscription,
                sub_type,
                permits,
            } => {
                handle
                    .request(
                        Subscribe {
                            topic,
                            subscription,
                            consumer_id: id,
                            request_id: self.next_request_id(),
                            sub_type,
                        }
                        .into(),
                    )
                    .await?;
                if permits > 0 {
                    handle
                        .send(
                            Flow {
                                consumer_id: id,
                                message_permits: permits,
                            }
                            .into(),
                        )
                        .await?;
                }
                log::info!("Replayed consumer {} onto {}", id, handle.broker_address);
            }
            ClientSession::Producer {
                topic,
                producer_name,
            } => {
                handle
                    .request(
                        crate::message::Producer {
                            producer_id: id,
                            producer_name: Some(producer_name),
                            topic,
                            request_id: self.next_request_id(),
                        }
                        .into(),
                    )
                    .await?;
                log::info!("Replayed producer {} onto {}", id, handle.broker_address);
            }
        }
        Ok(())
    }
}

/// One supervisor per broker slot: sleeps until the connection dies, then
/// re-establishes it under jittered exponential backoff and replays every
/// session that was bound to it. Exits when the Pulsar instance is gone.
async fn supervise(inner: Weak<PulsarInner>, slot: Arc<ConnectionSlot>) {
    loop {
        slot.wait_down().await;

        let mut backoff = BACKOFF_BASE;
        loop {
            let Some(inner) = inner.upgrade() else { return };
            match inner.establish(&slot.broker_address).await {
                Ok(handle) => {
                    slot.set_ready(handle.clone());
                    inner.replay_sessions(&slot.broker_address, &handle).await;
                    log::info!("Reconnected to {}", slot.broker_address);
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "Reconnect to {} failed ({}); retrying in {:?}",
                        slot.broker_address,
                        e,
                        backoff
                    );
                }
            }
            drop(inner);
            tokio::time::sleep(jittered(backoff)).await;
            backoff = (backoff * 2).min(BACKOFF_CAP);
        }
    }
}

pub struct Pulsar {
    pub(crate) config: PulsarConfig,
    pub(crate) auth_plugin: Option<Box<dyn AuthenticationPlugin + Sync + Send + 'static>>,
}

impl Pulsar {
    pub fn new(config: PulsarConfig) -> Self {
        Self {
            config,
            auth_plugin: None,
        }
    }

    pub fn run(self) -> PulsarManager {
        PulsarManager::new(Arc::new(PulsarInner {
            config: self.config,
            auth_plugin: self.auth_plugin.map(Arc::from),
            registry: Registry::new(),
            dial_lock: tokio::sync::Mutex::new(()),
            request_id_generator: Arc::new(AtomicU64::new(0)),
        }))
    }
}

pub struct PulsarBuilder {
    config: Option<PulsarConfig>,
    auth_plugin: Option<Box<dyn AuthenticationPlugin + Send + Sync + 'static>>,
}

impl Default for PulsarBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PulsarBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            auth_plugin: None,
        }
    }

    pub fn with_config(mut self, config: PulsarConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_auth_plugin<T>(mut self, auth_plugin: T) -> Self
    where
        T: AuthenticationPlugin + Send + Sync + 'static,
    {
        self.auth_plugin = Some(Box::new(auth_plugin));
        self
    }

    pub fn build(self) -> Pulsar {
        Pulsar {
            config: self.config.unwrap(),
            auth_plugin: self.auth_plugin,
        }
    }
}

pub struct PulsarManager {
    client_id_generator: AtomicU64,
    inner: Arc<PulsarInner>,
}

impl PulsarManager {
    pub(crate) fn new(inner: Arc<PulsarInner>) -> Self {
        Self {
            client_id_generator: AtomicU64::new(0),
            inner,
        }
    }

    pub(crate) fn new_client_id(&self) -> u64 {
        self.client_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn request_id_generator(&self) -> Arc<AtomicU64> {
        self.inner.request_id_generator.clone()
    }

    /// Create a client bound to the base broker: ensure the connection is
    /// live, wire an inbox into its reader, and record the client in the
    /// registry. Topic lookup may later move the client to another
    /// broker's connection.
    pub(crate) async fn register(
        &self,
        client_id: u64,
        client_name: String,
    ) -> Result<Client, NeutronError> {
        let base = self.inner.config.broker_address();
        let slot = self.inner.ensure_connection(&base).await?;

        let (inbox_tx, inbox_rx) = mpsc::channel(INBOX_CAPACITY);
        slot.wait_ready(RECONNECT_WAIT)
            .await?
            .bind(client_id, inbox_tx.clone())
            .await?;
        self.inner
            .registry
            .add_client(client_id, inbox_tx, base.clone());

        Ok(Client::new(
            self.inner.clone(),
            slot,
            inbox_rx,
            client_id,
            client_name,
            self.request_id_generator(),
        ))
    }
}
