use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::broker_address::BrokerAddress;
use crate::client::Client;
use crate::connection::{ConnectionHandle, PulsarConnection};
use crate::error::NeutronError;
use crate::message::Connect;
use crate::registry::{ConnectionSlot, Registry, INBOX_CAPACITY};
use crate::AuthenticationPlugin;

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
}

impl PulsarInner {
    pub(crate) fn auth_plugin(&self) -> Option<Arc<dyn AuthenticationPlugin + Send + Sync>> {
        self.auth_plugin.clone()
    }

    async fn connect_command(&self, broker_address: &BrokerAddress) -> Result<Connect, NeutronError> {
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
        Ok(connection.spawn(
            broker_address.clone(),
            connected,
            Arc::downgrade(self),
        ))
    }

    /// The slot for `broker_address`, dialing a fresh connection if none
    /// is live. Clients keep the slot; the handle inside it can change.
    pub(crate) async fn ensure_connection(
        self: &Arc<Self>,
        broker_address: &BrokerAddress,
    ) -> Result<Arc<ConnectionSlot>, NeutronError> {
        if let Some(slot) = self.registry.get_slot(broker_address) {
            if slot.is_ready() {
                return Ok(slot);
            }
        }

        let _dialing = self.dial_lock.lock().await;
        // Someone else may have dialed while this task waited its turn.
        if let Some(slot) = self.registry.get_slot(broker_address) {
            if slot.is_ready() {
                return Ok(slot);
            }
        }

        let handle = self.establish(broker_address).await?;
        let slot = ConnectionSlot::new(broker_address.clone(), handle);
        self.registry.insert_slot(slot.clone());
        Ok(slot)
    }

    /// Called by a connection actor as it dies. Closes the slot (unless a
    /// replacement already took it over) and disconnects the clients that
    /// were bound to it, so nothing blocks on a connection that no longer
    /// exists.
    pub(crate) async fn connection_closed(
        &self,
        broker_address: &BrokerAddress,
        dead: std::sync::Weak<ConnectionHandle>,
    ) {
        if let Some(slot) = self.registry.get_slot(broker_address) {
            if !slot.set_closed_if_current(&dead) {
                // A newer connection owns this slot; nothing to do.
                return;
            }
            self.registry.remove_slot(broker_address);
        }
        let dropped = self.registry.disconnect_clients_of(broker_address);
        if dropped > 0 {
            log::warn!(
                "Connection to {} closed; {} client(s) disconnected",
                broker_address,
                dropped
            );
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
    request_id_generator: Arc<AtomicU64>,
    inner: Arc<PulsarInner>,
}

impl PulsarManager {
    pub(crate) fn new(inner: Arc<PulsarInner>) -> Self {
        Self {
            client_id_generator: AtomicU64::new(0),
            request_id_generator: Arc::new(AtomicU64::new(0)),
            inner,
        }
    }

    pub(crate) fn new_client_id(&self) -> u64 {
        self.client_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Request ids must be unique across every client sharing a
    /// connection — responses are correlated by them — so allocation is
    /// owned here rather than per client.
    pub(crate) fn request_id_generator(&self) -> Arc<AtomicU64> {
        self.request_id_generator.clone()
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
        slot.handle()?.bind(client_id, inbox_tx.clone()).await?;
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
