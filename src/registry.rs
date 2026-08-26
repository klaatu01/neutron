use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::broker_address::BrokerAddress;
use crate::connection::ConnectionHandle;
use crate::message::{Inbound, SubType};
use crate::NeutronError;

/// Capacity of each client's inbox. This number is coupled to consumer
/// flow control: the broker is granted at most `message_permits * 2 +
/// overflow` credit (see `Consumer::check_and_flow`), and with permits at
/// 250 outstanding credit stays well below this capacity even across a
/// reconnect's fresh grant — so the reader's `try_send` into an inbox
/// succeeds by construction, and a full inbox means the credit accounting
/// is broken, not that the consumer is merely slow.
pub(crate) const INBOX_CAPACITY: usize = 2048;

/// What a client asked the broker for — everything needed to re-establish
/// its session on a fresh connection.
#[derive(Debug, Clone)]
pub(crate) enum ClientSession {
    Unregistered,
    Consumer {
        topic: String,
        subscription: String,
        sub_type: SubType,
        /// The most recent flow grant, re-issued on replay.
        permits: u32,
    },
    Producer {
        topic: String,
        producer_name: String,
    },
}

/// Registry bookkeeping for one consumer or producer.
pub(crate) struct ClientEntry {
    /// The sending side of the client's inbox, held here so a fresh
    /// connection can be bound to the same channel the client is already
    /// reading from.
    pub(crate) inbox_tx: mpsc::Sender<Inbound>,
    pub(crate) broker_address: BrokerAddress,
    pub(crate) session: ClientSession,
}

enum SlotState {
    Ready(Arc<ConnectionHandle>),
    Down,
}

/// A named place a connection lives. Clients hold the slot, not the
/// connection: the handle inside can be replaced while clients are
/// mid-flight, which is what makes reconnection possible at all.
pub(crate) struct ConnectionSlot {
    pub(crate) broker_address: BrokerAddress,
    state: Mutex<SlotState>,
    /// Bumped on every state change; `wait_ready` / `wait_down` park on
    /// it instead of polling.
    revision: watch::Sender<u64>,
}

impl ConnectionSlot {
    pub(crate) fn new(broker_address: BrokerAddress, handle: Arc<ConnectionHandle>) -> Arc<Self> {
        let (revision, _) = watch::channel(0);
        Arc::new(Self {
            broker_address,
            state: Mutex::new(SlotState::Ready(handle)),
            revision,
        })
    }

    fn bump(&self) {
        self.revision.send_modify(|revision| *revision += 1);
    }

    /// The current connection, or `Disconnected` while the slot is down.
    pub(crate) fn handle(&self) -> Result<Arc<ConnectionHandle>, NeutronError> {
        match &*self.state.lock().unwrap() {
            SlotState::Ready(handle) => Ok(handle.clone()),
            SlotState::Down => Err(NeutronError::Disconnected),
        }
    }

    pub(crate) fn set_ready(&self, handle: Arc<ConnectionHandle>) {
        *self.state.lock().unwrap() = SlotState::Ready(handle);
        self.bump();
    }

    /// Mark the slot down, but only if `dead` is still the handle it
    /// holds — a death notice for a connection that has already been
    /// replaced must not take down its replacement. Returns whether the
    /// slot transitioned.
    pub(crate) fn set_down_if_current(&self, dead: &Weak<ConnectionHandle>) -> bool {
        let mut state = self.state.lock().unwrap();
        match &*state {
            SlotState::Ready(current) if Weak::as_ptr(dead) == Arc::as_ptr(current) => {
                *state = SlotState::Down;
                drop(state);
                self.bump();
                true
            }
            SlotState::Ready(_) => false,
            SlotState::Down => false,
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        matches!(&*self.state.lock().unwrap(), SlotState::Ready(_))
    }

    /// Wait until the slot holds a live connection, up to `timeout`.
    pub(crate) async fn wait_ready(
        &self,
        timeout: Duration,
    ) -> Result<Arc<ConnectionHandle>, NeutronError> {
        let mut revision = self.revision.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(handle) = self.handle() {
                return Ok(handle);
            }
            match tokio::time::timeout_at(deadline, revision.changed()).await {
                Ok(Ok(())) => continue,
                // The slot can no longer change state, or time is up.
                Ok(Err(_)) | Err(_) => return Err(NeutronError::Disconnected),
            }
        }
    }

    /// Wait until the slot is down (its connection died).
    pub(crate) async fn wait_down(&self) {
        let mut revision = self.revision.subscribe();
        loop {
            if !self.is_ready() {
                return;
            }
            if revision.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Shared routing state: which broker each client is bound to, and which
/// slot serves each broker. This is a table, not a task — clients and
/// connection actors index it directly, and no message ever queues here.
pub(crate) struct Registry {
    connections: Mutex<HashMap<BrokerAddress, Arc<ConnectionSlot>>>,
    clients: Mutex<HashMap<u64, ClientEntry>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn get_slot(&self, broker_address: &BrokerAddress) -> Option<Arc<ConnectionSlot>> {
        self.connections.lock().unwrap().get(broker_address).cloned()
    }

    pub(crate) fn insert_slot(&self, slot: Arc<ConnectionSlot>) {
        self.connections
            .lock()
            .unwrap()
            .insert(slot.broker_address.clone(), slot);
    }

    pub(crate) fn add_client(
        &self,
        id: u64,
        inbox_tx: mpsc::Sender<Inbound>,
        broker_address: BrokerAddress,
    ) {
        self.clients.lock().unwrap().insert(
            id,
            ClientEntry {
                inbox_tx,
                broker_address,
                session: ClientSession::Unregistered,
            },
        );
    }

    pub(crate) fn remove_client(&self, id: u64) {
        self.clients.lock().unwrap().remove(&id);
    }

    pub(crate) fn set_client_broker(&self, id: u64, broker_address: BrokerAddress) {
        if let Some(entry) = self.clients.lock().unwrap().get_mut(&id) {
            entry.broker_address = broker_address;
        }
    }

    pub(crate) fn set_session(&self, id: u64, session: ClientSession) {
        if let Some(entry) = self.clients.lock().unwrap().get_mut(&id) {
            entry.session = session;
        }
    }

    pub(crate) fn set_flow_permits(&self, id: u64, message_permits: u32) {
        if let Some(entry) = self.clients.lock().unwrap().get_mut(&id) {
            if let ClientSession::Consumer { permits, .. } = &mut entry.session {
                *permits = message_permits;
            }
        }
    }

    pub(crate) fn client_inbox(&self, id: u64) -> Option<mpsc::Sender<Inbound>> {
        self.clients
            .lock()
            .unwrap()
            .get(&id)
            .map(|entry| entry.inbox_tx.clone())
    }

    /// Everything needed to replay the clients bound to one broker onto a
    /// fresh connection.
    pub(crate) fn clients_on(
        &self,
        broker_address: &BrokerAddress,
    ) -> Vec<(u64, mpsc::Sender<Inbound>, ClientSession)> {
        self.clients
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| &entry.broker_address == broker_address)
            .map(|(id, entry)| (*id, entry.inbox_tx.clone(), entry.session.clone()))
            .collect()
    }
}
