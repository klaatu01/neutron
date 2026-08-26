use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::time::Instant;

use crate::message::{Inbound, Outbound};
use crate::NeutronError;

/// How long a request may wait for its response before the sweeper fails it.
pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// How often expired entries are swept.
const SWEEP_PERIOD: Duration = Duration::from_secs(1);

/// The identity under which an in-flight request awaits its response.
///
/// Every key is derived from an identifier the broker echoes back on the
/// wire, so concurrent requests of the same kind can never collide:
/// control commands carry a `request_id`, and send receipts carry the
/// `(producer_id, sequence_id)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CorrelationKey {
    /// Control commands (lookup, subscribe, producer, ack): the wire
    /// `request_id`, unique per allocator by construction.
    RequestId(u64),
    /// `CommandSendReceipt` carries no request id; receipts correlate on
    /// the producer's own sequence numbering.
    ProducerSequence { producer_id: u64, sequence_id: u64 },
    /// The CONNECT handshake: `CONNECTED` carries no id, so at most one
    /// may be in flight per table.
    Connect,
}

impl CorrelationKey {
    /// The key under which this outbound command will await a response,
    /// or `None` for fire-and-forget commands (ping, pong, flow, auth).
    pub(crate) fn of_outbound(outbound: &Outbound) -> Option<CorrelationKey> {
        match outbound {
            Outbound::Connect(_) => Some(CorrelationKey::Connect),
            Outbound::Send(send) => Some(CorrelationKey::ProducerSequence {
                producer_id: send.producer_id(),
                sequence_id: send.sequence_id(),
            }),
            Outbound::Ack(acks) => acks
                .first()
                .map(|ack| CorrelationKey::RequestId(ack.request_id)),
            Outbound::LookupTopic(lookup) => Some(CorrelationKey::RequestId(lookup.request_id)),
            Outbound::Subscribe(subscribe) => {
                Some(CorrelationKey::RequestId(subscribe.request_id))
            }
            Outbound::Producer(producer) => Some(CorrelationKey::RequestId(producer.request_id)),
            Outbound::Ping | Outbound::Pong | Outbound::Flow(_) | Outbound::AuthChallenge(_) => {
                None
            }
        }
    }

    /// The key this inbound frame resolves, or `None` if it is not a
    /// response (messages, pings, auth challenges).
    pub(crate) fn of_inbound(inbound: &Inbound) -> Option<CorrelationKey> {
        match inbound {
            Inbound::Connected(_) => Some(CorrelationKey::Connect),
            Inbound::SendReceipt(receipt) => Some(CorrelationKey::ProducerSequence {
                producer_id: receipt.producer_id,
                sequence_id: receipt.sequence_id,
            }),
            Inbound::AckReciept(receipt) => Some(CorrelationKey::RequestId(receipt.request_id)),
            Inbound::LookupTopicResponse(response) => {
                Some(CorrelationKey::RequestId(response.request_id))
            }
            Inbound::Success(success) => Some(CorrelationKey::RequestId(success.request_id)),
            Inbound::ProducerSuccess(success) => {
                Some(CorrelationKey::RequestId(success.request_id))
            }
            Inbound::Ping | Inbound::Pong | Inbound::Message(_) | Inbound::AuthChallengeRequest(_) => {
                None
            }
        }
    }
}

pub(crate) type ResponseSender =
    futures::channel::oneshot::Sender<Result<Inbound, NeutronError>>;

struct Pending {
    tx: ResponseSender,
    deadline: Instant,
}

/// The table of requests awaiting responses.
///
/// Every entry leaves the table exactly one way: resolved by a matching
/// response, failed by the deadline sweeper, or failed by [`drain`] on
/// teardown. Entries can never be silently evicted — a duplicate key is
/// rejected at registration instead of overwriting the existing waiter.
///
/// The lock is a plain [`std::sync::Mutex`]: it is never held across an
/// await point, and the critical sections are single map operations.
///
/// [`drain`]: Inflight::drain
pub(crate) struct Inflight {
    entries: Mutex<HashMap<CorrelationKey, Pending>>,
    timeout: Duration,
    sweeper_started: AtomicBool,
}

impl Inflight {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_timeout(OPERATION_TIMEOUT)
    }

    pub(crate) fn with_timeout(timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            timeout,
            sweeper_started: AtomicBool::new(false),
        })
    }

    /// Register a waiter under `key`. Rejects a key that is already
    /// occupied — the existing waiter keeps its entry, and the rejected
    /// sender is completed with [`NeutronError::DuplicateRequest`].
    pub(crate) fn register(&self, key: CorrelationKey, tx: ResponseSender) {
        let mut entries = self.entries.lock().unwrap();
        match entries.entry(key) {
            std::collections::hash_map::Entry::Occupied(occupied) => {
                log::error!(
                    "duplicate in-flight correlation key {:?}; rejecting the new request",
                    occupied.key()
                );
                let _ = tx.send(Err(NeutronError::DuplicateRequest));
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(Pending {
                    tx,
                    deadline: Instant::now() + self.timeout,
                });
            }
        }
    }

    /// Complete the waiter matching this inbound frame, if one exists.
    /// Returns whether the frame was consumed as a response.
    pub(crate) fn try_resolve(&self, inbound: &Inbound) -> bool {
        let Some(key) = CorrelationKey::of_inbound(inbound) else {
            return false;
        };
        let pending = self.entries.lock().unwrap().remove(&key);
        match pending {
            Some(pending) => {
                let _ = pending.tx.send(Ok(inbound.clone()));
                true
            }
            None => false,
        }
    }

    /// Fail every entry whose deadline has passed with
    /// [`NeutronError::OperationTimeout`]. Returns how many expired.
    pub(crate) fn sweep(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<Pending> = {
            let mut entries = self.entries.lock().unwrap();
            let keys: Vec<CorrelationKey> = entries
                .iter()
                .filter(|(_, pending)| pending.deadline <= now)
                .map(|(key, _)| key.clone())
                .collect();
            keys.into_iter()
                .filter_map(|key| entries.remove(&key))
                .collect()
        };
        let count = expired.len();
        for pending in expired {
            let _ = pending.tx.send(Err(NeutronError::OperationTimeout));
        }
        if count > 0 {
            log::warn!("{} in-flight request(s) timed out", count);
        }
        count
    }

    /// Fail every remaining entry with `err`. Called on connection
    /// teardown so no waiter ever hangs past the connection's life.
    pub(crate) fn drain(&self, err: NeutronError) {
        let entries: Vec<Pending> = {
            let mut map = self.entries.lock().unwrap();
            map.drain().map(|(_, pending)| pending).collect()
        };
        for pending in entries {
            let _ = pending.tx.send(Err(err.clone()));
        }
    }

    /// Spawn the deadline sweeper for this table. The task holds a weak
    /// reference and exits when the table is dropped. Idempotent.
    ///
    /// Must be called from within a tokio runtime.
    pub(crate) fn start_sweeper(self: &Arc<Self>) {
        if self.sweeper_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak: Weak<Inflight> = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_PERIOD);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match weak.upgrade() {
                    Some(table) => {
                        table.sweep();
                    }
                    None => break,
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::proto::pulsar::MessageIdData;
    use crate::message::{Ack, AckReciept, Inbound, Outbound, Send, SendReceipt};

    fn ack(consumer_id: u64, request_id: u64) -> Outbound {
        Outbound::Ack(vec![Ack {
            consumer_id,
            message_id: MessageIdData::new(),
            request_id,
        }])
    }

    fn send(producer_id: u64, sequence_id: u64) -> Outbound {
        Outbound::Send(Send::Single {
            producer_name: format!("producer-{}", producer_id),
            producer_id,
            sequence_id,
            payload: bytes::Bytes::new(),
        })
    }

    fn register(inflight: &Inflight, outbound: &Outbound) -> futures::channel::oneshot::Receiver<Result<Inbound, NeutronError>> {
        let (tx, rx) = futures::channel::oneshot::channel();
        inflight.register(CorrelationKey::of_outbound(outbound).unwrap(), tx);
        rx
    }

    /// Two consumers ack concurrently; each receives its own receipt.
    #[tokio::test]
    async fn concurrent_acks_resolve_to_their_own_waiters() {
        let inflight = Inflight::new();
        let rx_a = register(&inflight, &ack(1, 100));
        let rx_b = register(&inflight, &ack(2, 200));

        assert!(inflight.try_resolve(&Inbound::AckReciept(AckReciept {
            consumer_id: 2,
            request_id: 200,
        })));
        assert!(inflight.try_resolve(&Inbound::AckReciept(AckReciept {
            consumer_id: 1,
            request_id: 100,
        })));

        match rx_a.await.unwrap().unwrap() {
            Inbound::AckReciept(receipt) => assert_eq!(receipt.consumer_id, 1),
            other => panic!("unexpected inbound: {:?}", other),
        }
        match rx_b.await.unwrap().unwrap() {
            Inbound::AckReciept(receipt) => assert_eq!(receipt.consumer_id, 2),
            other => panic!("unexpected inbound: {:?}", other),
        }
    }

    /// Two producers publish with the same sequence id; the producer id
    /// disambiguates.
    #[tokio::test]
    async fn concurrent_sends_resolve_by_producer_and_sequence() {
        let inflight = Inflight::new();
        let rx_a = register(&inflight, &send(1, 0));
        let rx_b = register(&inflight, &send(2, 0));

        assert!(inflight.try_resolve(&Inbound::SendReceipt(SendReceipt {
            producer_id: 1,
            sequence_id: 0,
            message_id: MessageIdData::new(),
        })));

        match rx_a.await.unwrap().unwrap() {
            Inbound::SendReceipt(receipt) => assert_eq!(receipt.producer_id, 1),
            other => panic!("unexpected inbound: {:?}", other),
        }

        assert!(inflight.try_resolve(&Inbound::SendReceipt(SendReceipt {
            producer_id: 2,
            sequence_id: 0,
            message_id: MessageIdData::new(),
        })));
        match rx_b.await.unwrap().unwrap() {
            Inbound::SendReceipt(receipt) => assert_eq!(receipt.producer_id, 2),
            other => panic!("unexpected inbound: {:?}", other),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entries_fail_with_timeout() {
        let inflight = Inflight::with_timeout(Duration::from_secs(5));
        let rx = register(&inflight, &ack(1, 1));

        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(inflight.sweep(), 0);

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(inflight.sweep(), 1);

        assert!(matches!(
            rx.await.unwrap(),
            Err(NeutronError::OperationTimeout)
        ));
    }

    #[tokio::test]
    async fn drain_fails_every_waiter() {
        let inflight = Inflight::new();
        let rx_a = register(&inflight, &ack(1, 1));
        let rx_b = register(&inflight, &send(1, 7));

        inflight.drain(NeutronError::Disconnected);

        assert!(matches!(rx_a.await.unwrap(), Err(NeutronError::Disconnected)));
        assert!(matches!(rx_b.await.unwrap(), Err(NeutronError::Disconnected)));
    }

    /// A duplicate key rejects the new request; the original waiter is
    /// untouched.
    #[tokio::test]
    async fn duplicate_key_rejects_the_new_request_only() {
        let inflight = Inflight::new();
        let rx_first = register(&inflight, &ack(1, 42));
        let rx_dup = register(&inflight, &ack(9, 42));

        assert!(matches!(
            rx_dup.await.unwrap(),
            Err(NeutronError::DuplicateRequest)
        ));

        assert!(inflight.try_resolve(&Inbound::AckReciept(AckReciept {
            consumer_id: 1,
            request_id: 42,
        })));
        match rx_first.await.unwrap().unwrap() {
            Inbound::AckReciept(receipt) => assert_eq!(receipt.consumer_id, 1),
            other => panic!("unexpected inbound: {:?}", other),
        }
    }
}
