//! A minimal in-process Pulsar broker for integration tests and client
//! benchmarks: real TCP, real frames through the production `Codec`,
//! scripted just enough to exercise a client's full path (handshake,
//! lookup, subscribe, flow, publish, ack) and its failure modes.
//!
//! Compiled for tests, and for benches behind the `bench` feature (the
//! `bench_broker` example serves it to external clients — including
//! other Pulsar client implementations).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use protobuf::MessageField;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::codec::{Codec, Payload};
use crate::message::proto::pulsar as proto;
use crate::message::proto::pulsar::base_command::Type;
use crate::message::MessageCommand;

#[derive(Debug, Clone, PartialEq)]
pub enum BrokerEvent {
    Connect,
    Lookup {
        topic: String,
    },
    Subscribe {
        consumer_id: u64,
        topic: String,
        subscription: String,
    },
    Flow {
        consumer_id: u64,
        permits: u32,
    },
    Ack {
        consumer_id: u64,
        request_id: u64,
    },
    Producer {
        producer_id: u64,
        topic: String,
    },
    Send {
        producer_id: u64,
        sequence_id: u64,
    },
}

enum PushCmd {
    Frame(Box<MessageCommand>),
    /// Respond to every held ack, most recent first.
    ReleaseHeldAcksReversed,
    Shutdown,
}

/// When set, every FLOW is answered by pushing `permits` messages of
/// `payload_size` bytes to that consumer, until `remaining` runs dry —
/// the broker becomes a load generator paced by the client's own flow
/// control.
struct AutoFeed {
    payload_size: usize,
    remaining: AtomicU64,
}

struct BrokerState {
    events: mpsc::UnboundedSender<BrokerEvent>,
    /// When set, ACKs are held unanswered until released.
    hold_acks: AtomicBool,
    /// brokerServiceUrl to advertise in lookup responses; empty means
    /// "this broker".
    advertise: Mutex<String>,
    service_url: String,
    auto_feed: Mutex<Option<Arc<AutoFeed>>>,
    /// Benchmarks turn event recording off: an unbounded event log
    /// nobody drains would grow with every send.
    record_events: AtomicBool,
    pushers: Mutex<Vec<mpsc::UnboundedSender<PushCmd>>>,
}

impl BrokerState {
    fn record(&self, event: BrokerEvent) {
        if self.record_events.load(Ordering::Relaxed) {
            let _ = self.events.send(event);
        }
    }
}

pub struct FakeBroker {
    pub port: u16,
    state: Arc<BrokerState>,
    events: tokio::sync::Mutex<mpsc::UnboundedReceiver<BrokerEvent>>,
}

impl FakeBroker {
    pub async fn start() -> Self {
        Self::start_on(0).await
    }

    pub async fn start_on(port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let state = Arc::new(BrokerState {
            events: events_tx,
            hold_acks: AtomicBool::new(false),
            advertise: Mutex::new(String::new()),
            service_url: format!("pulsar://127.0.0.1:{}", port),
            auto_feed: Mutex::new(None),
            record_events: AtomicBool::new(true),
            pushers: Mutex::new(Vec::new()),
        });

        tokio::spawn({
            let state = state.clone();
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    stream.set_nodelay(true).ok();
                    tokio::spawn(serve_connection(stream, state.clone()));
                }
            }
        });

        Self {
            port,
            state,
            events: tokio::sync::Mutex::new(events_rx),
        }
    }

    pub fn service_url(&self) -> String {
        self.state.service_url.clone()
    }

    pub fn config(&self) -> crate::PulsarConfig {
        crate::PulsarConfig {
            endpoint_url: "pulsar://127.0.0.1".to_string(),
            endpoint_port: self.port,
        }
    }

    /// Advertise another broker's URL in lookup responses, so clients
    /// move there.
    pub fn advertise(&self, url: &str) {
        *self.state.advertise.lock().unwrap() = url.to_string();
    }

    pub fn hold_acks(&self) {
        self.state.hold_acks.store(true, Ordering::SeqCst);
    }

    /// Answer every held ack, most recent first.
    pub fn release_held_acks_reversed(&self) {
        for pusher in self.state.pushers.lock().unwrap().iter() {
            let _ = pusher.send(PushCmd::ReleaseHeldAcksReversed);
        }
    }

    /// Serve `total` messages of `payload_size` bytes to consumers, paced
    /// by their own FLOW credit.
    pub fn auto_feed(&self, payload_size: usize, total: u64) {
        *self.state.auto_feed.lock().unwrap() = Some(Arc::new(AutoFeed {
            payload_size,
            remaining: AtomicU64::new(total),
        }));
    }

    /// Stop recording protocol events.
    pub fn quiet(&self) {
        self.state.record_events.store(false, Ordering::SeqCst);
    }

    /// Push a single message to a consumer over every live connection.
    pub fn push_message(&self, consumer_id: u64, entry_id: u64, data: &[u8]) {
        let frame = message_frame(consumer_id, entry_id, data);
        for pusher in self.state.pushers.lock().unwrap().iter() {
            let _ = pusher.send(PushCmd::Frame(Box::new(frame.clone())));
        }
    }

    /// Drop every live connection at the socket, as a crashing broker
    /// would.
    pub fn kill_connections(&self) {
        for pusher in self.state.pushers.lock().unwrap().drain(..) {
            let _ = pusher.send(PushCmd::Shutdown);
        }
    }

    /// The next recorded protocol event.
    pub async fn next_event(&self) -> BrokerEvent {
        self.events
            .lock()
            .await
            .recv()
            .await
            .expect("broker event stream ended")
    }

    /// Wait until `want` shows up, skipping unrelated events.
    pub async fn expect_event(&self, want: BrokerEvent) {
        loop {
            let got = self.next_event().await;
            if got == want {
                return;
            }
        }
    }

    /// Wait for the next event matching `pred` and return it.
    pub async fn wait_for(&self, pred: impl Fn(&BrokerEvent) -> bool) -> BrokerEvent {
        loop {
            let got = self.next_event().await;
            if pred(&got) {
                return got;
            }
        }
    }
}

/// How many inbound frames are pulled per wake, and so how many
/// responses share one flush.
const SERVE_BATCH: usize = 256;

async fn serve_connection(stream: TcpStream, state: Arc<BrokerState>) {
    let framed = Framed::new(stream, Codec);
    let (mut sink, stream) = framed.split();
    let mut stream = stream.ready_chunks(SERVE_BATCH);
    let (push_tx, mut push_rx) = mpsc::unbounded_channel();
    state.pushers.lock().unwrap().push(push_tx);
    let mut held_acks: Vec<MessageCommand> = Vec::new();
    // Per-connection entry id counter for auto-fed messages.
    let mut next_entry_id: u64 = 0;

    'connection: loop {
        tokio::select! {
            Some(cmd) = push_rx.recv() => match cmd {
                PushCmd::Frame(frame) => {
                    if sink.send(*frame).await.is_err() {
                        break;
                    }
                }
                PushCmd::ReleaseHeldAcksReversed => {
                    for response in held_acks.drain(..).rev().collect::<Vec<_>>() {
                        if sink.send(response).await.is_err() {
                            return;
                        }
                    }
                }
                PushCmd::Shutdown => break,
            },
            chunk = stream.next() => {
                let Some(chunk) = chunk else { break };
                let mut responded = false;
                for frame in chunk {
                    let Ok(frame) = frame else { break 'connection };
                    // FLOW with auto-feed fans one frame out into many;
                    // handled apart from the one-in-one-out commands.
                    if frame.command.type_() == Type::FLOW {
                        let flow = &frame.command.flow;
                        state.record(BrokerEvent::Flow {
                            consumer_id: flow.consumer_id(),
                            permits: flow.messagePermits(),
                        });
                        let feed = state.auto_feed.lock().unwrap().clone();
                        if let Some(feed) = feed {
                            let payload = vec![0x6eu8; feed.payload_size];
                            for _ in 0..flow.messagePermits() {
                                let credit = feed.remaining.fetch_update(
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                    |remaining| remaining.checked_sub(1),
                                );
                                if credit.is_err() {
                                    break;
                                }
                                let entry_id = next_entry_id;
                                next_entry_id += 1;
                                let message =
                                    message_frame(flow.consumer_id(), entry_id, &payload);
                                if sink.feed(message).await.is_err() {
                                    break 'connection;
                                }
                                responded = true;
                            }
                        }
                        continue;
                    }
                    if let Some(response) = respond(frame, &state, &mut held_acks) {
                        if sink.feed(response).await.is_err() {
                            break 'connection;
                        }
                        responded = true;
                    }
                }
                if responded && sink.flush().await.is_err() {
                    break;
                }
            }
        }
    }
}

/// The broker's reaction to one client frame: an event on the log and
/// usually a response frame.
fn respond(
    frame: MessageCommand,
    state: &BrokerState,
    held_acks: &mut Vec<MessageCommand>,
) -> Option<MessageCommand> {
    let command = &frame.command;
    match command.type_() {
        Type::CONNECT => {
            state.record(BrokerEvent::Connect);
            let mut connected = proto::CommandConnected::new();
            connected.set_server_version("fake-broker".into());
            connected.set_protocol_version(21);
            Some(base(Type::CONNECTED, |base| {
                base.connected = MessageField::some(connected)
            }))
        }
        Type::PING => Some(base(Type::PONG, |base| {
            base.pong = MessageField::some(proto::CommandPong::new())
        })),
        Type::PONG => None,
        Type::PARTITIONED_METADATA => {
            // Non-partitioned topics everywhere.
            let request = &command.partitionMetadata;
            let mut response = proto::CommandPartitionedTopicMetadataResponse::new();
            response.set_request_id(request.request_id());
            response.set_partitions(0);
            response.set_response(
                proto::command_partitioned_topic_metadata_response::LookupType::Success,
            );
            Some(base(Type::PARTITIONED_METADATA_RESPONSE, |base| {
                base.partitionMetadataResponse = MessageField::some(response)
            }))
        }
        Type::LOOKUP => {
            let lookup = &command.lookupTopic;
            state.record(BrokerEvent::Lookup {
                topic: lookup.topic().to_string(),
            });
            let mut response = proto::CommandLookupTopicResponse::new();
            response.set_request_id(lookup.request_id());
            response.set_response(proto::command_lookup_topic_response::LookupType::Connect);
            response.set_authoritative(true);
            let advertise = state.advertise.lock().unwrap().clone();
            if advertise.is_empty() {
                response.set_brokerServiceUrl(state.service_url.clone());
            } else {
                response.set_brokerServiceUrl(advertise);
            }
            response.set_proxy_through_service_url(false);
            Some(base(Type::LOOKUP_RESPONSE, |base| {
                base.lookupTopicResponse = MessageField::some(response)
            }))
        }
        Type::SUBSCRIBE => {
            let subscribe = &command.subscribe;
            state.record(BrokerEvent::Subscribe {
                consumer_id: subscribe.consumer_id(),
                topic: subscribe.topic().to_string(),
                subscription: subscribe.subscription().to_string(),
            });
            let mut success = proto::CommandSuccess::new();
            success.set_request_id(subscribe.request_id());
            Some(base(Type::SUCCESS, |base| {
                base.success = MessageField::some(success)
            }))
        }
        Type::ACK => {
            let ack = &command.ack;
            state.record(BrokerEvent::Ack {
                consumer_id: ack.consumer_id(),
                request_id: ack.request_id(),
            });
            // Clients that did not ask for an ack receipt (no request id)
            // get none — matching broker behavior.
            if !ack.has_request_id() {
                return None;
            }
            let mut response = proto::CommandAckResponse::new();
            response.set_consumer_id(ack.consumer_id());
            response.set_request_id(ack.request_id());
            let response = base(Type::ACK_RESPONSE, |base| {
                base.ackResponse = MessageField::some(response)
            });
            if state.hold_acks.load(Ordering::SeqCst) {
                held_acks.push(response);
                None
            } else {
                Some(response)
            }
        }
        Type::PRODUCER => {
            let producer = &command.producer;
            state.record(BrokerEvent::Producer {
                producer_id: producer.producer_id(),
                topic: producer.topic().to_string(),
            });
            let mut success = proto::CommandProducerSuccess::new();
            success.set_request_id(producer.request_id());
            success.set_producer_name(producer.producer_name().to_string());
            success.set_last_sequence_id(-1);
            success.set_producer_ready(true);
            Some(base(Type::PRODUCER_SUCCESS, |base| {
                base.producer_success = MessageField::some(success)
            }))
        }
        Type::SEND => {
            let send = &command.send;
            state.record(BrokerEvent::Send {
                producer_id: send.producer_id(),
                sequence_id: send.sequence_id(),
            });
            let mut receipt = proto::CommandSendReceipt::new();
            receipt.set_producer_id(send.producer_id());
            receipt.set_sequence_id(send.sequence_id());
            receipt.message_id = MessageField::some(message_id(0, send.sequence_id()));
            Some(base(Type::SEND_RECEIPT, |base| {
                base.send_receipt = MessageField::some(receipt)
            }))
        }
        Type::CLOSE_PRODUCER => {
            let mut success = proto::CommandSuccess::new();
            success.set_request_id(command.close_producer.request_id());
            Some(base(Type::SUCCESS, |base| {
                base.success = MessageField::some(success)
            }))
        }
        Type::CLOSE_CONSUMER => {
            let mut success = proto::CommandSuccess::new();
            success.set_request_id(command.close_consumer.request_id());
            Some(base(Type::SUCCESS, |base| {
                base.success = MessageField::some(success)
            }))
        }
        other => {
            log::warn!("fake broker: unhandled {:?}", other);
            None
        }
    }
}

fn base(command_type: Type, fill: impl FnOnce(&mut proto::BaseCommand)) -> MessageCommand {
    let mut base = proto::BaseCommand::new();
    base.set_type(command_type);
    fill(&mut base);
    MessageCommand {
        command: base,
        payload: None,
    }
}

fn message_id(ledger_id: u64, entry_id: u64) -> proto::MessageIdData {
    let mut id = proto::MessageIdData::new();
    id.set_ledgerId(ledger_id);
    id.set_entryId(entry_id);
    id
}

fn message_frame(consumer_id: u64, entry_id: u64, data: &[u8]) -> MessageCommand {
    let mut message = proto::CommandMessage::new();
    message.set_consumer_id(consumer_id);
    message.message_id = MessageField::some(message_id(0, entry_id));

    let mut metadata = proto::MessageMetadata::new();
    metadata.set_producer_name("fake-broker".into());
    metadata.set_sequence_id(entry_id);
    metadata.set_publish_time(0);

    let mut frame = base(Type::MESSAGE, |base| {
        base.message = MessageField::some(message)
    });
    frame.payload = Some(Payload {
        metadata,
        data: bytes::Bytes::copy_from_slice(data),
    });
    frame
}
