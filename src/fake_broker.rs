//! A minimal in-process Pulsar broker for integration tests: real TCP,
//! real frames through the production `Codec`, scripted just enough to
//! exercise the client's full path (handshake, lookup, subscribe, flow,
//! publish, ack) and its failure modes.

use std::sync::atomic::{AtomicBool, Ordering};
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
pub(crate) enum BrokerEvent {
    Connect,
    Lookup { topic: String },
    Subscribe { consumer_id: u64, topic: String, subscription: String },
    Flow { consumer_id: u64, permits: u32 },
    Ack { consumer_id: u64, request_id: u64 },
    Producer { producer_id: u64, topic: String },
    Send { producer_id: u64, sequence_id: u64 },
}

enum PushCmd {
    Frame(Box<MessageCommand>),
    /// Respond to every held ack, most recent first.
    ReleaseHeldAcksReversed,
    Shutdown,
}

struct BrokerState {
    events: mpsc::UnboundedSender<BrokerEvent>,
    /// When set, ACKs are held unanswered until released.
    hold_acks: AtomicBool,
    /// brokerServiceUrl to advertise in lookup responses; empty means
    /// "stay where you are".
    advertise: Mutex<String>,
    pushers: Mutex<Vec<mpsc::UnboundedSender<PushCmd>>>,
}

pub(crate) struct FakeBroker {
    pub(crate) port: u16,
    state: Arc<BrokerState>,
    events: tokio::sync::Mutex<mpsc::UnboundedReceiver<BrokerEvent>>,
}

impl FakeBroker {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let state = Arc::new(BrokerState {
            events: events_tx,
            hold_acks: AtomicBool::new(false),
            advertise: Mutex::new(String::new()),
            pushers: Mutex::new(Vec::new()),
        });

        tokio::spawn({
            let state = state.clone();
            async move {
                while let Ok((stream, _)) = listener.accept().await {
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

    pub(crate) fn service_url(&self) -> String {
        format!("pulsar://127.0.0.1:{}", self.port)
    }

    pub(crate) fn config(&self) -> crate::PulsarConfig {
        crate::PulsarConfig {
            endpoint_url: "pulsar://127.0.0.1".to_string(),
            endpoint_port: self.port,
        }
    }

    /// Advertise another broker's URL in lookup responses, so clients
    /// move there.
    pub(crate) fn advertise(&self, url: &str) {
        *self.state.advertise.lock().unwrap() = url.to_string();
    }

    pub(crate) fn hold_acks(&self) {
        self.state.hold_acks.store(true, Ordering::SeqCst);
    }

    /// Answer every held ack, most recent first.
    pub(crate) fn release_held_acks_reversed(&self) {
        for pusher in self.state.pushers.lock().unwrap().iter() {
            let _ = pusher.send(PushCmd::ReleaseHeldAcksReversed);
        }
    }

    /// Push a single message to a consumer over every live connection.
    pub(crate) fn push_message(&self, consumer_id: u64, entry_id: u64, data: &[u8]) {
        let frame = message_frame(consumer_id, entry_id, data);
        for pusher in self.state.pushers.lock().unwrap().iter() {
            let _ = pusher.send(PushCmd::Frame(Box::new(frame.clone())));
        }
    }

    /// Drop every live connection at the socket, as a crashing broker
    /// would.
    pub(crate) fn kill_connections(&self) {
        for pusher in self.state.pushers.lock().unwrap().drain(..) {
            let _ = pusher.send(PushCmd::Shutdown);
        }
    }

    /// The next recorded protocol event.
    pub(crate) async fn next_event(&self) -> BrokerEvent {
        self.events.lock().await.recv().await.expect("broker event stream ended")
    }

    /// Wait until `want` shows up, skipping unrelated events.
    pub(crate) async fn expect_event(&self, want: BrokerEvent) {
        loop {
            let got = self.next_event().await;
            if got == want {
                return;
            }
        }
    }

    /// Wait for the next event matching `pred` and return it.
    pub(crate) async fn wait_for(&self, pred: impl Fn(&BrokerEvent) -> bool) -> BrokerEvent {
        loop {
            let got = self.next_event().await;
            if pred(&got) {
                return got;
            }
        }
    }
}

async fn serve_connection(stream: TcpStream, state: Arc<BrokerState>) {
    let mut framed = Framed::new(stream, Codec);
    let (push_tx, mut push_rx) = mpsc::unbounded_channel();
    state.pushers.lock().unwrap().push(push_tx);
    let mut held_acks: Vec<MessageCommand> = Vec::new();

    loop {
        tokio::select! {
            Some(cmd) = push_rx.recv() => match cmd {
                PushCmd::Frame(frame) => {
                    if framed.send(*frame).await.is_err() {
                        break;
                    }
                }
                PushCmd::ReleaseHeldAcksReversed => {
                    for response in held_acks.drain(..).rev().collect::<Vec<_>>() {
                        if framed.send(response).await.is_err() {
                            return;
                        }
                    }
                }
                PushCmd::Shutdown => break,
            },
            frame = framed.next() => {
                let Some(Ok(frame)) = frame else { break };
                let Some(response) = respond(frame, &state, &mut held_acks) else { continue };
                if framed.send(response).await.is_err() {
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
            let _ = state.events.send(BrokerEvent::Connect);
            let mut connected = proto::CommandConnected::new();
            connected.set_server_version("fake-broker".into());
            connected.set_protocol_version(21);
            Some(base(Type::CONNECTED, |base| base.connected = MessageField::some(connected)))
        }
        Type::PING => Some(base(Type::PONG, |base| {
            base.pong = MessageField::some(proto::CommandPong::new())
        })),
        Type::PONG => None,
        Type::LOOKUP => {
            let lookup = &command.lookupTopic;
            let _ = state.events.send(BrokerEvent::Lookup {
                topic: lookup.topic().to_string(),
            });
            let mut response = proto::CommandLookupTopicResponse::new();
            response.set_request_id(lookup.request_id());
            response.set_response(proto::command_lookup_topic_response::LookupType::Connect);
            let advertise = state.advertise.lock().unwrap().clone();
            if !advertise.is_empty() {
                response.set_brokerServiceUrl(advertise);
            }
            response.set_proxy_through_service_url(false);
            Some(base(Type::LOOKUP_RESPONSE, |base| {
                base.lookupTopicResponse = MessageField::some(response)
            }))
        }
        Type::SUBSCRIBE => {
            let subscribe = &command.subscribe;
            let _ = state.events.send(BrokerEvent::Subscribe {
                consumer_id: subscribe.consumer_id(),
                topic: subscribe.topic().to_string(),
                subscription: subscribe.subscription().to_string(),
            });
            let mut success = proto::CommandSuccess::new();
            success.set_request_id(subscribe.request_id());
            Some(base(Type::SUCCESS, |base| base.success = MessageField::some(success)))
        }
        Type::FLOW => {
            let flow = &command.flow;
            let _ = state.events.send(BrokerEvent::Flow {
                consumer_id: flow.consumer_id(),
                permits: flow.messagePermits(),
            });
            None
        }
        Type::ACK => {
            let ack = &command.ack;
            let _ = state.events.send(BrokerEvent::Ack {
                consumer_id: ack.consumer_id(),
                request_id: ack.request_id(),
            });
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
            let _ = state.events.send(BrokerEvent::Producer {
                producer_id: producer.producer_id(),
                topic: producer.topic().to_string(),
            });
            let mut success = proto::CommandProducerSuccess::new();
            success.set_request_id(producer.request_id());
            success.set_producer_name(producer.producer_name().to_string());
            Some(base(Type::PRODUCER_SUCCESS, |base| {
                base.producer_success = MessageField::some(success)
            }))
        }
        Type::SEND => {
            let send = &command.send;
            let _ = state.events.send(BrokerEvent::Send {
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
        other => {
            log::warn!("fake broker: unhandled {:?}", other);
            None
        }
    }
}

fn base(
    command_type: Type,
    fill: impl FnOnce(&mut proto::BaseCommand),
) -> MessageCommand {
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

    let mut frame = base(Type::MESSAGE, |base| base.message = MessageField::some(message));
    frame.payload = Some(Payload {
        metadata,
        data: bytes::Bytes::copy_from_slice(data),
    });
    frame
}
