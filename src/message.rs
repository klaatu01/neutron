use std::io::Cursor;

use bytes::{Buf, BufMut};
use nom::{
    bytes::streaming::take,
    number::streaming::{be_u16, be_u32},
    IResult,
};
use protobuf::{Message as _, MessageField};

use self::proto::pulsar::{AuthData, BaseCommand, MessageIdData, MessageMetadata};
use crate::{
    codec::Payload,
    resolver_manager::{Resolvable, ResolverKey},
};

pub mod proto {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

    impl Eq for pulsar::MessageIdData {}

    impl std::hash::Hash for pulsar::MessageIdData {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.ledgerId.hash(state);
            self.entryId.hash(state);
            self.partition.hash(state);
            self.batch_index.hash(state);
            self.ack_set.hash(state);
            self.batch_size.hash(state);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub command: BaseCommand,
    pub payload: Option<Payload>,
}

#[derive(Debug, Clone)]
pub enum Outbound {
    Connection(ConnectionOutbound),
    Engine(EngineOutbound),
    Client(ClientOutbound),
}

impl Resolvable for Outbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            Outbound::Engine(command) => command.resolver_id(),
            Outbound::Client(command) => command.resolver_id(),
            Outbound::Connection(command) => command.resolver_id(),
        }
    }
}

impl ToString for Outbound {
    fn to_string(&self) -> String {
        format!(
            "-> {}",
            match self {
                Outbound::Connection(command) => format!("{:?}", command.base_command()),
                Outbound::Engine(command) => format!("{:?}", command.base_command()),
                Outbound::Client(command) => format!("{:?}", command.base_command()),
            }
        )
    }
}

impl Into<Message> for Outbound {
    fn into(self) -> Message {
        match self {
            Outbound::Connection(command) => command.into(),
            Outbound::Engine(command) => command.into(),
            Outbound::Client(command) => command.into(),
        }
    }
}

impl Outbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            Outbound::Connection(command) => command.base_command(),
            Outbound::Engine(command) => command.base_command(),
            Outbound::Client(command) => command.base_command(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Inbound {
    Connection(ConnectionInbound),
    Engine(EngineInbound),
    Client(ClientInbound),
}

impl Resolvable for Inbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            Inbound::Engine(command) => command.resolver_id(),
            Inbound::Client(command) => command.resolver_id(),
            Inbound::Connection(command) => command.resolver_id(),
        }
    }
}

impl ToString for Inbound {
    fn to_string(&self) -> String {
        format!(
            "<- {}",
            match self {
                Inbound::Connection(command) => format!("{:?}", command.base_command()),
                Inbound::Engine(command) => format!("{:?}", command.base_command()),
                Inbound::Client(command) => format!("{:?}", command.base_command()),
            }
        )
    }
}

impl TryFrom<&Message> for Inbound {
    type Error = std::io::Error;
    fn try_from(value: &Message) -> Result<Self, Self::Error> {
        if let Ok(connection) = ConnectionInbound::try_from(value) {
            return Ok(Inbound::Connection(connection));
        } else if let Ok(engine) = EngineInbound::try_from(value) {
            return Ok(Inbound::Engine(engine));
        } else if let Ok(client) = ClientInbound::try_from(value) {
            return Ok(Inbound::Client(client));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Unsupported command",
        ))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ConnectionInbound {
    Ping,
    Pong,
}

impl TryFrom<&Message> for ConnectionInbound {
    type Error = std::io::Error;
    fn try_from(message: &Message) -> Result<ConnectionInbound, Self::Error> {
        match message.command.type_() {
            proto::pulsar::base_command::Type::PING => Ok(ConnectionInbound::Ping),
            proto::pulsar::base_command::Type::PONG => Ok(ConnectionInbound::Pong),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unsupported command",
            )),
        }
    }
}

impl ConnectionInbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            ConnectionInbound::Ping => proto::pulsar::base_command::Type::PING,
            ConnectionInbound::Pong => proto::pulsar::base_command::Type::PONG,
        }
    }
}

impl Resolvable for ConnectionInbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            ConnectionInbound::Pong => Some("PINGPONG".to_string()),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ConnectionOutbound {
    Pong,
    Ping,
}

impl Into<Outbound> for ConnectionOutbound {
    fn into(self) -> Outbound {
        Outbound::Connection(self)
    }
}

impl Into<Message> for ConnectionOutbound {
    fn into(self) -> Message {
        match self {
            ConnectionOutbound::Pong => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::PONG);
                let pong = proto::pulsar::CommandPong::new();
                base.pong = MessageField::some(pong);
                Message {
                    command: base,
                    payload: None,
                }
            }
            ConnectionOutbound::Ping => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::PING);
                let ping = proto::pulsar::CommandPing::new();
                base.ping = MessageField::some(ping);
                Message {
                    command: base,
                    payload: None,
                }
            }
        }
    }
}

impl ConnectionOutbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            ConnectionOutbound::Pong => proto::pulsar::base_command::Type::PONG,
            ConnectionOutbound::Ping => proto::pulsar::base_command::Type::PING,
        }
    }
}

impl Resolvable for ConnectionOutbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            ConnectionOutbound::Ping => Some("PINGPONG".to_string()),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum EngineOutbound {
    Connect { auth_data: Option<Vec<u8>> },
}

impl Into<Message> for EngineOutbound {
    fn into(self) -> Message {
        match self {
            EngineOutbound::Connect { auth_data } => {
                let mut connect = proto::pulsar::CommandConnect::new();
                connect.set_client_version("0.0.1".to_string());
                connect.set_protocol_version(21);

                if let Some(v) = auth_data {
                    connect.set_auth_method(proto::pulsar::AuthMethod::AuthMethodAthens);
                    connect.set_auth_data(v.to_vec())
                }

                let mut base = proto::pulsar::BaseCommand::new();
                base.connect = MessageField::some(connect);
                base.set_type(proto::pulsar::base_command::Type::CONNECT.into());
                Message {
                    command: base,
                    payload: None,
                }
            }
        }
    }
}

impl Into<Outbound> for EngineOutbound {
    fn into(self) -> Outbound {
        Outbound::Engine(self)
    }
}

impl EngineOutbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            EngineOutbound::Connect { .. } => proto::pulsar::base_command::Type::CONNECT,
        }
    }
}

impl Resolvable for EngineOutbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            EngineOutbound::Connect { .. } => Some("CONNECT".to_string()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum EngineInbound {
    SendReceipt {
        producer_id: u64,
        sequence_id: u64,
        message_id: MessageIdData,
    },
    Connected,
    AuthChallenge,
}

impl Resolvable for EngineInbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            EngineInbound::SendReceipt {
                producer_id,
                sequence_id,
                ..
            } => Some(format!("{producer_id}:{sequence_id}")),
            EngineInbound::Connected => Some("CONNECT".to_string()),
            _ => None,
        }
    }
}

impl Into<Inbound> for EngineInbound {
    fn into(self) -> Inbound {
        Inbound::Engine(self)
    }
}

impl EngineInbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            EngineInbound::SendReceipt { .. } => proto::pulsar::base_command::Type::SEND_RECEIPT,
            EngineInbound::Connected => proto::pulsar::base_command::Type::CONNECTED,
            EngineInbound::AuthChallenge => proto::pulsar::base_command::Type::AUTH_CHALLENGE,
        }
    }
}

impl TryFrom<&Message> for EngineInbound {
    type Error = std::io::Error;
    fn try_from(message: &Message) -> Result<EngineInbound, Self::Error> {
        match message.command.type_() {
            proto::pulsar::base_command::Type::CONNECTED => Ok(EngineInbound::Connected),
            proto::pulsar::base_command::Type::AUTH_CHALLENGE => Ok(EngineInbound::AuthChallenge),
            proto::pulsar::base_command::Type::SEND_RECEIPT => {
                let send = &message.command.send;
                let producer_id = send.producer_id();
                let sequence_id = send.sequence_id();
                let message_id = send.message_id.clone().unwrap();
                Ok(EngineInbound::SendReceipt {
                    message_id,
                    producer_id,
                    sequence_id,
                })
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unsupported command",
            )),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ClientInbound {
    Message {
        producer_id: u64,
        sequence_id: u64,
        message_id: MessageIdData,
        payload: Vec<u8>,
    },
    LookupTopic {
        request_id: u64,
        broker_service_url: String,
        broker_service_url_tls: String,
        response: proto::pulsar::command_lookup_topic_response::LookupType,
        authoritative: bool,
    },
    Success {
        request_id: u64,
    },
    AckResponse {
        consumer_id: u64,
        request_id: u64,
    },
}

impl Resolvable for ClientInbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            ClientInbound::Message {
                producer_id,
                sequence_id,
                ..
            } => Some(format!("{producer_id}:{sequence_id}")),
            ClientInbound::LookupTopic { request_id, .. } => Some(format!("LOOKUP:{request_id}")),
            ClientInbound::Success { request_id } => Some(format!("SUCCESS:{request_id}")),
            ClientInbound::AckResponse { request_id, .. } => Some(format!("ACK:{request_id}")),
        }
    }
}

impl TryFrom<&Message> for ClientInbound {
    type Error = std::io::Error;
    fn try_from(message: &Message) -> Result<ClientInbound, Self::Error> {
        match message.command.type_() {
            proto::pulsar::base_command::Type::MESSAGE => {
                let message_id = message.command.message.message_id.clone().unwrap();
                let payload = message.payload.as_ref().unwrap().data.clone();
                let producer_id = message.command.message.consumer_id();
                Ok(ClientInbound::Message {
                    message_id,
                    payload,
                    producer_id,
                    sequence_id: 0,
                })
            }
            proto::pulsar::base_command::Type::LOOKUP_RESPONSE => {
                let lookup_topic = &message.command.lookupTopicResponse;
                let request_id = lookup_topic.request_id();
                let broker_service_url = lookup_topic.brokerServiceUrl().to_string();
                let broker_service_url_tls = lookup_topic.brokerServiceUrlTls().to_string();
                let response = lookup_topic.response();
                let authoritative = lookup_topic.authoritative();
                Ok(ClientInbound::LookupTopic {
                    request_id,
                    broker_service_url,
                    broker_service_url_tls,
                    response,
                    authoritative,
                })
            }
            proto::pulsar::base_command::Type::SUCCESS => {
                let success = &message.command.success;
                let request_id = success.request_id();
                Ok(ClientInbound::Success { request_id })
            }
            proto::pulsar::base_command::Type::ACK_RESPONSE => {
                let ack = &message.command.ackResponse;
                let consumer_id = ack.consumer_id();
                let request_id = ack.request_id();
                if ack.has_error() {
                    println!("Error: {:?}", ack.error());
                }
                Ok(ClientInbound::AckResponse {
                    consumer_id,
                    request_id,
                })
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unsupported command",
            )),
        }
    }
}

impl Into<Inbound> for ClientInbound {
    fn into(self) -> Inbound {
        Inbound::Client(self)
    }
}

impl ClientInbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            ClientInbound::Message { .. } => proto::pulsar::base_command::Type::MESSAGE,
            ClientInbound::LookupTopic { .. } => proto::pulsar::base_command::Type::LOOKUP_RESPONSE,
            ClientInbound::Success { .. } => proto::pulsar::base_command::Type::SUCCESS,
            ClientInbound::AckResponse { .. } => proto::pulsar::base_command::Type::ACK_RESPONSE,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Ack {
    pub consumer_id: u64,
    pub message_id: MessageIdData,
    pub request_id: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ClientOutbound {
    Send {
        producer_id: u64,
        sequence_id: u64,
        message_id: MessageIdData,
        payload: Vec<u8>,
    },
    Ack(Vec<Ack>),
    Ping,
    Pong,
    CloseProducer,
    CloseConsumer,
    AuthChallenge(Vec<u8>),
    LookupTopic {
        topic: String,
        request_id: u64,
    },
    Subscribe {
        topic: String,
        subscription: String,
        consumer_id: u64,
        request_id: u64,
        sub_type: proto::pulsar::command_subscribe::SubType,
    },
    Flow {
        consumer_id: u64,
        message_permits: u32,
    },
}

impl Resolvable for ClientOutbound {
    fn resolver_id(&self) -> Option<ResolverKey> {
        match self {
            ClientOutbound::Send {
                producer_id,
                sequence_id,
                ..
            } => Some(format!("{producer_id}:{sequence_id}")),
            ClientOutbound::LookupTopic { request_id, .. } => Some(format!("LOOKUP:{request_id}")),
            ClientOutbound::Subscribe { request_id, .. } => Some(format!("SUCCESS:{request_id}")),
            ClientOutbound::Ack(acks) => {
                let ack = acks.first().unwrap();
                Some(format!("ACK:{}", ack.request_id))
            }
            _ => None,
        }
    }
}

impl Into<Message> for ClientOutbound {
    fn into(self) -> Message {
        let command = match &self {
            ClientOutbound::Send {
                message_id,
                producer_id,
                sequence_id,
                ..
            } => {
                let mut send = proto::pulsar::CommandSend::new();
                send.set_producer_id(*producer_id);
                send.set_sequence_id(*sequence_id);
                send.set_num_messages(1);
                send.message_id = MessageField::some(message_id.clone());
                let mut base = proto::pulsar::BaseCommand::new();
                base.send = MessageField::some(send);
                base.set_type(proto::pulsar::base_command::Type::SEND.into());
                base
            }
            ClientOutbound::Ack(acks) => {
                let mut ack = proto::pulsar::CommandAck::new();
                ack.set_consumer_id(0);

                if acks.len() > 1 {
                    ack.set_ack_type(proto::pulsar::command_ack::AckType::Cumulative);
                } else {
                    ack.set_ack_type(proto::pulsar::command_ack::AckType::Individual);
                }

                ack.message_id = acks.iter().map(|id| id.message_id.clone()).collect();
                ack.request_id = acks.first().unwrap().request_id.into();

                let mut base = proto::pulsar::BaseCommand::new();
                base.ack = MessageField::some(ack);
                base.set_type(proto::pulsar::base_command::Type::ACK.into());
                base
            }
            ClientOutbound::Ping => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::PING);
                let ping = proto::pulsar::CommandPing::new();
                base.ping = MessageField::some(ping);
                base
            }
            ClientOutbound::Pong => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::PONG);
                let pong = proto::pulsar::CommandPong::new();
                base.pong = MessageField::some(pong);
                base
            }
            ClientOutbound::CloseProducer => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::CLOSE_PRODUCER.into());
                base
            }
            ClientOutbound::CloseConsumer => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::CLOSE_CONSUMER.into());
                base
            }
            ClientOutbound::AuthChallenge(bytes) => {
                let mut auth_challenge = proto::pulsar::CommandAuthChallenge::new();
                let mut auth_data = AuthData::new();
                auth_data.set_auth_data(bytes.to_vec());
                auth_challenge.challenge = MessageField::some(auth_data);
                let mut base = proto::pulsar::BaseCommand::new();
                base.authChallenge = MessageField::some(auth_challenge);
                base.set_type(proto::pulsar::base_command::Type::AUTH_CHALLENGE.into());
                base
            }
            ClientOutbound::LookupTopic { topic, request_id } => {
                let mut lookup_topic = proto::pulsar::CommandLookupTopic::new();
                lookup_topic.set_topic(topic.to_string());
                lookup_topic.set_request_id(*request_id);
                lookup_topic.set_authoritative(false);
                let mut base = proto::pulsar::BaseCommand::new();
                base.lookupTopic = MessageField::some(lookup_topic);
                base.set_type(proto::pulsar::base_command::Type::LOOKUP.into());
                base
            }
            ClientOutbound::Subscribe {
                topic,
                subscription,
                consumer_id,
                request_id,
                sub_type,
            } => {
                let mut subscribe = proto::pulsar::CommandSubscribe::new();
                subscribe.set_topic(topic.to_string());
                subscribe.set_subscription(subscription.to_string());
                subscribe.set_consumer_id(*consumer_id);
                subscribe.set_request_id(*request_id);
                subscribe.set_subType(*sub_type);
                let mut base = proto::pulsar::BaseCommand::new();
                base.subscribe = MessageField::some(subscribe);
                base.set_type(proto::pulsar::base_command::Type::SUBSCRIBE.into());
                base
            }
            ClientOutbound::Flow {
                consumer_id,
                message_permits,
            } => {
                let mut flow = proto::pulsar::CommandFlow::new();
                flow.set_consumer_id(*consumer_id);
                flow.set_messagePermits(*message_permits);
                let mut base = proto::pulsar::BaseCommand::new();
                base.flow = MessageField::some(flow);
                base.set_type(proto::pulsar::base_command::Type::FLOW.into());
                base
            }
        };

        let payload = match self {
            ClientOutbound::Send { payload, .. } => Some(Payload {
                metadata: MessageMetadata::new(),
                data: payload,
            }),
            _ => None,
        };

        Message { command, payload }
    }
}

impl ClientOutbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            ClientOutbound::Send { .. } => proto::pulsar::base_command::Type::SEND,
            ClientOutbound::Ack(_) => proto::pulsar::base_command::Type::ACK,
            ClientOutbound::Ping => proto::pulsar::base_command::Type::PING,
            ClientOutbound::Pong => proto::pulsar::base_command::Type::PONG,
            ClientOutbound::CloseProducer => proto::pulsar::base_command::Type::CLOSE_PRODUCER,
            ClientOutbound::CloseConsumer => proto::pulsar::base_command::Type::CLOSE_CONSUMER,
            ClientOutbound::AuthChallenge(_) => proto::pulsar::base_command::Type::AUTH_CHALLENGE,
            ClientOutbound::LookupTopic { .. } => proto::pulsar::base_command::Type::LOOKUP,
            ClientOutbound::Subscribe { .. } => proto::pulsar::base_command::Type::SUBSCRIBE,
            ClientOutbound::Flow { .. } => proto::pulsar::base_command::Type::FLOW,
        }
    }
}

impl Into<Outbound> for ClientOutbound {
    fn into(self) -> Outbound {
        Outbound::Client(self)
    }
}

#[cfg(test)]
mod test {
    use crate::codec::Codec;
    use crate::message::Message;
    use bytes::BytesMut;
    use protobuf::{Enum, MessageField};
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn parse_payload_command() {
        let input: &[u8] = &[
            0x00, 0x00, 0x00, 0x3D, 0x00, 0x00, 0x00, 0x08, 0x08, 0x06, 0x32, 0x04, 0x08, 0x00,
            0x10, 0x08, 0x0E, 0x01, 0x42, 0x83, 0x54, 0xB5, 0x00, 0x00, 0x00, 0x19, 0x0A, 0x0E,
            0x73, 0x74, 0x61, 0x6E, 0x64, 0x61, 0x6C, 0x6F, 0x6E, 0x65, 0x2D, 0x30, 0x2D, 0x33,
            0x10, 0x08, 0x18, 0xBE, 0xC0, 0xFC, 0x84, 0xD2, 0x2C, 0x68, 0x65, 0x6C, 0x6C, 0x6F,
            0x2D, 0x70, 0x75, 0x6C, 0x73, 0x61, 0x72, 0x2D, 0x38,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();
        {
            let send = message.clone().command.send.unwrap();
            assert_eq!(send.producer_id(), 0);
            assert_eq!(send.sequence_id(), 8);
        }
        {
            let payload = message.payload.as_ref().unwrap();
            assert_eq!(payload.metadata.producer_name(), "standalone-0-3");
            assert_eq!(payload.metadata.sequence_id(), 8);
            assert_eq!(payload.metadata.publish_time(), 1533850624062);
        }
    }

    #[test]
    fn encode_then_decode() {
        let input = Message {
            command: {
                let mut base = super::proto::pulsar::BaseCommand::new();
                base.set_type(super::proto::pulsar::base_command::Type::SEND);
                let mut send = super::proto::pulsar::CommandSend::new();
                send.set_producer_id(0);
                send.set_sequence_id(8);
                send.set_num_messages(1);
                send.message_id = MessageField::some({
                    let mut message_id = super::proto::pulsar::MessageIdData::new();
                    message_id.set_ledgerId(0);
                    message_id.set_entryId(0);
                    message_id.set_partition(0);
                    message_id.set_batch_index(0);
                    message_id.set_batch_size(0);
                    message_id
                });
                base.send = MessageField::some(send);
                base
            },
            payload: Some(super::Payload {
                metadata: {
                    let mut metadata = super::proto::pulsar::MessageMetadata::new();
                    metadata.set_producer_name("standalone-0-3".to_string());
                    metadata.set_sequence_id(8);
                    metadata.set_publish_time(1533850624062);
                    metadata
                },
                data: vec![0, 1, 2, 3, 4, 5, 6, 7],
            }),
        };
        let mut buf = BytesMut::new();
        Codec.encode(input, &mut buf).unwrap();
        let decoded = Codec.decode(&mut buf.into()).unwrap();
    }

    #[test]
    fn base_command_type_parsing() {
        use super::proto::pulsar::base_command::Type;
        let mut successes = 0;
        for i in 0..40 {
            if let Some(type_) = Type::from_i32(i) {
                successes += 1;
                assert_eq!(type_ as i32, i);
            }
        }
        assert_eq!(successes, 38);
    }
}
