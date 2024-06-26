use std::fmt::{Display, Formatter};

use bytes::{BufMut, BytesMut};
use chrono::Utc;
use nom::{
    bytes::streaming::take,
    combinator::{map_res, verify},
    number::streaming::be_u32,
    IResult,
};
use protobuf::{Message as _, MessageField};

use crate::{
    broker_address::BrokerAddress, codec::Payload, command_resolver::ResolverKey, NeutronError,
};

use self::proto::pulsar::{
    AuthData, BaseCommand, MessageIdData, MessageMetadata, SingleMessageMetadata,
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
pub struct MessageCommand {
    pub command: BaseCommand,
    pub payload: Option<Payload>,
}

#[derive(Debug)]
pub(crate) enum Command<Outbound, Inbound>
where
    Outbound: Clone,
    Inbound: Clone,
{
    Request(Outbound),
    RequestResponse(
        Outbound,
        futures::channel::oneshot::Sender<Result<Inbound, NeutronError>>,
    ),
}

impl Command<Outbound, Inbound> {
    pub fn get_outbound(&self) -> Outbound {
        match self {
            Command::Request(outbound) => outbound.clone(),
            Command::RequestResponse(outbound, _) => outbound.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Outbound {
    Pong,
    Ping,
    Connect(Connect),
    Send(Send),
    Ack(Vec<Ack>),
    LookupTopic(LookupTopic),
    Subscribe(Subscribe),
    AuthChallenge(AuthChallenge),
    Flow(Flow),
    Producer(Producer),
}

impl Display for Outbound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Outbound::Ping => write!(f, "Ping"),
            Outbound::Pong => write!(f, "Pong"),
            Outbound::Connect(_) => write!(f, "Connect"),
            Outbound::Send(_) => write!(f, "Send"),
            Outbound::Ack(_) => write!(f, "Ack"),
            Outbound::LookupTopic(_) => write!(f, "LookupTopic"),
            Outbound::Subscribe(_) => write!(f, "Subscribe"),
            Outbound::AuthChallenge(_) => write!(f, "AuthChallenge"),
            Outbound::Flow(_) => write!(f, "Flow"),
            Outbound::Producer(_) => write!(f, "Producer"),
        }
    }
}

impl ResolverKey for Outbound {
    fn try_key(&self) -> Option<String> {
        match self {
            Outbound::Connect(_) => Some("CONNECT".to_string()),
            Outbound::Send(send) => Some(format!("SEND:{}", send.sequence_id())),
            Outbound::Ack(_) => Some("ACK".to_string()),
            Outbound::LookupTopic(_) => Some("LOOKUP".to_string()),
            Outbound::Subscribe(_) => Some("SUBSCRIBE".to_string()),
            Outbound::AuthChallenge(_) => Some("AUTH_CHALLENGE".to_string()),
            Outbound::Flow(_) => Some("FLOW".to_string()),
            Outbound::Producer(_) => Some("PRODUCER".to_string()),
            _ => None,
        }
    }
}

impl From<Outbound> for MessageCommand {
    fn from(val: Outbound) -> Self {
        match val {
            Outbound::Ping => Ping.into(),
            Outbound::Pong => Pong.into(),
            Outbound::Connect(connect) => connect.into(),
            Outbound::Send(send) => send.into(),
            Outbound::Ack(ack) => ack.into(),
            Outbound::LookupTopic(lookup_topic) => lookup_topic.into(),
            Outbound::Subscribe(subscribe) => subscribe.into(),
            Outbound::AuthChallenge(auth_challenge) => auth_challenge.into(),
            Outbound::Flow(flow) => flow.into(),
            Outbound::Producer(producer) => producer.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Inbound {
    Ping,
    Pong,
    Connected(Connected),
    SendReceipt(SendReceipt),
    AckReciept(AckReciept),
    Message(Message),
    LookupTopicResponse(LookupTopicResponse),
    AuthChallengeRequest(AuthChallengeRequest),
    Success(Success),
    ProducerSuccess(ProducerSuccess),
}

impl Display for Inbound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Inbound::Ping => write!(f, "Ping"),
            Inbound::Pong => write!(f, "Pong"),
            Inbound::Connected(_) => write!(f, "Connected"),
            Inbound::SendReceipt(_) => write!(f, "SendReceipt"),
            Inbound::AckReciept(_) => write!(f, "AckReciept"),
            Inbound::Message(_) => write!(f, "Message"),
            Inbound::LookupTopicResponse(_) => write!(f, "LookupTopicResponse"),
            Inbound::AuthChallengeRequest(_) => write!(f, "AuthChallengeRequest"),
            Inbound::Success(_) => write!(f, "Success"),
            Inbound::ProducerSuccess(_) => write!(f, "ProducerSuccess"),
        }
    }
}

impl Inbound {
    pub fn try_consumer_or_producer_id(&self) -> Option<u64> {
        match self {
            Inbound::SendReceipt(receipt) => Some(receipt.producer_id),
            Inbound::AckReciept(receipt) => Some(receipt.consumer_id),
            Inbound::Message(message) => Some(message.consumer_id()),
            _ => None,
        }
    }
}

impl ResolverKey for Inbound {
    fn try_key(&self) -> Option<String> {
        match self {
            Inbound::Connected(_) => Some("CONNECT".to_string()),
            Inbound::SendReceipt(SendReceipt { sequence_id, .. }) => {
                Some(format!("SEND:{}", sequence_id))
            }
            Inbound::AckReciept(_) => Some("ACK".to_string()),
            Inbound::Message(_) => Some("MESSAGE".to_string()),
            Inbound::LookupTopicResponse(_) => Some("LOOKUP".to_string()),
            Inbound::AuthChallengeRequest(_) => Some("AUTH_CHALLENGE".to_string()),
            Inbound::Success(_) => Some("SUBSCRIBE".to_string()),
            Inbound::ProducerSuccess(_) => Some("PRODUCER".to_string()),
            _ => None,
        }
    }
}

impl TryFrom<MessageCommand> for Inbound {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::PING => Ok(Inbound::Ping),
            proto::pulsar::base_command::Type::PONG => Ok(Inbound::Pong),
            proto::pulsar::base_command::Type::CONNECTED => {
                Connected::try_from(value).map(Inbound::Connected)
            }
            proto::pulsar::base_command::Type::SEND_RECEIPT => {
                SendReceipt::try_from(value).map(Inbound::SendReceipt)
            }
            proto::pulsar::base_command::Type::ACK_RESPONSE => {
                AckReciept::try_from(value).map(Inbound::AckReciept)
            }
            proto::pulsar::base_command::Type::MESSAGE => {
                Message::try_from(value).map(Inbound::Message)
            }
            proto::pulsar::base_command::Type::LOOKUP_RESPONSE => {
                LookupTopicResponse::try_from(value).map(Inbound::LookupTopicResponse)
            }
            proto::pulsar::base_command::Type::AUTH_CHALLENGE => {
                AuthChallengeRequest::try_from(value).map(Inbound::AuthChallengeRequest)
            }
            proto::pulsar::base_command::Type::SUCCESS => {
                Success::try_from(value).map(Inbound::Success)
            }
            proto::pulsar::base_command::Type::PRODUCER_SUCCESS => {
                ProducerSuccess::try_from(value).map(Inbound::ProducerSuccess)
            }
            proto::pulsar::base_command::Type::ERROR => {
                log::error!("Error: {:?}", value.command.error.message());
                Err(NeutronError::UnsupportedCommand)
            }
            _ => Err(NeutronError::UnsupportedCommand),
        }
    }
}

// Ping

#[derive(Debug, Clone)]
struct Ping;

impl From<Ping> for Outbound {
    fn from(_val: Ping) -> Self {
        Outbound::Ping
    }
}

impl TryFrom<Inbound> for Ping {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::Ping => Ok(Ping),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl From<Ping> for MessageCommand {
    fn from(_val: Ping) -> Self {
        let mut command = proto::pulsar::BaseCommand::new();
        command.set_type(proto::pulsar::base_command::Type::PING);
        let ping = proto::pulsar::CommandPing::new();
        command.ping = MessageField::some(ping);
        MessageCommand {
            command,
            payload: None,
        }
    }
}

impl TryFrom<MessageCommand> for Ping {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::PING => Ok(Ping),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// Pong

#[derive(Debug, Clone)]
struct Pong;

impl From<Pong> for Outbound {
    fn from(_val: Pong) -> Self {
        Outbound::Pong
    }
}

impl TryFrom<Inbound> for Pong {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::Pong => Ok(Pong),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl From<Pong> for MessageCommand {
    fn from(_val: Pong) -> Self {
        let mut command = proto::pulsar::BaseCommand::new();
        command.set_type(proto::pulsar::base_command::Type::PONG);
        let pong = proto::pulsar::CommandPong::new();
        command.pong = MessageField::some(pong);
        MessageCommand {
            command,
            payload: None,
        }
    }
}

impl TryFrom<MessageCommand> for Pong {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::PONG => Ok(Pong),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// Connect

#[derive(Debug, Clone)]
pub struct Connect {
    pub auth_data: Option<Vec<u8>>,
    pub auth_method_name: Option<String>,
    pub broker_address: Option<BrokerAddress>,
}

impl From<Connect> for Outbound {
    fn from(val: Connect) -> Self {
        Outbound::Connect(val)
    }
}

impl From<Connect> for MessageCommand {
    fn from(val: Connect) -> Self {
        let mut connect = proto::pulsar::CommandConnect::new();
        connect.set_client_version("2.0.0-incubating".to_string());
        connect.set_protocol_version(21);

        if let Some(v) = val.auth_data {
            connect.set_auth_method_name(val.auth_method_name.unwrap_or("none".to_string()));
            connect.set_auth_data(v.to_vec())
        }

        if let Some(broker_address) = val.broker_address {
            if let Some(proxy) = broker_address.get_proxy() {
                connect.set_proxy_to_broker_url(proxy.to_string());
            }
        }

        let mut base = proto::pulsar::BaseCommand::new();
        base.connect = MessageField::some(connect);
        base.set_type(proto::pulsar::base_command::Type::CONNECT);
        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// Connected

#[derive(Debug, Clone)]
pub struct Connected;

impl TryFrom<Inbound> for Connected {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::Connected(connected) => Ok(connected),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for Connected {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::CONNECTED => Ok(Connected),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// Send
#[derive(Debug, Clone)]
pub enum Send {
    Single {
        producer_name: String,
        producer_id: u64,
        sequence_id: u64,
        payload: Vec<u8>,
    },
    Batch {
        producer_name: String,
        producer_id: u64,
        sequence_id: u64,
        payloads: Vec<Vec<u8>>,
    },
}

#[derive(Debug, Clone)]
pub struct BatchedPayload {
    pub metadata: SingleMessageMetadata,
    pub payload: Vec<u8>,
}

impl Send {
    pub fn producer_name(&self) -> &str {
        match self {
            Send::Single { producer_name, .. } => producer_name,
            Send::Batch { producer_name, .. } => producer_name,
        }
    }

    pub fn producer_id(&self) -> u64 {
        match self {
            Send::Single { producer_id, .. } => *producer_id,
            Send::Batch { producer_id, .. } => *producer_id,
        }
    }

    pub fn sequence_id(&self) -> u64 {
        match self {
            Send::Single { sequence_id, .. } => *sequence_id,
            Send::Batch { sequence_id, .. } => *sequence_id,
        }
    }

    pub fn number_of_messages(&self) -> u32 {
        match self {
            Send::Single { .. } => 1,
            Send::Batch { payloads, .. } => payloads.len() as u32,
        }
    }

    pub fn payload(self) -> Vec<u8> {
        match self {
            Send::Single { payload, .. } => payload.clone(),
            Send::Batch { payloads, .. } => {
                let mut buffer = BytesMut::new();

                for payload in payloads {
                    let mut metadata = SingleMessageMetadata::new();
                    metadata.set_payload_size(payload.len() as i32);
                    let metadata_size = metadata.compute_size() as u32;
                    let metadata_bytes = metadata.write_to_bytes().unwrap();
                    buffer.put_u32(metadata_size);
                    buffer.put_slice(&metadata_bytes);
                    buffer.put_slice(&payload);
                }

                buffer.to_vec()
            }
        }
    }
}

impl From<Send> for Outbound {
    fn from(val: Send) -> Self {
        Outbound::Send(val)
    }
}

impl From<Send> for MessageCommand {
    fn from(val: Send) -> Self {
        let command = {
            let mut send = proto::pulsar::CommandSend::new();
            send.set_producer_id(val.producer_id());
            send.set_sequence_id(val.sequence_id());
            send.set_num_messages(val.number_of_messages() as i32);
            let mut base = proto::pulsar::BaseCommand::new();
            base.send = MessageField::some(send);
            base.set_type(proto::pulsar::base_command::Type::SEND);
            base
        };

        let metadata = {
            let now_as_millis = Utc::now().timestamp_millis() as u64;
            let mut metadata = MessageMetadata::new();
            metadata.set_producer_name(val.producer_name().to_string());
            metadata.set_sequence_id(val.sequence_id());
            metadata.set_publish_time(now_as_millis);
            metadata.set_event_time(now_as_millis);
            metadata.set_num_messages_in_batch(val.number_of_messages() as i32);
            metadata
        };

        let payload = Payload {
            metadata,
            data: val.payload(),
        };

        MessageCommand {
            command,
            payload: Some(payload),
        }
    }
}

// SendReceipt

#[derive(Debug, Clone)]
pub struct SendReceipt {
    pub producer_id: u64,
    pub sequence_id: u64,
    pub message_id: MessageIdData,
}

impl TryFrom<Inbound> for SendReceipt {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::SendReceipt(receipt) => Ok(receipt),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for SendReceipt {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::SEND_RECEIPT => {
                let send = value.command.send_receipt.unwrap();
                Ok(SendReceipt {
                    producer_id: send.producer_id(),
                    sequence_id: send.sequence_id(),
                    message_id: send.message_id.unwrap(),
                })
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// Ack

#[derive(Debug, Clone)]
pub struct Ack {
    pub consumer_id: u64,
    pub message_id: MessageIdData,
    pub request_id: u64,
}

impl From<Ack> for Outbound {
    fn from(val: Ack) -> Self {
        Outbound::Ack(vec![val])
    }
}

impl From<Vec<Ack>> for Outbound {
    fn from(val: Vec<Ack>) -> Self {
        Outbound::Ack(val)
    }
}

impl From<Vec<Ack>> for MessageCommand {
    fn from(val: Vec<Ack>) -> Self {
        let mut command = proto::pulsar::CommandAck::new();
        let first = val.first().unwrap();
        command.set_consumer_id(first.consumer_id);

        if val.len() > 1 {
            command.set_ack_type(proto::pulsar::command_ack::AckType::Cumulative);
        } else {
            command.set_ack_type(proto::pulsar::command_ack::AckType::Individual);
        }

        command.message_id = val.iter().map(|id| id.message_id.clone()).collect();
        command.request_id = val.first().unwrap().request_id.into();

        let mut base = proto::pulsar::BaseCommand::new();
        base.ack = MessageField::some(command);
        base.set_type(proto::pulsar::base_command::Type::ACK);

        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// AckReciept

#[derive(Debug, Clone)]
pub struct AckReciept {
    pub consumer_id: u64,
    pub request_id: u64,
}

impl TryFrom<Inbound> for AckReciept {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::AckReciept(receipt) => Ok(receipt),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for AckReciept {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::ACK_RESPONSE => {
                let ack = value.command.ackResponse.unwrap();
                Ok(AckReciept {
                    consumer_id: ack.consumer_id(),
                    request_id: ack.request_id(),
                })
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SingleMessage {
    pub consumer_id: u64,
    pub message_id: MessageIdData,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BatchMessage {
    pub consumer_id: u64,
    pub message_id: MessageIdData,
    pub payloads: Vec<BatchedPayload>,
}
// Message
#[derive(Debug, Clone)]
pub enum Message {
    Single(SingleMessage),
    Batch(BatchMessage),
}

impl Message {
    pub fn consumer_id(&self) -> u64 {
        match self {
            Message::Single(msg) => msg.consumer_id,
            Message::Batch(msg) => msg.consumer_id,
        }
    }
}

impl TryFrom<Inbound> for Message {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::Message(message) => Ok(message),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn batched_payload(i: &[u8]) -> IResult<&[u8], BatchedPayload> {
    let (i, metadata_size) = be_u32(i)?;
    let (i, metadata) = verify(
        map_res(take(metadata_size), SingleMessageMetadata::parse_from_bytes),
        // payload_size is defined as i32 in protobuf
        |metadata| metadata.payload_size() >= 0,
    )(i)?;

    let (i, payload) = take(metadata.payload_size() as u32)(i)?;

    Ok((
        i,
        BatchedPayload {
            metadata,
            payload: payload.to_vec(),
        },
    ))
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
pub(crate) fn parse_batched_message(
    count: u32,
    payload: &[u8],
) -> Result<Vec<BatchedPayload>, NeutronError> {
    let (_, result) = nom::multi::count(batched_payload, count as usize)(payload)
        .map_err(|_| NeutronError::DecodeFailed)?;
    Ok(result)
}

impl TryFrom<MessageCommand> for Message {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::MESSAGE => {
                let message_id = value.command.message.message_id.clone().unwrap();
                let payload = value.payload.as_ref().unwrap();
                let metadata = payload.metadata.clone();
                let payload = payload.data.clone();
                let consumer_id = value.command.message.consumer_id();
                if metadata.num_messages_in_batch() > 1 {
                    Ok(Message::Batch(BatchMessage {
                        consumer_id,
                        message_id,
                        payloads: parse_batched_message(
                            metadata.num_messages_in_batch() as u32,
                            &payload,
                        )?,
                    }))
                } else {
                    Ok(Message::Single(SingleMessage {
                        consumer_id,
                        message_id,
                        payload,
                    }))
                }
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// LookupTopic

#[derive(Debug, Clone)]
pub struct LookupTopic {
    pub request_id: u64,
    pub topic: String,
}

impl From<LookupTopic> for Outbound {
    fn from(val: LookupTopic) -> Self {
        Outbound::LookupTopic(val)
    }
}

impl From<LookupTopic> for MessageCommand {
    fn from(val: LookupTopic) -> Self {
        let mut command = proto::pulsar::CommandLookupTopic::new();
        command.set_topic(val.topic);
        command.set_request_id(val.request_id);

        let mut base = proto::pulsar::BaseCommand::new();
        base.lookupTopic = MessageField::some(command);
        base.set_type(proto::pulsar::base_command::Type::LOOKUP);

        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// LookupTopicResponse

#[derive(Debug, Clone)]
pub enum LookupResponseType {
    Connect,
    Redirect,
    Failed,
}

#[derive(Debug, Clone)]
pub struct LookupTopicResponse {
    pub request_id: u64,
    pub broker_service_url: String,
    pub broker_service_url_tls: String,
    pub response_type: LookupResponseType,
    pub authoritative: bool,
    pub proxy: bool,
}

impl LookupTopicResponse {
    pub fn get_broker_service_url(&self) -> String {
        if !self.broker_service_url.is_empty() {
            self.broker_service_url.clone()
        } else {
            self.broker_service_url_tls.clone()
        }
    }
}

impl TryFrom<Inbound> for LookupTopicResponse {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::LookupTopicResponse(response) => Ok(response),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for LookupTopicResponse {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::LOOKUP_RESPONSE => {
                let response = value.command.lookupTopicResponse.unwrap();
                let response_type = match response.response() {
                    proto::pulsar::command_lookup_topic_response::LookupType::Connect => {
                        LookupResponseType::Connect
                    }
                    proto::pulsar::command_lookup_topic_response::LookupType::Redirect => {
                        LookupResponseType::Redirect
                    }
                    proto::pulsar::command_lookup_topic_response::LookupType::Failed => {
                        LookupResponseType::Failed
                    }
                };

                Ok(LookupTopicResponse {
                    request_id: response.request_id(),
                    broker_service_url: response.brokerServiceUrl().to_string(),
                    broker_service_url_tls: response.brokerServiceUrlTls().to_string(),
                    response_type,
                    authoritative: response.authoritative(),
                    proxy: response.proxy_through_service_url(),
                })
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// AuthChallenge

#[derive(Debug, Clone)]
pub struct AuthChallenge {
    pub auth_method_name: String,
    pub auth_data: Vec<u8>,
}

impl From<AuthChallenge> for Outbound {
    fn from(val: AuthChallenge) -> Self {
        Outbound::AuthChallenge(val)
    }
}

impl From<AuthChallenge> for MessageCommand {
    fn from(val: AuthChallenge) -> Self {
        let auth_data = AuthData {
            auth_data: Some(val.auth_data),
            auth_method_name: Some(val.auth_method_name),
            ..Default::default()
        };

        let mut command = proto::pulsar::CommandAuthResponse::new();
        command.response = MessageField::some(auth_data);

        let mut base = proto::pulsar::BaseCommand::new();
        base.authResponse = MessageField::some(command);
        base.set_type(proto::pulsar::base_command::Type::AUTH_CHALLENGE);

        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// AuthChallengeRequest

#[derive(Debug, Clone)]
pub struct AuthChallengeRequest;

impl TryFrom<Inbound> for AuthChallengeRequest {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::AuthChallengeRequest(auth_challenge_request) => Ok(auth_challenge_request),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for AuthChallengeRequest {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::AUTH_CHALLENGE => Ok(AuthChallengeRequest),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// Flow

#[derive(Debug, Clone)]
pub struct Flow {
    pub consumer_id: u64,
    pub message_permits: u32,
}

impl From<Flow> for Outbound {
    fn from(val: Flow) -> Self {
        Outbound::Flow(val)
    }
}

impl From<Flow> for MessageCommand {
    fn from(val: Flow) -> Self {
        let mut command = proto::pulsar::CommandFlow::new();
        command.set_consumer_id(val.consumer_id);
        command.set_messagePermits(val.message_permits);

        let mut base = proto::pulsar::BaseCommand::new();
        base.flow = MessageField::some(command);
        base.set_type(proto::pulsar::base_command::Type::FLOW);

        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// Subscribe

#[derive(Debug, Clone)]
pub enum SubType {
    Exclusive,
    Shared,
    Failover,
    KeyShared,
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    pub topic: String,
    pub subscription: String,
    pub consumer_id: u64,
    pub request_id: u64,
    pub sub_type: SubType,
}

impl From<Subscribe> for Outbound {
    fn from(val: Subscribe) -> Self {
        Outbound::Subscribe(val)
    }
}

impl From<Subscribe> for MessageCommand {
    fn from(val: Subscribe) -> Self {
        let mut command = proto::pulsar::CommandSubscribe::new();
        command.set_topic(val.topic);
        command.set_subscription(val.subscription);
        command.set_subType(match val.sub_type {
            SubType::Exclusive => proto::pulsar::command_subscribe::SubType::Exclusive,
            SubType::Shared => proto::pulsar::command_subscribe::SubType::Shared,
            SubType::Failover => proto::pulsar::command_subscribe::SubType::Failover,
            SubType::KeyShared => proto::pulsar::command_subscribe::SubType::Key_Shared,
        });
        command.set_consumer_id(val.consumer_id);
        command.set_request_id(val.request_id);

        let mut base = proto::pulsar::BaseCommand::new();
        base.subscribe = MessageField::some(command);
        base.set_type(proto::pulsar::base_command::Type::SUBSCRIBE);

        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// Success

#[derive(Debug, Clone)]
pub struct Success {
    pub request_id: u64,
}

impl TryFrom<Inbound> for Success {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::Success(success) => Ok(success),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for Success {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::SUCCESS => {
                let success = value.command.success.unwrap();
                Ok(Success {
                    request_id: success.request_id(),
                })
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

// Producer Registration
#[derive(Debug, Clone)]
pub struct Producer {
    pub(crate) topic: String,
    pub(crate) producer_id: u64,
    pub(crate) request_id: u64,
    pub(crate) producer_name: Option<String>,
}

impl From<Producer> for Outbound {
    fn from(val: Producer) -> Self {
        Outbound::Producer(val)
    }
}

impl From<Producer> for MessageCommand {
    fn from(val: Producer) -> Self {
        let mut command = proto::pulsar::CommandProducer::new();
        command.set_producer_id(val.producer_id);
        command.set_request_id(val.request_id);
        command.set_topic(val.topic);
        if let Some(name) = val.producer_name {
            command.set_producer_name(name);
        }

        let mut base = proto::pulsar::BaseCommand::new();
        base.producer = MessageField::some(command);
        base.set_type(proto::pulsar::base_command::Type::PRODUCER);

        MessageCommand {
            command: base,
            payload: None,
        }
    }
}

// Producer Success
#[derive(Debug, Clone)]
pub struct ProducerSuccess {
    pub request_id: u64,
    pub producer_name: String,
}

impl TryFrom<Inbound> for ProducerSuccess {
    type Error = NeutronError;

    fn try_from(value: Inbound) -> Result<Self, Self::Error> {
        match value {
            Inbound::ProducerSuccess(success) => Ok(success),
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

impl TryFrom<MessageCommand> for ProducerSuccess {
    type Error = NeutronError;

    fn try_from(value: MessageCommand) -> Result<Self, Self::Error> {
        match value.command.type_() {
            proto::pulsar::base_command::Type::PRODUCER_SUCCESS => {
                let success = value.command.producer_success.unwrap();
                Ok(ProducerSuccess {
                    request_id: success.request_id(),
                    producer_name: success.producer_name().to_string(),
                })
            }
            _ => Err(NeutronError::Unresolvable),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::codec::Codec;
    use crate::message::MessageCommand;
    use bytes::BytesMut;
    use protobuf::{Enum, MessageField};
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn parse_payload_command_1() {
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
    fn parse_payload_command_2() {
        let input: &[u8] = &[
            0, 0, 0, 61, 0, 0, 0, 10, 8, 6, 50, 6, 8, 0, 16, 0, 24, 1, 14, 1, 193, 160, 85, 188, 0,
            0, 0, 31, 10, 13, 116, 101, 115, 116, 45, 112, 114, 111, 100, 117, 99, 101, 114, 16, 0,
            24, 233, 233, 167, 204, 215, 49, 96, 233, 233, 167, 204, 215, 49, 100, 97, 116, 97, 45,
            48,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();
        {
            let send = message.clone().command.send.unwrap();
            assert_eq!(send.producer_id(), 0);
            assert_eq!(send.sequence_id(), 0);
        }
        {
            let payload = message.payload.as_ref().unwrap();
            assert_eq!(payload.metadata.producer_name(), "test-producer");
            assert_eq!(payload.metadata.sequence_id(), 0);
            assert_eq!(payload.metadata.publish_time(), 1707141100777);
        }
    }

    #[test]
    fn encode_then_decode() {
        let input = MessageCommand {
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
        let _decoded = Codec.decode(&mut buf).unwrap();
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
