use bytes::BufMut;
use nom::{
    bytes::streaming::take,
    number::streaming::{be_u16, be_u32},
    IResult,
};
use protobuf::{Message as _, MessageField};

use self::proto::pulsar::{AuthData, BaseCommand, MessageIdData, MessageMetadata};

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

pub enum Outbound {
    Connection(ConnectionOutbound),
    Engine(EngineOutbound),
    Client(ClientOutbound),
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

pub enum Inbound {
    Connection(ConnectionInbound),
    Engine(EngineInbound),
    Client(ClientInbound),
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
}

impl TryFrom<&Message> for ConnectionInbound {
    type Error = std::io::Error;
    fn try_from(message: &Message) -> Result<ConnectionInbound, Self::Error> {
        match message.command.type_() {
            proto::pulsar::base_command::Type::PING => Ok(ConnectionInbound::Ping),
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
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ConnectionOutbound {
    Pong,
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
        }
    }
}

impl ConnectionOutbound {
    pub fn base_command(&self) -> proto::pulsar::base_command::Type {
        match self {
            ConnectionOutbound::Pong => proto::pulsar::base_command::Type::PONG,
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

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum EngineInbound {
    SendReceipt { message_id: MessageIdData },
    Connected,
    AuthChallenge,
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
                let send = message.command.send.clone().unwrap();
                let message_id = send.message_id.unwrap();
                Ok(EngineInbound::SendReceipt { message_id })
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
        message_id: MessageIdData,
        payload: Vec<u8>,
    },
}

impl TryFrom<&Message> for ClientInbound {
    type Error = std::io::Error;
    fn try_from(message: &Message) -> Result<ClientInbound, Self::Error> {
        match message.command.type_() {
            proto::pulsar::base_command::Type::MESSAGE => {
                let message_id = message.command.message.message_id.clone().unwrap();
                let payload = message.payload.as_ref().unwrap().data.clone();
                Ok(ClientInbound::Message {
                    message_id,
                    payload,
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
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ClientOutbound {
    Send {
        message_id: MessageIdData,
        payload: Vec<u8>,
    },
    Ack(Vec<MessageIdData>),
    Ping,
    Pong,
    CloseProducer,
    CloseConsumer,
    AuthChallenge(Vec<u8>),
}

impl Into<Message> for ClientOutbound {
    fn into(self) -> Message {
        let command = match &self {
            ClientOutbound::Send { message_id, .. } => {
                let mut send = proto::pulsar::CommandSend::new();
                send.set_producer_id(0);
                send.set_sequence_id(0);
                send.set_num_messages(1);
                send.message_id = MessageField::some(message_id.clone());
                let mut base = proto::pulsar::BaseCommand::new();
                base.send = MessageField::some(send);
                base.set_type(proto::pulsar::base_command::Type::SEND.into());
                base
            }
            ClientOutbound::Ack(message_ids) => {
                let mut ack = proto::pulsar::CommandAck::new();
                ack.set_consumer_id(0);

                if message_ids.len() > 1 {
                    ack.set_ack_type(proto::pulsar::command_ack::AckType::Cumulative);
                } else {
                    ack.set_ack_type(proto::pulsar::command_ack::AckType::Individual);
                }

                ack.message_id = message_ids.clone();
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
        }
    }
}

impl Into<Outbound> for ClientOutbound {
    fn into(self) -> Outbound {
        Outbound::Client(self)
    }
}

impl TryFrom<&[u8]> for Message {
    type Error = std::io::Error;
    fn try_from(bytes: &[u8]) -> Result<Message, Self::Error> {
        let (bytes, command) = command_frame(bytes).unwrap();

        let (bytes, payload) = match bytes.is_empty() {
            false => payload_frame(bytes)
                .map(|(bytes, p)| (bytes, Some(p)))
                .unwrap_or((bytes, None)),
            true => (bytes, None),
        };

        let command = BaseCommand::parse_from_bytes(command.command).unwrap();

        let payload = payload.map(|p| Payload {
            metadata: MessageMetadata::parse_from_bytes(p.metadata).unwrap(),
            data: bytes.to_vec(),
        });

        Ok(Message { command, payload })
    }
}

impl TryFrom<Vec<u8>> for Message {
    type Error = std::io::Error;
    fn try_from(bytes: Vec<u8>) -> Result<Message, Self::Error> {
        bytes.as_slice().try_into()
    }
}

impl TryFrom<&Vec<u8>> for Message {
    type Error = std::io::Error;
    fn try_from(bytes: &Vec<u8>) -> Result<Message, Self::Error> {
        bytes.as_slice().try_into()
    }
}

impl Into<Vec<u8>> for Message {
    fn into(self) -> Vec<u8> {
        let mut buf = Vec::new();
        let command_bytes = self.command.write_to_bytes().unwrap();
        let command_size = self.command.compute_size() as u32;
        let header_size = if self.payload.is_some() { 18 } else { 8 };
        let metadata_size = self
            .payload
            .as_ref()
            .map_or(0, |p| p.metadata.compute_size() as u32);
        let payload_size = self.payload.as_ref().map_or(0, |p| p.data.len() as u32);
        let total_size = command_size + metadata_size + payload_size + header_size - 4;

        buf.put_u32(total_size);
        buf.put_u32(command_size);
        buf.put_slice(&command_bytes);

        self.payload.map(|payload| {
            let payload_bytes: Vec<u8> = payload.into();
            buf.put_slice(&payload_bytes);
        });

        buf
    }
}

impl Into<Vec<u8>> for Payload {
    fn into(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u16(0x0e01);
        buf.put_u32(0);
        buf.put_u32(self.metadata.compute_size() as u32);
        buf.put_slice(&self.metadata.write_to_bytes().unwrap());
        buf.put_slice(&self.data);
        let checksum = crc32c::crc32c(&buf[6..]);
        buf[2..6].copy_from_slice(&checksum.to_be_bytes());
        buf
    }
}

struct CommandFrame<'a> {
    #[allow(dead_code)]
    total_size: u32,
    #[allow(dead_code)]
    command_size: u32,
    command: &'a [u8],
}

fn command_frame(bytes: &[u8]) -> IResult<&[u8], CommandFrame> {
    let (bytes, total_size) = be_u32::<_, nom::error::Error<&[u8]>>(bytes).unwrap();
    let (bytes, command_size) = be_u32::<_, nom::error::Error<&[u8]>>(bytes).unwrap();
    let (bytes, command) = take::<_, _, nom::error::Error<&[u8]>>(command_size)(bytes).unwrap();
    Ok((
        bytes,
        CommandFrame {
            total_size,
            command_size,
            command,
        },
    ))
}

struct PayloadFrame<'a> {
    #[allow(dead_code)]
    magic_number: u16,
    #[allow(dead_code)]
    checksum: u32,
    #[allow(dead_code)]
    metadata_size: u32,
    metadata: &'a [u8],
}

fn payload_frame(bytes: &[u8]) -> IResult<&[u8], PayloadFrame> {
    let (bytes, magic_number) = be_u16(bytes)?;
    let (bytes, checksum) = be_u32(bytes)?;
    let (bytes, metadata_size) = be_u32(bytes)?;
    let (bytes, metadata) = take(metadata_size)(bytes)?;
    Ok((
        bytes,
        PayloadFrame {
            magic_number,
            checksum,
            metadata_size,
            metadata,
        },
    ))
}

#[derive(Debug, Clone)]
pub struct Payload {
    /// message metadata added by Pulsar
    pub metadata: MessageMetadata,
    /// raw message data
    pub data: Vec<u8>,
}

pub struct SendReceipt {
    pub message_id: MessageIdData,
    pub rx: futures::channel::oneshot::Receiver<()>,
}

impl SendReceipt {
    pub fn create_pair(
        message_id: &MessageIdData,
    ) -> (Self, futures::channel::oneshot::Sender<()>) {
        let (tx, rx) = futures::channel::oneshot::channel();
        (
            Self {
                message_id: message_id.clone(),
                rx,
            },
            tx,
        )
    }

    // TODO: This should be a Result of valid error types
    pub async fn wait_for_receipt(self) -> Result<(), ()> {
        Ok(self.rx.await.unwrap())
    }
}

#[cfg(test)]
mod test {
    use protobuf::Enum;

    use crate::message::Message;

    #[test]
    fn parse_payload_command() {
        let input: &[u8] = &[
            0x00, 0x00, 0x00, 0x3D, 0x00, 0x00, 0x00, 0x08, 0x08, 0x06, 0x32, 0x04, 0x08, 0x00,
            0x10, 0x08, 0x0E, 0x01, 0x42, 0x83, 0x54, 0xB5, 0x00, 0x00, 0x00, 0x19, 0x0A, 0x0E,
            0x73, 0x74, 0x61, 0x6E, 0x64, 0x61, 0x6C, 0x6F, 0x6E, 0x65, 0x2D, 0x30, 0x2D, 0x33,
            0x10, 0x08, 0x18, 0xBE, 0xC0, 0xFC, 0x84, 0xD2, 0x2C, 0x68, 0x65, 0x6C, 0x6C, 0x6F,
            0x2D, 0x70, 0x75, 0x6C, 0x73, 0x61, 0x72, 0x2D, 0x38,
        ];

        let message: Message = input.try_into().unwrap();
        let send = message.clone().command.send.unwrap();
        {
            assert_eq!(send.producer_id(), 0);
            assert_eq!(send.sequence_id(), 8);
        }
        {
            let payload = message.payload.as_ref().unwrap();
            assert_eq!(payload.metadata.producer_name(), "standalone-0-3");
            assert_eq!(payload.metadata.sequence_id(), 8);
            assert_eq!(payload.metadata.publish_time(), 1533850624062);
        }

        let output: Vec<u8> = message.into();
        assert_eq!(output.as_slice(), input);
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
