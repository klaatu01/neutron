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

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ClientCommand {
    Send(Vec<u8>),
    Ack(Vec<MessageIdData>),
    Ping,
    Pong,
    Connect { auth_data: Option<Vec<u8>> },
    CloseProducer,
    CloseConsumer,
    AuthChallenge(Vec<u8>),
}

impl Into<Message> for ClientCommand {
    fn into(self) -> Message {
        let command = match &self {
            ClientCommand::Send(_) => {
                let mut send = proto::pulsar::CommandSend::new();
                send.set_producer_id(0);
                send.set_sequence_id(0);
                send.set_num_messages(1);
                let mut base = proto::pulsar::BaseCommand::new();
                base.send = MessageField::some(send);
                base.set_type(proto::pulsar::base_command::Type::SEND.into());
                base
            }
            ClientCommand::Ack(message_ids) => {
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
            ClientCommand::Ping => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::PING);
                let ping = proto::pulsar::CommandPing::new();
                base.ping = MessageField::some(ping);
                base
            }
            ClientCommand::Pong => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::PONG);
                let pong = proto::pulsar::CommandPong::new();
                base.pong = MessageField::some(pong);
                base
            }
            ClientCommand::Connect { auth_data } => {
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
                base
            }
            ClientCommand::CloseProducer => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::CLOSE_PRODUCER.into());
                base
            }
            ClientCommand::CloseConsumer => {
                let mut base = proto::pulsar::BaseCommand::new();
                base.set_type(proto::pulsar::base_command::Type::CLOSE_CONSUMER.into());
                base
            }
            ClientCommand::AuthChallenge(bytes) => {
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
            ClientCommand::Send(bytes) => Some(Payload {
                metadata: MessageMetadata::new(),
                data: bytes,
            }),
            _ => None,
        };

        Message { command, payload }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ServerMessage {
    SendReceipt {
        message_id: MessageIdData,
    },
    Message {
        message_id: MessageIdData,
        payload: Vec<u8>,
    },
    Ping,
    Connected,
    AuthChallenge,
}

impl TryFrom<Message> for ServerMessage {
    type Error = std::io::Error;
    fn try_from(message: Message) -> Result<ServerMessage, Self::Error> {
        let command = message.command;
        let _payload = message.payload;
        match command.type_() {
            //           proto::pulsar::base_command::Type::SEND_RECEIPT => {
            //               let send_receipt =
            //                   command
            //                       .send_receipt
            //                       .into_option()
            //                       .ok_or(std::io::Error::new(
            //                           std::io::ErrorKind::Other,
            //                           "Missing send receipt",
            //                       ))?;
            //               let message_id =
            //                   send_receipt
            //                       .message_id
            //                       .into_option()
            //                       .ok_or(std::io::Error::new(
            //                           std::io::ErrorKind::Other,
            //                           "Missing message id",
            //                       ))?;
            //               Ok(ServerMessages::SendReceipt { message_id })
            //           }
            //           proto::pulsar::base_command::Type::MESSAGE => {
            //               let message_id = command.message.unwrap().message_id.into_option().ok_or(
            //                   std::io::Error::new(std::io::ErrorKind::Other, "Missing message id"),
            //               )?;
            //               let payload = payload.unwrap();
            //               Ok(ServerMessages::Message {
            //                   message_id,
            //                   payload: payload.data,
            //               })
            //           }
            //            proto::pulsar::base_command::Type::PONG => Ok(ServerMessages::Pong),
            //            proto::pulsar::base_command::Type::AUTH_CHALLENGE => Ok(ServerMessages::AuthChallenge),
            proto::pulsar::base_command::Type::CONNECTED => Ok(ServerMessage::Connected),
            proto::pulsar::base_command::Type::PING => Ok(ServerMessage::Ping),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unknown command type",
            )),
        }
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