use bytes::BufMut;
use nom::{
    bytes::streaming::take,
    number::streaming::{be_u16, be_u32},
    IResult,
};
use protobuf::Message as _;

use self::proto::pulsar::{BaseCommand, CommandSendReceipt, MessageIdData, MessageMetadata};

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
    pub rx: futures::channel::oneshot::Receiver<CommandSendReceipt>,
}

impl SendReceipt {
    pub fn create_pair(
        message_id: &MessageIdData,
    ) -> (Self, futures::channel::oneshot::Sender<CommandSendReceipt>) {
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
    pub async fn wait_for_receipt(self) -> Result<CommandSendReceipt, ()> {
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
