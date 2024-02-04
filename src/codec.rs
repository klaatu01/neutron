use std::io::Cursor;

use bytes::{Buf, BufMut};
use nom::bytes::streaming::take;
use nom::number::streaming::{be_u16, be_u32};
use nom::IResult;
use protobuf::Message as _;

use crate::message::proto::pulsar::{BaseCommand, MessageMetadata};
use crate::{error::NeutronError, message::Message};

pub struct Codec;

struct CommandFrame<'a> {
    #[allow(dead_code)]
    pub(crate) total_size: u32,
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

#[derive(Debug, Clone)]
pub struct Payload {
    /// message metadata added by Pulsar
    pub metadata: MessageMetadata,
    /// raw message data
    pub data: Vec<u8>,
}

fn payload_frame(bytes: &[u8]) -> IResult<&[u8], PayloadFrame> {
    let (bytes, magic_number) = be_u16(bytes)?;
    let (bytes, checksum) = be_u32(bytes)?;
    let (bytes, metadata_size) = be_u32(bytes)?;
    let (bytes, metadata) = take(metadata_size)(bytes)?;
    let (bytes, amount_until_payload) = be_u32(bytes)?;
    let (bytes, _) = take(amount_until_payload)(bytes)?;
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

impl From<std::io::Error> for NeutronError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: std::io::Error) -> Self {
        NeutronError::Io
    }
}

impl tokio_util::codec::Encoder<Message> for Codec {
    type Error = NeutronError;
    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut buf = Vec::new();
        let command_bytes = item.command.write_to_bytes().unwrap();
        let command_size = item.command.compute_size() as u32;
        let header_size = if item.payload.is_some() { 18 } else { 8 };
        let metadata_size = item
            .payload
            .as_ref()
            .map_or(0, |p| p.metadata.compute_size() as u32);
        let payload_size = item.payload.as_ref().map_or(0, |p| p.data.len() as u32);
        let total_size = command_size + metadata_size + payload_size + header_size - 4;

        buf.put_u32(total_size);
        buf.put_u32(command_size);
        buf.put_slice(&command_bytes);

        item.payload.map(|payload| {
            buf.put_u16(0x0e01);
            buf.put_u32(0);
            buf.put_u32(payload.metadata.compute_size() as u32);
            buf.put_slice(&payload.metadata.write_to_bytes().unwrap());
            buf.put_slice(&payload.data);
            let checksum = crc32c::crc32c(&buf[6..]);
            buf[2..6].copy_from_slice(&checksum.to_be_bytes());
        });

        dst.extend_from_slice(&buf);
        Ok(())
    }
}

impl tokio_util::codec::Decoder for Codec {
    type Item = Message;
    type Error = NeutronError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let mut buf = Cursor::new(src);
        let message_size = buf.get_u32() as usize + 4;
        let src = buf.into_inner();
        if src.len() >= message_size {
            let (bytes, command_frame) = command_frame(&src[..message_size]).unwrap();
            let command = BaseCommand::parse_from_bytes(command_frame.command).unwrap();

            let (bytes, payload_frame) = match bytes.is_empty() {
                false => payload_frame(bytes)
                    .map(|(bytes, p)| (bytes, Some(p)))
                    .map_err(|e| e)
                    .unwrap_or((bytes, None)),
                true => (bytes, None),
            };

            let payload = payload_frame.as_ref().map(|p| {
                let metadata = MessageMetadata::parse_from_bytes(p.metadata).unwrap();
                Payload {
                    metadata,
                    data: bytes.to_vec(),
                }
            });

            src.advance(message_size as usize);

            let message = Message { command, payload };

            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
}
