use bytes::{BufMut, Bytes};
use protobuf::Message as _;

use crate::message::proto::pulsar::{BaseCommand, MessageMetadata};
use crate::{error::NeutronError, message::MessageCommand};

pub struct Codec;

/// Wire layout:
///
/// ```text
/// [total_size: u32][command_size: u32][command]
///     ... optionally followed by ...
/// [magic: u16][checksum: u32][metadata_size: u32][metadata][payload]
/// ```
///
/// `total_size` counts everything after itself.
const FRAME_HEADER: usize = 8; // total_size + command_size
const PAYLOAD_HEADER: usize = 10; // magic + checksum + metadata_size
const MAGIC: u16 = 0x0e01;

#[derive(Debug, Clone)]
pub struct Payload {
    /// message metadata added by Pulsar
    pub metadata: MessageMetadata,
    /// raw message data — a zero-copy slice of the read buffer; cloning
    /// it is a reference-count bump, not a memcpy
    pub data: Bytes,
}

impl From<std::io::Error> for NeutronError {
    fn from(_err: std::io::Error) -> Self {
        NeutronError::Io
    }
}

impl tokio_util::codec::Encoder<MessageCommand> for Codec {
    type Error = NeutronError;

    fn encode(
        &mut self,
        item: MessageCommand,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let command_bytes = item
            .command
            .write_to_bytes()
            .map_err(|_| NeutronError::EncodeFailed)?;
        let command_size = command_bytes.len() as u32;

        match item.payload {
            None => {
                dst.reserve(FRAME_HEADER + command_bytes.len());
                dst.put_u32(4 + command_size);
                dst.put_u32(command_size);
                dst.put_slice(&command_bytes);
            }
            Some(payload) => {
                let metadata_bytes = payload
                    .metadata
                    .write_to_bytes()
                    .map_err(|_| NeutronError::EncodeFailed)?;
                let metadata_size = metadata_bytes.len() as u32;
                let total_size = 4
                    + command_size
                    + PAYLOAD_HEADER as u32
                    + metadata_size
                    + payload.data.len() as u32;

                dst.reserve(4 + total_size as usize);
                dst.put_u32(total_size);
                dst.put_u32(command_size);
                dst.put_slice(&command_bytes);

                dst.put_u16(MAGIC);
                let checksum_at = dst.len();
                dst.put_u32(0); // patched below
                let checksummed_from = dst.len();
                dst.put_u32(metadata_size);
                dst.put_slice(&metadata_bytes);
                dst.put_slice(&payload.data);

                let checksum = crc32c::crc32c(&dst[checksummed_from..]);
                dst[checksum_at..checksum_at + 4].copy_from_slice(&checksum.to_be_bytes());
            }
        }
        Ok(())
    }
}

impl tokio_util::codec::Decoder for Codec {
    type Item = MessageCommand;
    type Error = NeutronError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let total_size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize;
        let frame_size = total_size + 4;
        if src.len() < frame_size {
            src.reserve(frame_size - src.len());
            return Ok(None);
        }

        // The whole frame leaves the read buffer without copying; payload
        // bytes below are slices of this one allocation.
        let frame: Bytes = src.split_to(frame_size).freeze();

        let command_size =
            u32::from_be_bytes(frame[4..FRAME_HEADER].try_into().unwrap()) as usize;
        let command_end = FRAME_HEADER + command_size;
        if command_end > frame.len() {
            return Err(NeutronError::DecodeFailed);
        }
        let command = BaseCommand::parse_from_bytes(&frame[FRAME_HEADER..command_end])
            .map_err(|_| NeutronError::DecodeFailed)?;

        let payload = if command_end < frame.len() {
            let rest = &frame[command_end..];
            if rest.len() < PAYLOAD_HEADER {
                return Err(NeutronError::DecodeFailed);
            }
            let metadata_size = u32::from_be_bytes(rest[6..10].try_into().unwrap()) as usize;
            let metadata_end = PAYLOAD_HEADER + metadata_size;
            if metadata_end > rest.len() {
                return Err(NeutronError::DecodeFailed);
            }
            let metadata = MessageMetadata::parse_from_bytes(&rest[PAYLOAD_HEADER..metadata_end])
                .map_err(|_| NeutronError::DecodeFailed)?;
            let data = frame.slice(command_end + metadata_end..);
            Some(Payload { metadata, data })
        } else {
            None
        };

        Ok(Some(MessageCommand { command, payload }))
    }
}
