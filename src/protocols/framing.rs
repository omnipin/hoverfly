//! Length-prefixed protobuf framing used by all bee wire protocols.
//!
//! Bee uses a varint-prefixed payload (the protobuf wire convention). For
//! unary message exchanges we read/write a single uvarint length followed by
//! the payload bytes.

use bytes::{Buf, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use prost::Message;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("frame too large: {0}")]
    TooLarge(usize),
}

const MAX_FRAME: usize = 16 * 1024 * 1024;

pub async fn write_message<W: AsyncWrite + Unpin, M: Message>(
    w: &mut W,
    msg: &M,
) -> Result<(), FrameError> {
    let mut buf = Vec::with_capacity(msg.encoded_len() + 4);
    msg.encode_length_delimited(&mut buf)
        .map_err(|e| FrameError::Decode(e.to_string()))?;
    w.write_all(&buf)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;
    w.flush().await.map_err(|e| FrameError::Io(e.to_string()))?;
    Ok(())
}

pub async fn read_message<R: AsyncRead + Unpin, M: Message + Default>(
    r: &mut R,
) -> Result<M, FrameError> {
    let len = read_uvarint(r).await?;
    if len > MAX_FRAME {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;
    M::decode(&buf[..]).map_err(|e| FrameError::Decode(e.to_string()))
}

async fn read_uvarint<R: AsyncRead + Unpin>(r: &mut R) -> Result<usize, FrameError> {
    let mut byte = [0u8; 1];
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        r.read_exact(&mut byte)
            .await
            .map_err(|e| FrameError::Io(e.to_string()))?;
        let b = byte[0] as u64;
        value |= (b & 0x7f) << shift;
        if (b & 0x80) == 0 {
            return Ok(value as usize);
        }
        shift += 7;
    }
    Err(FrameError::Decode("uvarint overflow".into()))
}

/// Strip a single length-delimited message from a `BytesMut` if available.
pub fn try_take_message<M: Message + Default>(buf: &mut BytesMut) -> Result<Option<M>, FrameError> {
    let mut shift = 0u32;
    let mut len: u64 = 0;
    let bytes = &buf[..];
    for i in 0..bytes.len().min(10) {
        let b = bytes[i] as u64;
        len |= (b & 0x7f) << shift;
        let header_len = i + 1;
        if (b & 0x80) == 0 {
            let total = header_len + len as usize;
            if buf.len() < total {
                return Ok(None);
            }
            buf.advance(header_len);
            let payload = buf.split_to(len as usize);
            let msg = M::decode(&payload[..]).map_err(|e| FrameError::Decode(e.to_string()))?;
            return Ok(Some(msg));
        }
        shift += 7;
    }
    Ok(None)
}
