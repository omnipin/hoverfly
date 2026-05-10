//! Bee pushsync protocol — `/swarm/pushsync/1.3.0/pushsync`.
//!
//! Client opens stream → sends `Headers { headers: [] }` → sends
//! `Delivery { address, data, stamp }` → reads `Headers` → reads `Receipt`.

use crate::proto::headers as hdr;
use crate::proto::pushsync as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/pushsync/1.3.1/pushsync";

#[derive(Debug, Error)]
pub enum PushsyncError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
    #[error("peer error: {0}")]
    Peer(String),
}

#[derive(Debug, Clone)]
pub struct PushsyncReceipt {
    pub address: Vec<u8>,
    pub signature: Vec<u8>,
    pub nonce: Vec<u8>,
    pub storage_radius: u32,
}

/// Push a single chunk and read the receipt.
///
/// `chunk_data` must already include the 8-byte LE span prefix (i.e. the
/// nectar `ContentChunk::data()` framing).
pub async fn push<S>(
    stream: &mut S,
    address: &[u8; 32],
    chunk_data: &[u8],
    stamp: &[u8],
) -> Result<PushsyncReceipt, PushsyncError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // 1. Send empty request headers.
    let req_headers = hdr::Headers { headers: vec![] };
    write_message(stream, &req_headers).await?;

    // 2. Read response headers (ignored).
    let _resp_headers: hdr::Headers = read_message(stream).await?;

    // 3. Send delivery.
    let delivery = pb::Delivery {
        address: address.to_vec(),
        data: chunk_data.to_vec(),
        stamp: stamp.to_vec(),
    };
    write_message(stream, &delivery).await?;

    // 4. Read receipt.
    let receipt: pb::Receipt = read_message(stream).await?;
    if !receipt.err.is_empty() {
        return Err(PushsyncError::Peer(receipt.err));
    }
    Ok(PushsyncReceipt {
        address: receipt.address,
        signature: receipt.signature,
        nonce: receipt.nonce,
        storage_radius: receipt.storage_radius,
    })
}
