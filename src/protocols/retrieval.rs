//! Bee retrieval protocol — `/swarm/retrieval/1.4.0/retrieval`.
//!
//! Client opens stream → sends `Headers { headers: [] }` → sends
//! `Request { addr }` → reads `Headers` (response headers) → reads
//! `Delivery { data, stamp, err }`.

use crate::proto::headers as hdr;
use crate::proto::retrieval as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/retrieval/1.4.0/retrieval";

#[derive(Debug, Error)]
pub enum RetrievalError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
    #[error("peer error: {0}")]
    Peer(String),
    #[error("empty delivery")]
    Empty,
}

#[derive(Debug, Clone)]
pub struct ChunkDelivery {
    pub data: Vec<u8>,
    pub stamp: Vec<u8>,
}

/// Send a single retrieval request and read the delivery.
pub async fn fetch<S>(stream: &mut S, addr: &[u8; 32]) -> Result<ChunkDelivery, RetrievalError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // 1. Send empty request headers.
    let req_headers = hdr::Headers { headers: vec![] };
    write_message(stream, &req_headers).await?;

    // 2. Read response headers (ignored).
    let _resp_headers: hdr::Headers = read_message(stream).await?;

    // 3. Send request.
    let req = pb::Request { addr: addr.to_vec() };
    write_message(stream, &req).await?;

    // 4. Read delivery.
    let delivery: pb::Delivery = read_message(stream).await?;
    if !delivery.err.is_empty() {
        return Err(RetrievalError::Peer(delivery.err));
    }
    if delivery.data.is_empty() {
        return Err(RetrievalError::Empty);
    }
    Ok(ChunkDelivery {
        data: delivery.data,
        stamp: delivery.stamp,
    })
}
