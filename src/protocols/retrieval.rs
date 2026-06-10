//! Bee retrieval protocol — `/swarm/retrieval/1.4.0/retrieval`.
//!
//! Client opens stream → sends `Headers { headers: [] }` → sends
//! `Request { addr }` → reads `Headers` (response headers) → reads
//! `Delivery { data, stamp, err }`.

use crate::proto::headers as hdr;
use crate::proto::retrieval as pb;
use crate::protocols::framing::{FrameError, read_message, write_message};
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
///
/// The substream is **gracefully closed** before returning (on every path).
/// One retrieval = one substream, and bee-side yamux caps the number of
/// concurrent streams per connection. A plain `drop` of the libp2p `Stream`
/// only resets it lazily, so under rapid back-to-back fetches over one warm
/// session the per-connection stream slots fill up and subsequent
/// `open_stream` calls block until they time out — observed as retrieval
/// wedging after ~150 chunks (and a reload squeezing through only a handful
/// more, because the SharedWorker daemon keeps the same saturated sessions).
/// Closing the write side here lets yamux free the slot promptly, mirroring
/// bee's half-close after it finishes a retrieval exchange.
pub async fn fetch<S>(stream: &mut S, addr: &[u8; 32]) -> Result<ChunkDelivery, RetrievalError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let result = fetch_inner(stream, addr).await;
    // Best-effort graceful close regardless of outcome; the slot must be
    // released even when the exchange errored.
    close(stream).await;
    result
}

async fn fetch_inner<S>(stream: &mut S, addr: &[u8; 32]) -> Result<ChunkDelivery, RetrievalError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // 1. Send empty request headers.
    let req_headers = hdr::Headers { headers: vec![] };
    write_message(stream, &req_headers).await?;

    // 2. Read response headers (ignored).
    let _resp_headers: hdr::Headers = read_message(stream).await?;

    // 3. Send request.
    let req = pb::Request {
        addr: addr.to_vec(),
    };
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

/// Gracefully close the write half of a finished retrieval substream so the
/// yamux stream slot is released promptly instead of leaking until reset.
async fn close<S>(stream: &mut S)
where
    S: futures::AsyncWrite + Unpin,
{
    use futures::AsyncWriteExt;
    let _ = stream.close().await;
}

/// Server-side retrieval handler. Mirrors `fetch` from the other end:
/// read empty headers, write empty headers, read `Request { addr }`,
/// then call `lookup(addr)` and write the resulting `Delivery`.
///
/// `lookup` returns `Some((data, stamp))` when the chunk is locally
/// available, or `None` to respond with an empty-data error
/// (`Delivery { err: "chunk not found" }`). Mirroring bee's behaviour,
/// any framing / IO error during the write of an error response is
/// silently dropped — the peer will time out the stream and move on.
pub async fn respond<S, F>(stream: &mut S, lookup: F) -> Result<(), RetrievalError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
    F: FnMut(&[u8; 32]) -> Option<(Vec<u8>, Vec<u8>)>,
{
    let result = respond_inner(stream, lookup).await;
    // Release the inbound stream slot promptly (same rationale as `fetch`).
    close(stream).await;
    result
}

async fn respond_inner<S, F>(stream: &mut S, mut lookup: F) -> Result<(), RetrievalError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
    F: FnMut(&[u8; 32]) -> Option<(Vec<u8>, Vec<u8>)>,
{
    use crate::proto::headers as hdr;

    // 1. Read empty request headers.
    let _req_headers: hdr::Headers = read_message(stream).await?;

    // 2. Write empty response headers.
    let resp_headers = hdr::Headers { headers: vec![] };
    write_message(stream, &resp_headers).await?;

    // 3. Read request.
    let req: pb::Request = read_message(stream).await?;
    let mut addr = [0u8; 32];
    if req.addr.len() != 32 {
        let err = pb::Delivery {
            data: vec![],
            stamp: vec![],
            err: format!("invalid address length: {}", req.addr.len()),
        };
        write_message(stream, &err).await?;
        return Ok(());
    }
    addr.copy_from_slice(&req.addr);

    // 4. Look up and respond.
    match lookup(&addr) {
        Some((data, stamp)) => {
            let delivery = pb::Delivery {
                data,
                stamp,
                err: String::new(),
            };
            write_message(stream, &delivery).await?;
        }
        None => {
            let err = pb::Delivery {
                data: vec![],
                stamp: vec![],
                err: "chunk not found".to_string(),
            };
            write_message(stream, &err).await?;
        }
    }
    Ok(())
}
