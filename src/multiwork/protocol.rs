//! Wire protocol for multi-worker upload IPC.
//!
//! Coordinator opens a unix socket to each spawned worker, performs a
//! version handshake, then sends [`Request::PushBatch`] messages
//! carrying pre-stamped chunks. The worker pushes them through its
//! own session pool and replies with [`Response::BatchDone`] reporting
//! per-chunk outcomes. The conversation continues until the
//! coordinator sends [`Request::Shutdown`] (or the socket closes).
//!
//! Framing mirrors `src/daemon.rs`: `u32-LE length || body`. The body
//! is bincode-encoded rather than JSON because we ship gigabytes of
//! binary chunk data and JSON's u8-as-integer-array encoding would
//! inflate that 3-4× on the wire.
//!
//! Protocol versioning: [`Hello`]/[`HelloAck`] carry a single u32 that
//! is bumped whenever the message layout changes incompatibly. The
//! worker refuses to talk if the coordinator's version doesn't match
//! its own.

use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::client::StampedChunk;

/// Wire protocol version. Bumped on incompatible layout changes.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum frame size — 64 MiB. Generous so a coordinator can ship
/// large batches without the worker erroring out, but bounded so a
/// runaway frame size can't OOM us. Practical coordinator batches are
/// 64-256 stamped chunks, ~256 KiB-1 MiB on the wire.
pub const MAX_FRAME: u32 = 64 << 20;

// ---- coordinator → worker ----

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Initial handshake. Coordinator sends first.
    Hello { protocol: u32 },
    /// Push a batch of pre-stamped chunks. Worker processes them and
    /// replies with [`Response::BatchDone`] reporting per-chunk
    /// outcomes. Coordinator may have many `PushBatch` in flight if
    /// the worker is configured for pipelined batches; v1 is
    /// strictly request-response (one batch at a time per worker)
    /// for simplicity.
    PushBatch { chunks: Vec<StampedChunk> },
    /// Request current statistics for coordinator-side monitoring.
    /// Worker replies with [`Response::Stats`] without disrupting
    /// any in-flight `PushBatch`.
    GetStats,
    /// Graceful shutdown. Worker drains its in-flight pushes, replies
    /// [`Response::ShuttingDown`], persists its peerlist
    /// observations, and exits with status 0.
    Shutdown,
}

// ---- worker → coordinator ----

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    /// Reply to [`Request::Hello`]. Carries the worker's overlay
    /// (informational, for coordinator logs) and its actually-opened
    /// session-pool size so the coordinator can match batch sizes to
    /// worker capacity.
    HelloAck {
        protocol: u32,
        worker_overlay_hex: String,
        sessions_open: usize,
    },
    /// Reply to [`Request::PushBatch`]. One [`ChunkResult`] per chunk
    /// in the request, **in the same order** as the request batch.
    /// Order is the contract the coordinator relies on to correlate
    /// results with input chunks.
    BatchDone { results: Vec<ChunkResult> },
    /// Reply to [`Request::GetStats`].
    Stats {
        sessions_alive: usize,
        sessions_total: usize,
        pushed_ok: u64,
        pushed_shallow: u64,
        pushed_failed: u64,
    },
    /// Reply to [`Request::Shutdown`]. Worker exits immediately
    /// after sending this; coordinator should not send more requests.
    ShuttingDown,
    /// Recoverable per-message error (e.g. malformed request, version
    /// mismatch, batch too large). The worker stays connected; the
    /// coordinator can adapt and try again.
    ProtocolError { message: String },
}

/// Per-chunk push outcome reported by the worker.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkResult {
    pub addr: [u8; 32],
    pub outcome: ChunkOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ChunkOutcome {
    /// Bee returned a non-shallow receipt — chunk is durably stored
    /// in at least one peer's reserve.
    Ok,
    /// Bee returned a shallow receipt — chunk was forwarded but not
    /// stored in any peer's reserve. Coordinator may retry on
    /// another worker or accept the shallow outcome (mirrors the
    /// in-process dispatcher's fallback at `client.rs:1621`).
    Shallow,
    /// All push attempts within this worker failed for this chunk.
    /// Coordinator should retry on another worker.
    Failed { error: String },
}

// ---- framing ----

/// Read one length-prefixed frame from the socket and deserialize it
/// as `T` via bincode. Returns `Ok(None)` if the socket was closed
/// cleanly before any bytes were read (graceful peer disconnect).
pub async fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut UnixStream,
) -> io::Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} > {MAX_FRAME}"),
        ));
    }
    let mut body = vec![0u8; len as usize];
    stream.read_exact(&mut body).await?;
    let val: T = bincode::deserialize(&body)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(val))
}

/// Serialize `value` via bincode and write it to the socket as a
/// length-prefixed frame.
pub async fn write_frame<T: Serialize>(
    stream: &mut UnixStream,
    value: &T,
) -> io::Result<()> {
    let body = bincode::serialize(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if body.len() > MAX_FRAME as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} > {}", body.len(), MAX_FRAME),
        ));
    }
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Round-trip a Hello/HelloAck handshake over a real `UnixStream`
    /// pair. Covers framing, bincode encoding, and the request/response
    /// enum shapes in one shot.
    #[tokio::test]
    async fn hello_handshake_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let req: Request = read_frame(&mut stream).await.unwrap().unwrap();
            match req {
                Request::Hello { protocol } => {
                    assert_eq!(protocol, PROTOCOL_VERSION);
                    write_frame(
                        &mut stream,
                        &Response::HelloAck {
                            protocol: PROTOCOL_VERSION,
                            worker_overlay_hex: "0xdead".into(),
                            sessions_open: 7,
                        },
                    )
                    .await
                    .unwrap();
                }
                _ => panic!("expected Hello"),
            }
        });

        let mut client = UnixStream::connect(&path).await.unwrap();
        write_frame(&mut client, &Request::Hello { protocol: PROTOCOL_VERSION })
            .await
            .unwrap();
        let resp: Response = read_frame(&mut client).await.unwrap().unwrap();
        match resp {
            Response::HelloAck { protocol, worker_overlay_hex, sessions_open } => {
                assert_eq!(protocol, PROTOCOL_VERSION);
                assert_eq!(worker_overlay_hex, "0xdead");
                assert_eq!(sessions_open, 7);
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }
        server.await.unwrap();
    }

    /// Round-trip a PushBatch containing a single stamped chunk and
    /// confirm the bytes survive bincode unmodified.
    #[tokio::test]
    async fn push_batch_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let chunk = StampedChunk {
            addr: [0xab; 32],
            wire: (0..256).map(|i| i as u8).collect(),
            stamp: vec![0xcd; 113],
        };
        let chunk_clone = chunk.clone();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let req: Request = read_frame(&mut stream).await.unwrap().unwrap();
            match req {
                Request::PushBatch { chunks } => {
                    assert_eq!(chunks.len(), 1);
                    assert_eq!(chunks[0].addr, [0xab; 32]);
                    assert_eq!(chunks[0].wire.len(), 256);
                    assert_eq!(chunks[0].stamp, vec![0xcd; 113]);
                    write_frame(
                        &mut stream,
                        &Response::BatchDone {
                            results: vec![ChunkResult {
                                addr: chunks[0].addr,
                                outcome: ChunkOutcome::Ok,
                            }],
                        },
                    )
                    .await
                    .unwrap();
                }
                _ => panic!("expected PushBatch"),
            }
        });

        let mut client = UnixStream::connect(&path).await.unwrap();
        write_frame(&mut client, &Request::PushBatch { chunks: vec![chunk_clone] })
            .await
            .unwrap();
        let resp: Response = read_frame(&mut client).await.unwrap().unwrap();
        match resp {
            Response::BatchDone { results } => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].addr, [0xab; 32]);
                assert!(matches!(results[0].outcome, ChunkOutcome::Ok));
            }
            other => panic!("expected BatchDone, got {other:?}"),
        }
        server.await.unwrap();
    }
}
