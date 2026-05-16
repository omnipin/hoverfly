//! Worker process for multi-worker upload.
//!
//! A worker is a thin pusher: it owns an ephemeral overlay key (random
//! by default), a [`SessionPool`] to mainnet, and a Unix socket the
//! coordinator connects to. It receives pre-stamped chunks over the
//! socket and pushes them with its own libp2p identity, reporting
//! per-chunk outcomes back. It never sees the batch-owner key — the
//! coordinator does all stamping upfront.
//!
//! Lifecycle:
//!   1. Bind socket, accept exactly one connection.
//!   2. Handshake: read [`Request::Hello`], reply [`Response::HelloAck`]
//!      with our overlay and actually-opened session count.
//!   3. Loop:
//!        - [`Request::PushBatch`] → push via
//!          [`push_chunks_with_pool_collect`], reply
//!          [`Response::BatchDone`] with per-chunk outcomes.
//!        - [`Request::GetStats`] → reply [`Response::Stats`].
//!        - [`Request::Shutdown`] → reply [`Response::ShuttingDown`],
//!          drain, persist peerlist observations, exit 0.
//!   4. On socket close (coordinator died), persist peerlist and exit.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::UnixListener;
use tracing::{info, warn};

use crate::client::{push_chunks_with_pool_collect, SessionPool};
use crate::multiwork::protocol::{
    read_frame, write_frame, ChunkOutcome, ChunkResult, Request, Response, PROTOCOL_VERSION,
};
use crate::peers::{apply_log, PeerStore};
use crate::signer::SwarmSigner;
use crate::transport::Transport;

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("client: {0}")]
    Client(#[from] crate::client::ClientError),
    #[error("signer: {0}")]
    Signer(#[from] crate::signer::SignerError),
    #[error("coordinator sent unexpected request: {0}")]
    UnexpectedRequest(&'static str),
    #[error("coordinator disconnected before handshake")]
    HandshakeAbort,
    #[error("protocol mismatch: coordinator wants v{0}, we speak v{1}")]
    ProtocolMismatch(u32, u32),
}

/// Configuration assembled from CLI flags + defaults.
pub struct WorkerConfig {
    pub socket: PathBuf,
    pub peerlist: PathBuf,
    pub concurrency: usize,
    pub max_retries: usize,
    /// `None` → random ephemeral overlay key (the usual case). When
    /// set, the worker uses this exact key for its libp2p identity
    /// (useful for testing or for a long-lived worker that wants
    /// stable bee-side reputation).
    pub overlay_key_hex: Option<String>,
}

/// Run the worker to completion: bind socket, accept one coordinator
/// connection, serve requests until Shutdown or disconnect, save
/// peerlist observations, return.
pub async fn run(
    cfg: WorkerConfig,
    transport_cfg: crate::transport::TransportConfig,
) -> Result<(), WorkerError> {
    let network_id = transport_cfg.network_id;
    let signer = match cfg.overlay_key_hex.as_deref() {
        Some(hex) => SwarmSigner::from_hex(hex, network_id)?,
        None => SwarmSigner::random(network_id),
    };
    let overlay_hex = hex::encode(signer.overlay());
    info!(target: "isheika::worker",
        "worker overlay: {} (network_id={})", overlay_hex, network_id);

    let transport = Transport::new(signer, transport_cfg);
    let mut peers = PeerStore::load_or_create(&cfg.peerlist);
    if peers.is_empty() {
        return Err(WorkerError::Client(crate::client::ClientError::NoPeers(
            format!("peerlist {} is empty", cfg.peerlist.display()),
        )));
    }

    let pool = Arc::new(SessionPool::open(&transport, &peers, cfg.concurrency).await?);
    info!(target: "isheika::worker",
        "session pool: {} session(s) open", pool.len());

    if cfg.socket.exists() {
        let _ = std::fs::remove_file(&cfg.socket);
    }
    let listener = UnixListener::bind(&cfg.socket)?;
    info!(target: "isheika::worker",
        "listening on {}", cfg.socket.display());

    let result = serve_one(&listener, &transport, &pool, &overlay_hex, cfg.max_retries).await;

    // Persist reachability observations regardless of how the loop exited.
    apply_log(&mut peers, transport.reachability_log());
    if let Err(e) = peers.save(&cfg.peerlist) {
        warn!(target: "isheika::worker",
            "failed to persist peerlist back to {}: {}", cfg.peerlist.display(), e);
    }
    let _ = std::fs::remove_file(&cfg.socket);

    result
}

/// Accept one coordinator connection, run the protocol loop, return
/// on graceful Shutdown or coordinator disconnect.
async fn serve_one(
    listener: &UnixListener,
    transport: &Transport,
    pool: &Arc<SessionPool>,
    overlay_hex: &str,
    max_retries: usize,
) -> Result<(), WorkerError> {
    let (mut stream, _addr) = listener.accept().await?;
    info!(target: "isheika::worker", "coordinator connected");

    // Stats counters. Bumped from the BatchDone response builder.
    let pushed_ok = AtomicU64::new(0);
    let pushed_failed = AtomicU64::new(0);

    // Handshake.
    match read_frame::<Request>(&mut stream).await? {
        Some(Request::Hello { protocol }) => {
            if protocol != PROTOCOL_VERSION {
                let msg = format!(
                    "protocol mismatch: coordinator v{}, worker v{}",
                    protocol, PROTOCOL_VERSION
                );
                let _ = write_frame(
                    &mut stream,
                    &Response::ProtocolError { message: msg.clone() },
                )
                .await;
                return Err(WorkerError::ProtocolMismatch(protocol, PROTOCOL_VERSION));
            }
            write_frame(
                &mut stream,
                &Response::HelloAck {
                    protocol: PROTOCOL_VERSION,
                    worker_overlay_hex: overlay_hex.to_string(),
                    sessions_open: pool.len(),
                },
            )
            .await?;
        }
        Some(_) => return Err(WorkerError::UnexpectedRequest("expected Hello first")),
        None => return Err(WorkerError::HandshakeAbort),
    }

    // Request loop.
    loop {
        let req = match read_frame::<Request>(&mut stream).await? {
            Some(r) => r,
            None => {
                info!(target: "isheika::worker",
                    "coordinator disconnected, exiting");
                return Ok(());
            }
        };
        match req {
            Request::Hello { .. } => {
                // Second Hello is a protocol error but recoverable.
                write_frame(
                    &mut stream,
                    &Response::ProtocolError {
                        message: "Hello already exchanged".into(),
                    },
                )
                .await?;
            }
            Request::PushBatch { chunks } => {
                let batch_len = chunks.len();
                let addrs: Vec<[u8; 32]> = chunks.iter().map(|c| c.addr).collect();
                let failures =
                    push_chunks_with_pool_collect(transport, pool, chunks, max_retries, None)
                        .await;
                let failure_map: std::collections::HashMap<[u8; 32], String> =
                    failures.into_iter().map(|f| (f.addr, f.error)).collect();
                let mut ok_in_batch: u64 = 0;
                let mut failed_in_batch: u64 = 0;
                let results: Vec<ChunkResult> = addrs
                    .into_iter()
                    .map(|addr| {
                        let outcome = match failure_map.get(&addr) {
                            Some(err) => {
                                failed_in_batch += 1;
                                ChunkOutcome::Failed { error: err.clone() }
                            }
                            None => {
                                ok_in_batch += 1;
                                ChunkOutcome::Ok
                            }
                        };
                        ChunkResult { addr, outcome }
                    })
                    .collect();
                pushed_ok.fetch_add(ok_in_batch, Ordering::Relaxed);
                pushed_failed.fetch_add(failed_in_batch, Ordering::Relaxed);
                info!(target: "isheika::worker",
                    "batch done: {}/{} ok ({} failed)",
                    ok_in_batch, batch_len, failed_in_batch);
                write_frame(&mut stream, &Response::BatchDone { results }).await?;
            }
            Request::GetStats => {
                write_frame(
                    &mut stream,
                    &Response::Stats {
                        sessions_alive: pool.len(),
                        sessions_total: pool.len(),
                        pushed_ok: pushed_ok.load(Ordering::Relaxed),
                        pushed_shallow: 0, // v1 doesn't distinguish shallow at worker level
                        pushed_failed: pushed_failed.load(Ordering::Relaxed),
                    },
                )
                .await?;
            }
            Request::Shutdown => {
                info!(target: "isheika::worker",
                    "shutdown requested, draining");
                write_frame(&mut stream, &Response::ShuttingDown).await?;
                return Ok(());
            }
        }
    }
}
