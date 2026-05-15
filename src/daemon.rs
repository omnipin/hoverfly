//! Optional long-running daemon. Holds a `Transport`, in-memory
//! `PeerStore`, and a lazily-initialised [`SessionPool`] across many
//! upload/fetch requests so each request skips the ~3-10 s session-pool
//! fill cost amortised in the CLI's one-shot mode.
//!
//! Unix-socket IPC only (cfg-gated `#[cfg(unix)]` in `lib.rs`). Wire
//! protocol: `u32-LE length` + JSON. Each request opens a fresh
//! connection, sends one request, reads one response, and closes —
//! simpler than a streaming protocol and good enough at local-IPC
//! latencies. File contents are transferred by absolute path; the
//! daemon must have FS read access to upload inputs and write access
//! to fetch outputs.
//!
//! The daemon is **not** a security boundary: anyone who can connect
//! to the socket can read/write files the daemon has access to and
//! sign uploads with whatever private key the caller supplies.

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::cache::ChunkCache;
use crate::client::{
    fetch_bytes_ex, fetch_manifest_path_ex, upload_bytes_with_pool, upload_collection,
    upload_file_with_manifest_with_pool, SessionPool,
};
use crate::peers::{apply_log, PeerStore};
use crate::signer::SwarmSigner;
use crate::transport::Transport;
use crate::{ClientError, UploadFile};

/// Maximum request/response payload size (JSON only — file bytes are
/// passed by path, not inline). 1 MiB is plenty for any conceivable
/// argument list.
const MAX_FRAME: u32 = 1 << 20;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Ping,
    Upload(UploadRequest),
    Fetch(FetchRequest),
    /// Refresh the daemon's in-memory peerlist from disk (after the
    /// user has run `isheika discover` against the same peerlist file).
    ReloadPeers,
    /// Save the current in-memory peerlist (with reachability
    /// observations) back to its file.
    SavePeers,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadRequest {
    pub file: PathBuf,
    pub batch: String,
    pub depth: u8,
    pub key: String,
    pub max_retries: usize,
    pub concurrency: usize,
    pub raw: bool,
    pub collection: bool,
    pub manifest_path: Option<String>,
    pub content_type: Option<String>,
    pub index_document: Option<String>,
    pub error_document: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub hash: String,
    pub path: Option<String>,
    pub output: PathBuf,
    pub max_retries: usize,
    pub concurrency: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Pong,
    Uploaded { root: String, bytes: usize },
    Fetched { bytes_written: usize, content_type: Option<String> },
    Ok,
    Err { message: String },
}

/// Server state held across requests.
struct State {
    transport: Arc<Transport>,
    signer_network_id: u64,
    peers: RwLock<PeerStore>,
    peerlist_path: PathBuf,
    /// Lazy: filled on the first upload, reused for all subsequent
    /// uploads. Wrapped in `RwLock<Option<Arc<SessionPool>>>` so an
    /// upload can either reuse the existing pool or (if `None`) take
    /// a write lock and build one.
    pool: RwLock<Option<Arc<SessionPool>>>,
    /// Target pool size — daemon owner picks this once at startup.
    pool_target: usize,
    /// Shared chunk cache populated by every upload (and optionally
    /// every fetch), served by the inbound retrieval responder so
    /// freshly uploaded roots are retrievable through bzz.limo / any
    /// bee peer that routes a retrieval request back to us.
    cache: ChunkCache,
}

/// Optional inbound listener configuration for [`run`].
pub struct ListenConfig {
    pub listen: libp2p::Multiaddr,
    /// Publicly-routable multiaddr to advertise (must already include
    /// the `/p2p/<peer-id>` tail when set). When `None`, falls back to
    /// loopback advertisement — sufficient for local testing but bee
    /// peers won't route retrieval requests back to us.
    pub advertise: Option<libp2p::Multiaddr>,
    /// Daemon identity. Used both as the libp2p keypair for the
    /// listener and as the bee handshake signer (overlay derived from
    /// its eth address + a random nonce).
    pub identity: SwarmSigner,
}

/// Run a daemon listening on `socket_path`. Blocks until a `Shutdown`
/// request arrives or the listener errors out. The peerlist file at
/// `peerlist_path` is loaded at startup and saved on graceful shutdown;
/// callers can also force a save via the `SavePeers` op.
///
/// `listen` (if `Some`) starts an additional libp2p inbound listener
/// that accepts bee retrieval/handshake/pricing streams and serves
/// chunks from the in-memory cache populated by uploads.
pub async fn run(
    socket_path: PathBuf,
    peerlist_path: PathBuf,
    network_id: u64,
    pool_target: usize,
    dial_timeout: std::time::Duration,
    op_timeout: std::time::Duration,
    listen: Option<ListenConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Remove any stale socket from a previous crashed run. The daemon
    // owns the socket file for its lifetime.
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    // When the daemon has a stable identity (--identity supplied with
    // --listen), share that signer + libp2p keypair across both the
    // outbound transport and the inbound listener. This keeps a single
    // overlay + peer-id across every connection we open or accept, so
    // bee peers can dial back to our advertised underlay and find the
    // same identity they handshook with outbound, then add us to their
    // kademlia tables for retrieval routing.
    let cfg = crate::TransportConfig {
        timeout: op_timeout,
        dial_timeout,
        network_id,
        advertise: listen.as_ref().and_then(|lc| lc.advertise.clone()),
    };
    let transport = match listen.as_ref() {
        Some(lc) => {
            let kp = crate::inbound::libp2p_keypair_from_identity(&lc.identity);
            Arc::new(Transport::new_with_keypair(lc.identity.clone(), cfg, kp))
        }
        None => Arc::new(Transport::new(crate::SwarmSigner::random(network_id), cfg)),
    };
    let peers = PeerStore::load_or_create(&peerlist_path);
    if peers.is_empty() {
        warn!(target: "isheika::daemon",
            "peerlist {} is empty — daemon will refuse uploads until populated",
            peerlist_path.display());
    }
    let cache = ChunkCache::new();
    let state = Arc::new(State {
        transport,
        signer_network_id: network_id,
        peers: RwLock::new(peers),
        peerlist_path: peerlist_path.clone(),
        pool: RwLock::new(None),
        pool_target,
        cache: cache.clone(),
    });

    // Spawn the inbound bee-protocol listener if configured. Failure
    // to bind is fatal — the user asked for this listener explicitly.
    if let Some(lc) = listen {
        let inbound_cfg = crate::inbound::InboundConfig {
            listen: lc.listen.clone(),
            advertise: lc.advertise.clone(),
            signer: lc.identity,
            op_timeout,
            idle_timeout: op_timeout,
            cache: cache.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = crate::inbound::run(inbound_cfg).await {
                warn!(target: "isheika::daemon", "inbound listener exited: {e}");
            }
        });
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(target: "isheika::daemon",
        "listening on {} (peerlist {}, pool_target {})",
        socket_path.display(), peerlist_path.display(), pool_target);

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!(target: "isheika::daemon", "shutdown requested");
                break;
            }
            accept = listener.accept() => {
                let (stream, _) = accept?;
                let state = state.clone();
                let shutdown_tx = shutdown_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(state, stream, shutdown_tx).await {
                        debug!(target: "isheika::daemon", "connection error: {}", e);
                    }
                });
            }
        }
    }

    // Persist the peerlist before exiting so reachability observations
    // collected during the daemon's lifetime aren't lost.
    let peers = state.peers.read().await;
    apply_log(&mut peers.clone(), state.transport.reachability_log());
    if let Err(e) = peers.save(&state.peerlist_path) {
        warn!(target: "isheika::daemon", "failed to save peerlist on shutdown: {}", e);
    }
    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}

async fn handle_conn(
    state: Arc<State>,
    mut stream: UnixStream,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
) -> std::io::Result<()> {
    let req = match read_frame::<Request>(&mut stream).await? {
        Some(r) => r,
        None => return Ok(()),
    };
    let response = match req {
        Request::Ping => Response::Pong,
        Request::Upload(r) => handle_upload(&state, r).await,
        Request::Fetch(r) => handle_fetch(&state, r).await,
        Request::ReloadPeers => {
            let new_peers = PeerStore::load_or_create(&state.peerlist_path);
            *state.peers.write().await = new_peers;
            // Existing pool is stale w.r.t. the new peerlist's
            // reachability data; drop it so the next upload rebuilds.
            *state.pool.write().await = None;
            Response::Ok
        }
        Request::SavePeers => {
            let mut peers = state.peers.write().await;
            apply_log(&mut *peers, state.transport.reachability_log());
            match peers.save(&state.peerlist_path) {
                Ok(()) => Response::Ok,
                Err(e) => Response::Err { message: format!("save: {e}") },
            }
        }
        Request::Shutdown => {
            let _ = shutdown_tx.send(()).await;
            Response::Ok
        }
    };
    write_frame(&mut stream, &response).await?;
    Ok(())
}

async fn handle_upload(state: &Arc<State>, r: UploadRequest) -> Response {
    let result: Result<(String, usize), ClientError> = (async {
        let signer = crate::SwarmSigner::from_hex(&r.key, state.signer_network_id)
            .map_err(|e| ClientError::Stamp(e.to_string()))?;
        let data = std::fs::read(&r.file).map_err(|e| ClientError::File(e.to_string()))?;
        let bytes = data.len();

        let peers_guard = state.peers.read().await;

        // Collections / single-entry manifests still build their own
        // pool via the existing helpers (they handle dedup + multiple
        // pre-stamp passes). Only the raw / single-file path benefits
        // from the persistent pool — that's where most repeat upload
        // throughput goes anyway.
        let is_tar = r.file
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.eq_ignore_ascii_case("tar"))
            .unwrap_or(false);

        let root = if (r.collection || (is_tar && !r.raw)) && !r.raw {
            // Collections still use the one-shot path: the dedup + multi-
            // stamp logic in upload_collection is complex enough that
            // refactoring it for an external pool is a follow-up.
            let files = read_tar_files(&data)
                .map_err(|e| ClientError::File(e.to_string()))?;
            if files.is_empty() {
                return Err(ClientError::File("tar archive contains no regular files".into()));
            }
            upload_collection(
                &state.transport,
                &*peers_guard,
                &signer,
                &r.batch,
                r.depth,
                files,
                r.index_document.as_deref(),
                r.error_document.as_deref(),
                r.max_retries,
                r.concurrency,
            )
            .await?
        } else {
            // Raw and single-file-with-manifest uploads go through the
            // persistent pool. First request lazily fills it; subsequent
            // ones reuse it with zero dial-fill cost.
            let pool = ensure_pool(state, &*peers_guard).await?;
            if r.raw {
                upload_bytes_with_pool(
                    &state.transport,
                    &*pool,
                    &signer,
                    &r.batch,
                    r.depth,
                    &data,
                    r.max_retries,
                    Some(&state.cache),
                )
                .await?
            } else {
                let path = r.manifest_path.clone().unwrap_or_else(|| {
                    r.file
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| "file".to_string())
                });
                let ct = r.content_type.clone();
                upload_file_with_manifest_with_pool(
                    &state.transport,
                    &*pool,
                    &signer,
                    &r.batch,
                    r.depth,
                    &data,
                    &path,
                    ct.as_deref(),
                    r.max_retries,
                    Some(&state.cache),
                )
                .await?
            }
        };
        Ok((hex::encode(root.as_bytes()), bytes))
    })
    .await;
    match result {
        Ok((root, bytes)) => Response::Uploaded { root, bytes },
        Err(e) => Response::Err { message: e.to_string() },
    }
}

/// Ensure the daemon's persistent pool exists and has at least one
/// reachable session. Subsequent uploads see a pre-filled pool with no
/// dial-fill cost.
async fn ensure_pool(
    state: &Arc<State>,
    peers: &PeerStore,
) -> Result<Arc<SessionPool>, ClientError> {
    {
        let guard = state.pool.read().await;
        if let Some(p) = guard.as_ref() {
            if !p.is_empty() {
                return Ok(p.clone());
            }
        }
    }
    let mut guard = state.pool.write().await;
    if let Some(p) = guard.as_ref() {
        if !p.is_empty() {
            return Ok(p.clone());
        }
    }
    let pool = Arc::new(SessionPool::open(&state.transport, peers, state.pool_target).await?);
    info!(target: "isheika::daemon",
        "warm pool: {} session(s) open", pool.len());
    *guard = Some(pool.clone());
    Ok(pool)
}

async fn handle_fetch(state: &Arc<State>, r: FetchRequest) -> Response {
    let result: Result<(usize, Option<String>), ClientError> = (async {
        let peers = state.peers.read().await;
        let (bytes, ct) = if let Some(p) = r.path.as_deref() {
            let (b, c) =
                fetch_manifest_path_ex(&state.transport, &*peers, &r.hash, p, r.max_retries, r.concurrency)
                    .await?;
            (b, c)
        } else {
            let b = fetch_bytes_ex(&state.transport, &*peers, &r.hash, r.max_retries, r.concurrency)
                .await?;
            (b, None)
        };
        std::fs::write(&r.output, &bytes).map_err(|e| ClientError::File(e.to_string()))?;
        Ok((bytes.len(), ct))
    })
    .await;
    match result {
        Ok((bytes_written, content_type)) => Response::Fetched { bytes_written, content_type },
        Err(e) => Response::Err { message: e.to_string() },
    }
}

fn read_tar_files(bytes: &[u8]) -> Result<Vec<UploadFile>, Box<dyn std::error::Error>> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
    let mut out = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let header = entry.header().clone();
        if !header.entry_type().is_file() {
            continue;
        }
        let path = entry.path()?.to_string_lossy().into_owned();
        let path = path.trim_start_matches("./").to_string();
        if path.is_empty() || path == "." {
            continue;
        }
        let mut data = Vec::with_capacity(header.size().unwrap_or(0) as usize);
        std::io::Read::read_to_end(&mut entry, &mut data)?;
        let content_type = crate::mime::guess_from_path(&path).map(str::to_string);
        out.push(UploadFile { path, content_type, data });
    }
    Ok(out)
}

// ---- client side ----

/// Connect to a daemon listening on `socket_path` and exchange one
/// `request → response` round-trip. Returns the deserialized response
/// or an IO/protocol error.
pub async fn call(
    socket_path: &std::path::Path,
    request: &Request,
) -> std::io::Result<Response> {
    let mut stream = UnixStream::connect(socket_path).await?;
    write_frame(&mut stream, request).await?;
    let resp = read_frame::<Response>(&mut stream)
        .await?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "daemon hung up"))?;
    Ok(resp)
}

// ---- wire protocol ----

async fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut UnixStream,
) -> std::io::Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len} > {MAX_FRAME}"),
        ));
    }
    let mut body = vec![0u8; len as usize];
    stream.read_exact(&mut body).await?;
    let val: T = serde_json::from_slice(&body)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(val))
}

async fn write_frame<T: Serialize>(
    stream: &mut UnixStream,
    value: &T,
) -> std::io::Result<()> {
    let body = serde_json::to_vec(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if body.len() > MAX_FRAME as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}


