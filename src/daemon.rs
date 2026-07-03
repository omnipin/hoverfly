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
    RetrievalCache, SessionPool, discover_recursive_with_progress, fetch_bytes_cached_ex,
    fetch_manifest_path_cached_ex, upload_bytes_with_pool, upload_collection,
    upload_file_with_manifest_with_pool,
};
use crate::doh::Doh;
use crate::peers::{PeerStore, apply_log};
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
    /// user has run `hoverfly discover` against the same peerlist file).
    ReloadPeers,
    /// Save the current in-memory peerlist (with reachability
    /// observations) back to its file.
    SavePeers,
    /// Query the daemon's current pool + peerlist stats. Replies with
    /// [`Response::Status`].
    Status,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadRequest {
    pub file: PathBuf,
    pub batch: String,
    pub depth: u8,
    /// Whether the batch is immutable (fill-only stamping) vs mutable
    /// (overwrite-aware ring stamping). Defaults to `false` so a client
    /// that predates this field still deserializes as mutable.
    #[serde(default)]
    pub immutable: bool,
    pub key: String,
    pub max_retries: usize,
    pub concurrency: usize,
    pub raw: bool,
    pub collection: bool,
    /// When true, the daemon streams `Response::Progress { done, total }`
    /// frames over the connection as chunks are pushed, before the
    /// terminal `Uploaded`/`Err`. Defaults to false so a client that
    /// predates this field never receives frames it can't deserialize.
    #[serde(default)]
    pub progress: bool,
    pub manifest_path: Option<String>,
    pub content_type: Option<String>,
    pub index_document: Option<String>,
    pub error_document: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub hash: String,
    pub path: Option<String>,
    /// Where to write fetched bytes. `None` is only valid together with
    /// `list: true` (listing writes nothing). Kept optional so a list
    /// request needn't supply a dummy path.
    #[serde(default)]
    pub output: Option<PathBuf>,
    pub max_retries: usize,
    pub concurrency: usize,
    /// When true, enumerate the manifest's entries instead of fetching
    /// bytes; the daemon replies with `Response::Listed`. `#[serde(default)]`
    /// so older clients (which always fetch) deserialize as `false`.
    #[serde(default)]
    pub list: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Pong,
    /// Streamed during an upload: `done`/`total` chunks pushed so far.
    /// Zero or more `Progress` frames precede the terminal `Uploaded`
    /// or `Err` frame on the same connection. Clients that predate this
    /// variant will fail to deserialize it — but only newer clients ask
    /// for progress (see `UploadRequest.progress`), so old clients never
    /// receive one.
    Progress {
        done: usize,
        total: usize,
    },
    Uploaded {
        root: String,
        bytes: usize,
    },
    Fetched {
        bytes_written: usize,
        content_type: Option<String>,
    },
    /// Reply to a `Fetch` request with `list: true`: the manifest's entries.
    Listed {
        entries: Vec<crate::client::ManifestEntry>,
    },
    /// Reply to a `Status` request: current pool + peerlist stats.
    Status {
        /// Configured target pool size (`--pool-size`).
        pool_target: usize,
        /// Total entries currently in the pool (includes tombstoned /
        /// dead-skip entries not yet pruned).
        pool_len: usize,
        /// Entries whose underlying libp2p session is alive right now
        /// (driver task open AND not in the dead-skip window). This is
        /// the real "connected peers" number.
        live_count: usize,
        /// Total peers in the daemon's in-memory peerlist.
        peerlist_total: usize,
        /// Peers with a dialable underlay in the peerlist (dial candidates).
        peerlist_dialable: usize,
        /// Whether the lazy pool has been built yet (false before the
        /// eager fill / first request completes).
        pool_initialized: bool,
    },
    Ok,
    Err {
        message: String,
    },
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
    /// Optional DoH resolver + bootnodes for live peer discovery
    /// before pool fill. Multiple bootnodes are tried in order during
    /// each discover round, accumulating their peer lists, so a peer
    /// that bin-saturates and rejects our handshake substream doesn't
    /// block us — others can still supply hive responses.
    doh: Option<Doh>,
    bootnodes: Vec<libp2p::Multiaddr>,
    /// Number of recursive discovery hops to run during eager
    /// pool fill. Default 1 — enough for warm peers.json. Larger
    /// values seed a much bigger candidate set when peers.json is
    /// cold (CI, fresh VPS). Caller-supplied via the daemon
    /// command's `--discover-rounds` flag.
    discover_rounds: usize,
    /// Daemon identity ETH address. Used by the auto-iteration loop
    /// to compute vanity overlays for proposed anchor sets.
    /// `None` when the daemon runs without `--identity` (purely
    /// outbound, ephemeral overlay).
    identity_eth_address: Option<[u8; 20]>,
    /// Current overlay-nonce file path. Auto-iteration writes the
    /// suggested next-nonce here (with `.next` suffix) so an operator
    /// can opt into it by renaming and restarting the daemon.
    nonce_file_path: Option<PathBuf>,
    /// Persistent retrieval state (per-peer session cache + cross-chunk
    /// peer scoreboard) reused across every fetch request, so warm
    /// sessions and learned forwarder scores carry over between
    /// downloads instead of being rebuilt cold each time.
    retrieval_cache: RetrievalCache,
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
    /// Snapshot served on every inbound `/swarm/status/1.1.3/status`
    /// probe. Required for bee's `salud` to mark us Healthy and
    /// therefore stop preferentially selecting us for kademlia
    /// bin-prune disconnection. See `crate::protocols::status::StatusSnapshot`.
    pub status_snapshot: crate::protocols::status::StatusSnapshot,
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
    swap: Option<crate::transport::SwapConfig>,
    // Optional DoH + one-or-more bootnodes for live peer discovery
    // before pool fill. When set, the daemon does a `discover_rounds`-
    // round discover before opening the session pool, ensuring fresh
    // peers instead of stale file entries. With multiple bootnodes,
    // every round dials all bootnodes in parallel (in addition to the
    // accumulated frontier), so cold-start is robust against one peer
    // rejecting our handshake.
    discover: Option<(Doh, Vec<libp2p::Multiaddr>)>,
    // Path of the overlay-nonce file the daemon was started with.
    // The auto-iteration loop writes the suggested next-nonce to
    // `<path>.next` (next to the current nonce) so an operator
    // can opt into it by mv'ing it over and restarting.
    nonce_file_path: Option<PathBuf>,
    // Number of recursive discovery hops to perform during the
    // pre-pool-fill discover. 1 is enough when peers.json already
    // has thousands of entries; 3-5 helps cold-start runs (CI,
    // fresh VPS) where the daemon's discover is the only source.
    // The discover happens under the daemons stable identity
    // when `--listen` + `--identity` are set, so bees don't reject
    // it via kademlia saturation.
    discover_rounds: usize,
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
        // Decoupled from op_timeout: keep warm-pool connections that carry no
        // substreams between pushes from being self-closed by the swarm after
        // ~op_timeout. Long so hoverfly never ends an otherwise-live
        // connection; bee's RST remains the only closer. See PERFORMANCE.md
        // warm-pool notes.
        idle_timeout: std::time::Duration::from_secs(600),
        dial_timeout,
        network_id,
        advertise: listen.as_ref().and_then(|lc| lc.advertise.clone()),
        max_concurrent_substream_upgrades:
            crate::protocols::stream_pool::DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES,
    };
    // Status snapshot is configured at the daemon level (here) AND
    // separately at the listener level (via ListenConfig). Outbound
    // sessions need it because bee opens salud probes over the
    // outbound connection that the session pool maintains — NOT only
    // over the inbound listener (which never holds bee's "real"
    // connection to us anyway; bee's reacher uses a one-shot ping
    // dialer that closes immediately after).
    let status_snapshot = listen
        .as_ref()
        .map(|lc| lc.status_snapshot.clone())
        .unwrap_or_default();
    let transport = match listen.as_ref() {
        Some(lc) => {
            let kp = crate::inbound::libp2p_keypair_from_identity(&lc.identity);
            let mut t = Transport::new_with_keypair(lc.identity.clone(), cfg, kp);
            if let Some(sc) = swap.clone() {
                t = t.with_swap(sc);
            }
            t = t.with_status_snapshot(status_snapshot.clone());
            Arc::new(t)
        }
        None => {
            let mut t = Transport::new(crate::SwarmSigner::random(network_id), cfg);
            if let Some(sc) = swap.clone() {
                t = t.with_swap(sc);
            }
            t = t.with_status_snapshot(status_snapshot.clone());
            Arc::new(t)
        }
    };
    let peers = PeerStore::load_or_create(&peerlist_path);
    if peers.is_empty() {
        warn!(target: "hoverfly::daemon",
            "peerlist {} is empty — daemon will refuse uploads until populated",
            peerlist_path.display());
    }
    let cache = ChunkCache::new();
    let (doh, bootnodes) = discover
        .map(|(d, b)| (Some(d), b))
        .unwrap_or((None, Vec::new()));
    // Pluck the daemon's identity eth_address out of listen (if any)
    // BEFORE we move `listen` into the inbound spawn. Auto-iteration
    // needs the eth address to compute candidate vanity overlays.
    let identity_eth_address = listen.as_ref().map(|lc| *lc.identity.eth_address());
    let state = Arc::new(State {
        transport,
        signer_network_id: network_id,
        peers: RwLock::new(peers),
        peerlist_path: peerlist_path.clone(),
        pool: RwLock::new(None),
        pool_target,
        cache: cache.clone(),
        doh,
        bootnodes,
        discover_rounds: discover_rounds.max(1),
        identity_eth_address,
        nonce_file_path: nonce_file_path.clone(),
        retrieval_cache: RetrievalCache::new(),
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
            status_snapshot: lc.status_snapshot,
        };
        tokio::spawn(async move {
            if let Err(e) = crate::inbound::run(inbound_cfg).await {
                warn!(target: "hoverfly::daemon", "inbound listener exited: {e}");
            }
        });
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(target: "hoverfly::daemon",
        "listening on {} (peerlist {}, pool_target {})",
        socket_path.display(), peerlist_path.display(), pool_target);

    // Eager pool fill at startup. A bee node maintains its kademlia
    // continuously; our daemon should too. Without this, the first
    // upload pays the 60-300 s pool-fill cost synchronously, while
    // every subsequent upload reuses the warm pool. By dialing in the
    // background at startup, the first upload is fast as well.
    //
    // The same task also runs the background maintenance loop:
    // periodically re-checks `is_alive()` on every pool entry and
    // dispatches prewarm dials for dead ones, keeping the pool
    // at target size against the steady-state churn of bee bin-prune
    // RSTs (see PERFORMANCE.md "Session-death cause"). Without
    // maintenance the pool only shrinks over time; with it, the
    // pool holds steady at `pool_target` for as long as the daemon
    // runs.
    {
        let state = state.clone();
        tokio::spawn(async move {
            // Fire the lazy `ensure_pool` immediately. After it
            // returns, the warm pool is open and subsequent uploads
            // skip the fill.
            if let Err(e) = ensure_pool(&state).await {
                warn!(target: "hoverfly::daemon",
                    "eager pool fill failed: {e} (will retry lazily on first request)");
                return;
            }
            // Maintenance tick. Bee RSTs most random-overlay
            // connections within seconds (see PERFORMANCE.md "Session-
            // death cause"), so the pool bleeds out continuously. The
            // fix is bee's own model (`pkg/topology/kademlia` manage
            // loop): re-dial FAST so we out-pace the churn, rather than
            // slowly and losing ground. A light bee holds ~137 outbound
            // connections this way by refilling its bins every 15 s +
            // on every disconnect.
            //
            // The old 5-min interval was chosen to avoid bee's per-IP
            // rate limiter, but that concern is now handled elsewhere:
            // the per-peer parking limiter (`ratelimit::DialRateLimiter`)
            // paces re-dials to the SAME peer, and maintenance dials
            // FRESH peers (`top_up` excludes overlays already in the
            // pool) which hit DIFFERENT bee nodes — each of which
            // rate-limits our /32 independently, so a burst of N dials
            // to N distinct nodes isn't throttled. `top_up`'s downstream
            // `SESSION_DIAL_PARALLELISM` cap bounds the in-flight dials.
            //
            // Interval is `HOVERFLY_MAINTENANCE_SECS` (default 3 s). Fast +
            // capped-per-tick (see `maintain_pool`) so refills are SPREAD
            // over time: dialing the whole deficit in one burst makes the
            // cohort connect together and get RST together, producing a
            // 137<->0 sawtooth. A steady trickle desynchronises deaths into
            // a stable floor. A 1 s tick over-dials the redial treadmill (bee
            // RSTs non-participants in ~15 s) and steals push CPU from active
            // uploads on small hosts; 3 s keeps the floor without the tax.
            let maint_secs: u64 = std::env::var("HOVERFLY_MAINTENANCE_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|n: &u64| *n > 0)
                .unwrap_or(3);
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(maint_secs));
            tick.tick().await; // skip the immediate-tick semantics
            loop {
                tick.tick().await;
                if let Err(e) = maintain_pool(&state).await {
                    debug!(target: "hoverfly::daemon",
                        "pool maintenance tick failed: {e}");
                }
            }
        });
    }

    // Auto-iteration loop. Periodically inspect the pool's per-peer
    // push-success counters, pick the top-K peers, run a vanity-overlay
    // search against their overlays, and write the suggested next
    // nonce to `<nonce_file>.next` for the operator to opt into via
    // `mv <nonce_file>.next <nonce_file>` + restart.
    //
    // Why next-nonce-as-file rather than apply-at-runtime: the daemon's
    // libp2p identity (and therefore the advertised overlay) is fixed
    // for the lifetime of the listener — bee peers reach us via the
    // overlay they learned through hive, and reseeding mid-run would
    // strand inflight connections + invalidate all SWAP cheques we've
    // already signed against the old overlay.
    if let (Some(_), Some(_)) = (state.identity_eth_address, state.nonce_file_path.as_ref()) {
        let state = state.clone();
        tokio::spawn(async move {
            // First evaluation after ~3 minutes — enough time for the
            // initial pool to fill and for upload activity (if any) to
            // populate per-peer success counts.
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(180));
            tick.tick().await; // skip the immediate-tick semantics
            loop {
                tick.tick().await;
                if let Err(e) = auto_iterate_anchors(&state).await {
                    debug!(target: "hoverfly::daemon",
                        "auto-iterate tick failed: {e}");
                }
            }
        });
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!(target: "hoverfly::daemon", "shutdown requested");
                break;
            }
            accept = listener.accept() => {
                let (stream, _) = accept?;
                let state = state.clone();
                let shutdown_tx = shutdown_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(state, stream, shutdown_tx).await {
                        debug!(target: "hoverfly::daemon", "connection error: {}", e);
                    }
                });
            }
        }
    }

    // Persist the peerlist before exiting so reachability observations
    // collected during the daemon's lifetime aren't lost. Take a write
    // lock and apply the reachability log *in place* before saving — an
    // earlier version applied it to a throwaway `peers.clone()` and then
    // saved the un-updated original, silently dropping every dial result
    // learned during the session. Mirror the `SavePeers` handler.
    let mut peers = state.peers.write().await;
    apply_log(&mut peers, state.transport.reachability_log());
    if let Err(e) = peers.save(&state.peerlist_path) {
        warn!(target: "hoverfly::daemon", "failed to save peerlist on shutdown: {}", e);
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
    // Upload optionally streams `Progress` frames before its terminal
    // frame, so it owns the stream directly and returns once the final
    // frame is written. Every other op produces a single response we
    // write below.
    if let Request::Upload(r) = req {
        return handle_upload_streaming(&state, r, &mut stream).await;
    }
    let response = match req {
        Request::Ping => Response::Pong,
        Request::Upload(_) => unreachable!("handled above"),
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
                Err(e) => Response::Err {
                    message: format!("save: {e}"),
                },
            }
        }
        Request::Status => {
            let (pool_len, live_count, pool_initialized) = {
                match state.pool.read().await.as_ref() {
                    Some(p) => (p.len(), p.live_count(), true),
                    None => (0, 0, false),
                }
            };
            let (peerlist_total, peerlist_dialable) = {
                let peers = state.peers.read().await;
                let dialable = peers
                    .iter()
                    .filter(|p| p.first_dialable_underlay().is_some())
                    .count();
                (peers.len(), dialable)
            };
            Response::Status {
                pool_target: state.pool_target,
                pool_len,
                live_count,
                peerlist_total,
                peerlist_dialable,
                pool_initialized,
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

/// Run an upload and, if `r.progress` is set, stream `Response::Progress`
/// frames to `stream` as chunks are pushed, followed by the terminal
/// `Uploaded`/`Err` frame. Owns the connection so it can interleave
/// progress with the final result on one socket.
async fn handle_upload_streaming(
    state: &Arc<State>,
    r: UploadRequest,
    stream: &mut UnixStream,
) -> std::io::Result<()> {
    // Progress plumbing: the push loop invokes a sync `Fn(done, total)`
    // callback from worker tasks. Bridge it to async socket writes via an
    // unbounded channel — the callback only does a non-blocking `send`, and
    // the drain loop below writes each update as a `Progress` frame. Only
    // wired when the client opted in (`r.progress`), so old clients (which
    // send `progress: false`) never receive frames they can't parse.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(usize, usize)>();
    let progress: Option<crate::client::ProgressFn> = if r.progress {
        let tx = tx.clone();
        Some(Arc::new(move |done: usize, total: usize| {
            let _ = tx.send((done, total));
        }))
    } else {
        None
    };
    // Drop our extra sender so the drain loop's `recv()` returns `None`
    // once the upload future (holding the last clone via the callback)
    // finishes and its callback is dropped.
    drop(tx);

    // The upload future borrows `state`/`r`/`progress`; run it in place and
    // interleave progress drains via `tokio::select!`. `run_upload` yields
    // exactly once with the terminal result.
    let upload = run_upload(state, &r, progress.as_ref());
    tokio::pin!(upload);

    let result: Result<(String, usize), ClientError> = loop {
        tokio::select! {
            // Bias toward draining progress so the bar stays current even
            // when the upload future is also ready.
            biased;
            maybe = rx.recv() => {
                if let Some((done, total)) = maybe {
                    // Best-effort: if the client hung up, stop streaming but
                    // let the upload finish (its receipts are still worth
                    // completing / caching).
                    let _ = write_frame(stream, &Response::Progress { done, total }).await;
                }
                // `None` (all senders dropped) only happens once the upload
                // future has resolved and been dropped, which the other arm
                // catches first; nothing to do here.
            }
            res = &mut upload => break res,
        }
    };

    // Drain any progress updates emitted between the final push and the
    // upload future resolving, so the bar reaches 100% before the terminal
    // frame. Non-blocking.
    while let Ok((done, total)) = rx.try_recv() {
        let _ = write_frame(stream, &Response::Progress { done, total }).await;
    }

    let response = match result {
        Ok((root, bytes)) => Response::Uploaded { root, bytes },
        Err(e) => Response::Err {
            message: e.to_string(),
        },
    };
    write_frame(stream, &response).await
}

/// Core upload logic shared by streaming and (potential) non-streaming
/// callers. Returns `(root_hex, input_bytes)`.
async fn run_upload(
    state: &Arc<State>,
    r: &UploadRequest,
    progress: Option<&crate::client::ProgressFn>,
) -> Result<(String, usize), ClientError> {
    let signer = crate::SwarmSigner::from_hex(&r.key, state.signer_network_id)
        .map_err(|e| ClientError::Stamp(e.to_string()))?;
    let data = std::fs::read(&r.file).map_err(|e| ClientError::File(e.to_string()))?;
    let bytes = data.len();

    // Collections / single-entry manifests still build their own
    // pool via the existing helpers (they handle dedup + multiple
    // pre-stamp passes). Only the raw / single-file path benefits
    // from the persistent pool — that's where most repeat upload
    // throughput goes anyway.
    let is_tar = r
        .file
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("tar"))
        .unwrap_or(false);

    let root = if (r.collection || (is_tar && !r.raw)) && !r.raw {
        // Collections still use the one-shot path: the dedup + multi-
        // stamp logic in upload_collection is complex enough that
        // refactoring it for an external pool is a follow-up.
        let files = read_tar_files(&data).map_err(|e| ClientError::File(e.to_string()))?;
        if files.is_empty() {
            return Err(ClientError::File(
                "tar archive contains no regular files".into(),
            ));
        }
        // Default the website index to `index.html` for tar
        // collections — that's what a static site build expects.
        // An explicit empty string opts out.
        let index_doc = r
            .index_document
            .as_deref()
            .map(|s| if s.is_empty() { None } else { Some(s) })
            .unwrap_or(Some("index.html"));
        let peers = state.peers.read().await;
        upload_collection(
            &state.transport,
            &*peers,
            &signer,
            &r.batch,
            r.depth,
            r.immutable,
            files,
            index_doc,
            r.error_document.as_deref(),
            r.max_retries,
            r.concurrency,
            progress,
        )
        .await?
    } else {
        // Raw and single-file-with-manifest uploads go through the
        // persistent pool. First request lazily fills it; subsequent
        // ones reuse it with zero dial-fill cost.
        let pool = ensure_pool(state).await?;
        let peers = state.peers.read().await;
        if r.raw {
            upload_bytes_with_pool(
                &state.transport,
                &*pool,
                &*peers,
                &signer,
                &r.batch,
                r.depth,
                r.immutable,
                &data,
                r.max_retries,
                Some(&state.cache),
                progress,
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
                &*peers,
                &signer,
                &r.batch,
                r.depth,
                r.immutable,
                &data,
                &path,
                ct.as_deref(),
                r.max_retries,
                Some(&state.cache),
                progress,
            )
            .await?
        }
    };
    Ok((hex::encode(root.as_bytes()), bytes))
}

/// Ensure the daemon's persistent pool exists and has at least one
/// reachable session. Subsequent uploads see a pre-filled pool with no
/// dial-fill cost.
async fn ensure_pool(state: &Arc<State>) -> Result<Arc<SessionPool>, ClientError> {
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

    // Multi-round discover before pool fill, so we dial fresh peers
    // instead of stale file entries. The discover happens under the
    // daemon's stable signer — so when running with `--listen` +
    // `--identity` + `--nonce-file`, our vanity overlay carries
    // through, and bootnodes don't reject us with kademlia
    // saturation at handshake time.
    //
    // The 5-MIB-PER-DISCOVER wait was kept short (5s per peer) for
    // the original 1-round case; longer would needlessly pad the
    // first-upload cost on a warm peers.json. With >1 round the
    // total wall clock is roughly `rounds × ceil(frontier/16) ×
    // 5s` in the worst case, but practically bounded by the
    // QUIET_FOR short-circuit inside `discover_peers`.
    // Auto-skip the pre-fill discover when the saved peerlist is
    // already warm: if it holds enough *fresh known-good* peers (a
    // recent successful dial on record, no newer failure, dialable
    // underlay), the pool can fill straight from disk and the serial
    // discover round would only add latency with no benefit. On a
    // cold/stale peerlist the count falls short and discover runs, so
    // the node still self-heals.
    let skip_discover = {
        let peers = state.peers.read().await;
        let now = crate::peers::now_unix();
        let warm = peers.fresh_known_good_count(now, crate::peers::KNOWN_GOOD_FRESHNESS_SECS);
        if warm >= crate::peers::WARM_PEERLIST_MIN_KNOWN_GOOD {
            info!(target: "hoverfly::daemon",
                "peerlist is warm ({warm} fresh known-good peer(s) ≥ {}); \
                 skipping pre-fill discover and filling pool from peerlist",
                crate::peers::WARM_PEERLIST_MIN_KNOWN_GOOD);
            true
        } else {
            debug!(target: "hoverfly::daemon",
                "peerlist not warm enough ({warm} fresh known-good peer(s) < {}); \
                 running pre-fill discover",
                crate::peers::WARM_PEERLIST_MIN_KNOWN_GOOD);
            false
        }
    };

    if let Some(doh) = state.doh.as_ref().filter(|_| !skip_discover) {
        // Iterate every configured bootnode, accumulating their
        // hive responses. A peer that bin-saturates and rejects our
        // /swarm/handshake/14.0.0/handshake substream contributes
        // zero peers; the next bootnode in the list typically
        // succeeds, so we still warm a candidate set.
        use std::collections::HashSet;
        let mut all_fresh: Vec<crate::peers::Peer> = Vec::new();
        let mut seen_overlays: HashSet<String> = HashSet::new();
        for bootnode in state.bootnodes.iter() {
            match discover_recursive_with_progress(
                &state.transport,
                doh,
                bootnode,
                std::time::Duration::from_secs(5),
                state.discover_rounds,
                16,
                None::<crate::client::DiscoverProgressFn>,
            )
            .await
            {
                Ok(fresh) => {
                    let mut added = 0usize;
                    for p in fresh {
                        let key = p.overlay.to_lowercase();
                        if seen_overlays.insert(key) {
                            all_fresh.push(p);
                            added += 1;
                        }
                    }
                    debug!(target: "hoverfly::daemon",
                        "discover via {} contributed {} new peer(s)",
                        bootnode, added);
                }
                Err(e) => warn!(target: "hoverfly::daemon",
                    "pre-fill discover via {} failed: {e}", bootnode),
            }
        }
        if !all_fresh.is_empty() {
            info!(target: "hoverfly::daemon",
                "discovered {} fresh peer(s) across {} bootnode(s) in {} round(s) before pool fill",
                all_fresh.len(), state.bootnodes.len(), state.discover_rounds);
            let mut peers = state.peers.write().await;
            for p in all_fresh {
                peers.upsert(p);
            }
        } else {
            warn!(target: "hoverfly::daemon",
                "pre-fill discover returned no peers from any of the {} bootnode(s); \
                 falling back to saved peerlist",
                state.bootnodes.len());
        }
    }

    let peers = state.peers.read().await;
    let pool = Arc::new(SessionPool::open(&state.transport, &*peers, state.pool_target).await?);
    info!(target: "hoverfly::daemon",
        "warm pool: {} session(s) open", pool.len());
    *guard = Some(pool.clone());
    Ok(pool)
}

/// Background auto-iteration tick: identify the daemon's current
/// top-K performing peers (by `push_success_count`), run a vanity-overlay
/// search against their overlays, and write the suggested next-nonce
/// to `<nonce_file>.next`. Operator opts in by `mv`ing the file and
/// restarting the daemon.
///
/// The search uses the same multi-anchor algorithm as the
/// `vanity-overlay` subcommand (maximize the minimum PO across the
/// target set). Target PO is chosen adaptively: half of the average
/// PO of the current overlay to the top-K, rounded up — i.e. we aim
/// to be at PO ≥ (current_avg / 2) to every top peer, which is much
/// looser than a single-target deep anchor but spreads us across
/// multiple deep bins.
///
/// The tick is a no-op if:
/// - Pool has no peers with successful pushes yet.
/// - Our current overlay already meets the proposed target PO.
/// - The search budget exhausts without finding an improvement.
async fn auto_iterate_anchors(state: &Arc<State>) -> Result<(), ClientError> {
    use crate::signer::derive_overlay;
    use crate::transport::proximity;

    // Snapshot the pool. If it doesn't exist or has no successful
    // pushes recorded, there's nothing to iterate against.
    let pool = {
        let guard = state.pool.read().await;
        match guard.as_ref() {
            Some(p) => p.clone(),
            None => return Ok(()),
        }
    };
    let top = pool.top_peers_by_success(5);
    if top.is_empty() {
        debug!(target: "hoverfly::daemon",
            "auto-iterate: no peers with successful pushes yet, skipping");
        return Ok(());
    }

    let Some(eth_address) = state.identity_eth_address else {
        return Ok(());
    };
    let Some(nonce_file) = state.nonce_file_path.as_ref() else {
        return Ok(());
    };

    // Parse target overlays.
    let targets: Vec<[u8; 32]> = top
        .iter()
        .filter_map(|(hex_str, _)| {
            let bytes = hex::decode(hex_str).ok()?;
            if bytes.len() != 32 {
                return None;
            }
            let mut a = [0u8; 32];
            a.copy_from_slice(&bytes);
            Some(a)
        })
        .collect();
    if targets.is_empty() {
        return Ok(());
    }

    // Compute the current overlay (the one the daemon is actually
    // running with) so we can compare. We don't have direct access
    // to the daemon's overlay here without the SwarmSigner, but we
    // can read it from the nonce file + eth_address + network_id.
    let current_overlay = match std::fs::read_to_string(nonce_file) {
        Ok(s) => match hex::decode(s.trim()) {
            Ok(bytes) if bytes.len() == 32 => {
                let mut nonce = [0u8; 32];
                nonce.copy_from_slice(&bytes);
                derive_overlay(&eth_address, state.signer_network_id, &nonce)
            }
            _ => return Ok(()),
        },
        Err(_) => return Ok(()),
    };

    let current_pos: Vec<u8> = targets
        .iter()
        .map(|t| proximity(&current_overlay, t))
        .collect();
    let current_min_po = *current_pos.iter().min().unwrap_or(&0);
    let current_avg_po = (current_pos.iter().map(|p| *p as f64).sum::<f64>()
        / current_pos.len() as f64)
        .floor() as u8;

    info!(target: "hoverfly::daemon",
        "auto-iterate: top {} peers by push success — current overlay POs={:?} (min={}, avg={})",
        targets.len(), current_pos, current_min_po, current_avg_po);

    // Aim for min-PO equal to current_avg + 1: meaningful improvement
    // without being unrealistic to find. Bounded by 16 (single-target
    // PO above that takes minutes-hours of search per peer).
    let target_po = (current_avg_po + 1).min(16);
    if target_po <= current_min_po {
        debug!(target: "hoverfly::daemon",
            "auto-iterate: current overlay already meets target PO {} — no search needed",
            target_po);
        return Ok(());
    }

    // Brute-force counter-keyed nonce search, identical to
    // `vanity-overlay` subcommand. Budget: 5 M tries (~5 s on a
    // modern core). At PO=8 across 5 peers this is well within
    // expected search cost; if we can't find one in 5 s, the
    // tick gives up and retries next round (peer set may have
    // shifted).
    const MAX_ATTEMPTS: u64 = 5_000_000;
    let start = std::time::Instant::now();
    let mut best_min_po = current_min_po;
    let mut best_score: i64 = current_pos.iter().map(|p| *p as i64).sum();
    let mut best_nonce: Option<[u8; 32]> = None;
    for attempt in 0..MAX_ATTEMPTS {
        let mut nonce = [0u8; 32];
        nonce[..8].copy_from_slice(&attempt.to_le_bytes());
        nonce[8..16].copy_from_slice(&attempt.to_le_bytes());
        nonce[16..24].copy_from_slice(&attempt.to_le_bytes());
        nonce[24..32].copy_from_slice(&attempt.to_le_bytes());
        let overlay = derive_overlay(&eth_address, state.signer_network_id, &nonce);
        let mut min_po = u8::MAX;
        let mut score: i64 = 0;
        for t in &targets {
            let po = proximity(&overlay, t);
            if po < min_po {
                min_po = po;
            }
            score += po as i64;
        }
        if min_po > best_min_po || (min_po == best_min_po && score > best_score) {
            best_min_po = min_po;
            best_score = score;
            best_nonce = Some(nonce);
            if min_po >= target_po {
                info!(target: "hoverfly::daemon",
                    "auto-iterate: reached target PO {} after {} attempts ({:.1}s)",
                    target_po,
                    attempt + 1,
                    start.elapsed().as_secs_f64());
                break;
            }
        }
    }

    let Some(winning_nonce) = best_nonce else {
        debug!(target: "hoverfly::daemon",
            "auto-iterate: no improvement over current overlay in {} attempts", MAX_ATTEMPTS);
        return Ok(());
    };
    // Only write a suggestion if the new overlay STRICTLY improves
    // the min PO. The intra-search tie-breaker on score (sum of POs)
    // can pick a candidate with same min PO but better high tail
    // (e.g. PO=[0, 2, 0, 22, 7] beats [0, 2, 0, 4, 4] on score
    // but its min is still 0 — anchoring at 22 to one peer doesn't
    // help us with the bin-0 peers, which is where the bottleneck
    // is). Writing such "improvements" would spam the suggestion
    // file with noise an operator can't act on.
    if best_min_po <= current_min_po {
        debug!(target: "hoverfly::daemon",
            "auto-iterate: no min-PO improvement (best={}, current={}) — not writing suggestion",
            best_min_po, current_min_po);
        return Ok(());
    }
    let winning_overlay = derive_overlay(&eth_address, state.signer_network_id, &winning_nonce);
    let pos: Vec<u8> = targets
        .iter()
        .map(|t| proximity(&winning_overlay, t))
        .collect();

    // Write `<nonce_file>.next` for operator opt-in. Keep the
    // current overlay-nonce untouched — the daemon would have to
    // restart to pick up a new identity overlay anyway, and we
    // don't want to silently overwrite the operator's chosen file.
    let mut next_path = nonce_file.clone();
    let mut filename = next_path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    filename.push(".next");
    next_path.set_file_name(filename);
    if let Err(e) = std::fs::write(&next_path, hex::encode(winning_nonce)) {
        warn!(target: "hoverfly::daemon",
            "auto-iterate: failed to write {}: {e}", next_path.display());
        return Ok(());
    }

    info!(target: "hoverfly::daemon",
        "auto-iterate: improvement available — POs={:?} (min={}, vs current min={}). \
         wrote {} — to adopt: mv {} {} && restart",
        pos, best_min_po, current_min_po,
        next_path.display(),
        next_path.display(),
        nonce_file.display());

    Ok(())
}

/// Background maintenance tick: if the pool has dropped below
/// `pool_target` (sessions died from bee bin-prune RSTs, ghost-balance
/// retirement, etc.), dial fresh peers to top up. Mirrors what bee's
/// kademlia does continuously — bee always tries to fill empty bins
/// via `connectNeighbours`. Without this, our pool only shrinks over
/// time and uploads slow down proportionally.
///
/// Errors are non-fatal: a transient failure just means the next tick
/// will retry. The pool is left at whatever size we managed to reach.
async fn maintain_pool(state: &Arc<State>) -> Result<(), ClientError> {
    let pool = {
        let guard = state.pool.read().await;
        match guard.as_ref() {
            Some(p) => p.clone(),
            // No pool yet — eager_pool_fill task will populate one
            // soon, nothing for maintenance to do.
            None => return Ok(()),
        }
    };
    // Step 1: garbage-collect dead entries. Bee RSTs the majority of
    // our random-overlay-PO=0 connections within seconds of handshake
    // (see PERFORMANCE.md "Session-death cause"); without pruning,
    // those tombstones occupy pool slots forever and `top_up` thinks
    // the pool is full.
    let pruned = pool.prune_dead();
    let after_prune = pool.len();
    let target = state.pool_target;
    if pruned == 0 && after_prune >= target {
        debug!(target: "hoverfly::daemon",
            "pool maintenance: {} live, no top-up needed", after_prune);
        return Ok(());
    }
    // Spread the refill: add at most `target/8` fresh sessions per tick.
    // With the fast (1 s) tick this trickles the pool back up to target
    // over a handful of ticks rather than dialing the whole deficit at
    // once — the burst is what synchronises the cohort's lifetimes and
    // produces the sawtooth. `target/8` at 1 s (e.g. 17/s for target 137)
    // comfortably out-paces the observed death rate while keeping in-flight
    // dials bounded (also capped downstream by `SESSION_DIAL_PARALLELISM`).
    let max_per_tick = (target / 8).max(8);
    let refill_target = (after_prune + max_per_tick).min(target);
    debug!(target: "hoverfly::daemon",
        "pool maintenance: pruned {} dead, {} live, topping up to {} (+{}/tick)",
        pruned, after_prune, refill_target, max_per_tick);
    // Step 2: dial fresh peers to refill.
    let peers = state.peers.read().await;
    let added = pool.top_up(&state.transport, &*peers, refill_target).await;
    debug!(target: "hoverfly::daemon",
        "pool maintenance: added {} session(s), pool now {}",
        added, pool.len());
    Ok(())
}

async fn handle_fetch(state: &Arc<State>, r: FetchRequest) -> Response {
    // List mode: enumerate manifest entries, write nothing, reply Listed.
    if r.list {
        let peers = state.peers.read().await;
        return match crate::client::list_manifest_ex(
            &state.transport,
            &*peers,
            &r.hash,
            r.max_retries,
            r.concurrency,
        )
        .await
        {
            Ok(entries) => Response::Listed { entries },
            Err(e) => Response::Err {
                message: e.to_string(),
            },
        };
    }
    let result: Result<(usize, Option<String>), ClientError> = (async {
        let output = r.output.as_ref().ok_or_else(|| {
            ClientError::File("internal: fetch without --list requires an output path".into())
        })?;
        let peers = state.peers.read().await;
        let (bytes, ct) = if let Some(p) = r.path.as_deref() {
            let (b, c) = fetch_manifest_path_cached_ex(
                &state.transport,
                &*peers,
                &r.hash,
                p,
                r.max_retries,
                r.concurrency,
                &state.retrieval_cache,
            )
            .await?;
            (b, c)
        } else {
            let b = fetch_bytes_cached_ex(
                &state.transport,
                &*peers,
                &r.hash,
                r.max_retries,
                r.concurrency,
                &state.retrieval_cache,
            )
            .await?;
            (b, None)
        };
        std::fs::write(output, &bytes).map_err(|e| ClientError::File(e.to_string()))?;
        Ok((bytes.len(), ct))
    })
    .await;
    match result {
        Ok((bytes_written, content_type)) => Response::Fetched {
            bytes_written,
            content_type,
        },
        Err(e) => Response::Err {
            message: e.to_string(),
        },
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
        let content_type = crate::mime::guess_from_path(&path);
        out.push(UploadFile {
            path,
            content_type,
            data,
        });
    }
    Ok(out)
}

// ---- client side ----

/// Connect to a daemon listening on `socket_path` and exchange one
/// `request → response` round-trip. Returns the deserialized response
/// or an IO/protocol error.
pub async fn call(socket_path: &std::path::Path, request: &Request) -> std::io::Result<Response> {
    let mut stream = UnixStream::connect(socket_path).await?;
    write_frame(&mut stream, request).await?;
    let resp = read_frame::<Response>(&mut stream)
        .await?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "daemon hung up"))?;
    Ok(resp)
}

/// Upload-specific client call that consumes the daemon's progress stream.
///
/// Sends the request, then reads frames in a loop: each `Progress { done,
/// total }` invokes `progress` (if any); the first non-progress frame
/// (`Uploaded` / `Err`) is the terminal result and is returned. Use this
/// instead of [`call`] for `Request::Upload` so the client can render a
/// progress bar in its own terminal — the daemon has no terminal the user
/// is watching. When `request` has `progress: false`, the daemon simply
/// never sends `Progress` frames and this behaves exactly like [`call`].
pub async fn call_upload(
    socket_path: &std::path::Path,
    request: &Request,
    progress: Option<&crate::client::ProgressFn>,
) -> std::io::Result<Response> {
    let mut stream = UnixStream::connect(socket_path).await?;
    write_frame(&mut stream, request).await?;
    loop {
        let frame = read_frame::<Response>(&mut stream).await?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "daemon hung up")
        })?;
        match frame {
            Response::Progress { done, total } => {
                if let Some(p) = progress {
                    p(done, total);
                }
            }
            terminal => return Ok(terminal),
        }
    }
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

async fn write_frame<T: Serialize>(stream: &mut UnixStream, value: &T) -> std::io::Result<()> {
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
