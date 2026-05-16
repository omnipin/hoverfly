//! Coordinator process for multi-worker upload.
//!
//! Holds the batch-owner key. Reads the input file, BMT-splits and
//! stamps every chunk upfront (including manifest chunks for non-raw
//! uploads). Spawns N worker subprocesses, each with its own
//! ephemeral overlay key. Distributes chunks to workers via the
//! [`protocol`](super::protocol) wire and tracks per-chunk
//! retries across workers; if a chunk fails on one worker it gets
//! re-queued and dispatched to a different worker on the next pull.
//!
//! Single-machine, multi-overlay design: each worker is an
//! independent `isheika worker` subprocess connected to the
//! coordinator over a Unix socket. Bee sees N independent overlays
//! with N independent ghost-balance counters, scaling per-overlay
//! throughput close to N×.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use nectar_primitives::chunk::ChunkAddress;
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::client::{
    prepare_upload_bytes, prepare_upload_file_with_manifest, ProgressFn, StampedChunk,
};
use crate::multiwork::protocol::{
    read_frame, write_frame, ChunkOutcome, Request, Response, PROTOCOL_VERSION,
};
use crate::signer::SwarmSigner;
use crate::transport::TransportConfig;

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("client: {0}")]
    Client(#[from] crate::client::ClientError),
    #[error("signer: {0}")]
    Signer(#[from] crate::signer::SignerError),
    #[error("worker {id} timed out waiting for socket {sock}")]
    WorkerSocketTimeout { id: usize, sock: PathBuf },
    #[error("worker {id} handshake failed: {msg}")]
    HandshakeFailed { id: usize, msg: String },
    #[error("worker {id} protocol mismatch (v{theirs} vs v{ours})")]
    ProtocolMismatch { id: usize, theirs: u32, ours: u32 },
    #[error("worker {id} disconnected")]
    WorkerDisconnected { id: usize },
    #[error("worker {id} sent unexpected response")]
    UnexpectedResponse { id: usize },
    #[error("{failed_count} chunk(s) exhausted cross-worker retries; first: {sample_addr} ({sample_err})")]
    PermanentFailure {
        failed_count: usize,
        sample_addr: String,
        sample_err: String,
    },
    #[error("no workers spawned successfully")]
    NoWorkers,
}

/// Configuration for one coordinator run, assembled from CLI flags +
/// defaults.
pub struct CoordinatorConfig {
    pub file: PathBuf,
    pub batch_hex: String,
    pub depth: u8,
    /// Hex private key of the batch owner. Used for stamping;
    /// **never** shared with workers.
    pub batch_key_hex: String,
    pub peerlist: PathBuf,
    /// Number of worker subprocesses to spawn.
    pub workers: usize,
    /// Session pool size per worker (each worker's `--concurrency`).
    pub concurrency_per_worker: usize,
    /// Per-chunk peer-candidate cap inside each worker
    /// (each worker's `--max-retries`).
    pub max_retries_per_worker: usize,
    /// How many times a single chunk may be re-routed across workers
    /// before the coordinator gives up and aborts the upload.
    pub max_cross_worker_retries: usize,
    /// Chunks per PushBatch frame. Larger batches = fewer IPC round
    /// trips but coarser load balancing.
    pub batch_size: usize,
    /// If true, skip manifest wrapping (return BMT root of the raw
    /// bytes). If false, wrap in a single-entry mantaray manifest at
    /// `manifest_path` with `content_type` and return the manifest
    /// root.
    pub raw: bool,
    pub manifest_path: Option<String>,
    pub content_type: Option<String>,
    /// Path to the isheika binary used for spawning workers. Default
    /// is the current binary's path (`std::env::current_exe`).
    pub worker_binary: PathBuf,
}

/// Run a multi-worker upload to completion. Returns the manifest root
/// (or BMT root for `--raw` uploads).
///
/// `progress`, if provided, is called as `(pushed_so_far, total)`
/// on every successful or shallow chunk. Same shape as the regular
/// upload's progress callback so the CLI can reuse its indicatif bar.
pub async fn run(
    cfg: CoordinatorConfig,
    transport_cfg: TransportConfig,
    progress: Option<ProgressFn>,
) -> Result<ChunkAddress, CoordinatorError> {
    // 1. Read file, BMT-split, stamp every chunk upfront (data +
    //    manifest in one pass when applicable). Stamping uses the
    //    batch-owner key.
    let data = std::fs::read(&cfg.file)?;
    let signer = SwarmSigner::from_hex(&cfg.batch_key_hex, transport_cfg.network_id)?;
    let (root, work) = if cfg.raw {
        prepare_upload_bytes(&signer, &cfg.batch_hex, cfg.depth, &data)?
    } else {
        let path = cfg.manifest_path.clone().unwrap_or_else(|| {
            cfg.file
                .file_name()
                .and_then(|s| s.to_str())
                .map(str::to_string)
                .unwrap_or_else(|| "file".into())
        });
        prepare_upload_file_with_manifest(
            &signer,
            &cfg.batch_hex,
            cfg.depth,
            &data,
            &path,
            cfg.content_type.as_deref(),
        )?
    };
    let total = work.len();
    info!(target: "isheika::coordinator",
        "stamped {} chunks ({} bytes), root = {}", total, data.len(), root);

    // 2. Spawn worker subprocesses + handshake.
    let tmpdir = TempDir::new()?;
    let workers = spawn_workers(&cfg, transport_cfg.network_id, tmpdir.path()).await?;
    info!(target: "isheika::coordinator",
        "{} worker(s) ready", workers.len());

    // 3. Distribute + retry.
    let failed = distribute(workers, work, &cfg, total, progress).await?;
    if !failed.is_empty() {
        let (sample_addr, sample_err) = failed
            .first()
            .map(|(a, e)| (hex::encode(a), e.clone()))
            .unwrap_or_default();
        return Err(CoordinatorError::PermanentFailure {
            failed_count: failed.len(),
            sample_addr,
            sample_err,
        });
    }

    // tmpdir is dropped here, which removes the socket files. Workers
    // are also dropped (Child::kill_on_drop is set in spawn), so any
    // that didn't gracefully exit get SIGKILL'd as a safety net.
    drop(tmpdir);
    Ok(root)
}

/// One spawned worker, with its child process, socket connection,
/// and post-handshake metadata.
#[allow(dead_code)] // overlay_hex and sessions_open are diagnostic
struct WorkerHandle {
    id: usize,
    /// Subprocess. `kill_on_drop = true`, so dropping cleans up the
    /// worker even if the coordinator panics.
    _child: Child,
    stream: UnixStream,
    overlay_hex: String,
    sessions_open: usize,
}

async fn spawn_workers(
    cfg: &CoordinatorConfig,
    network_id: u64,
    socket_dir: &Path,
) -> Result<Vec<WorkerHandle>, CoordinatorError> {
    let mut spawning = FuturesUnordered::new();
    for id in 0..cfg.workers {
        let socket = socket_dir.join(format!("worker-{id}.sock"));
        let bin = cfg.worker_binary.clone();
        let peerlist = cfg.peerlist.clone();
        let conc = cfg.concurrency_per_worker;
        let retries = cfg.max_retries_per_worker;
        spawning.push(async move {
            spawn_one_worker(id, bin, socket, peerlist, conc, retries, network_id).await
        });
    }
    let mut handles: Vec<WorkerHandle> = Vec::with_capacity(cfg.workers);
    while let Some(result) = spawning.next().await {
        match result {
            Ok(h) => handles.push(h),
            Err(e) => {
                warn!(target: "isheika::coordinator",
                    "worker spawn failed: {e}");
            }
        }
    }
    if handles.is_empty() {
        return Err(CoordinatorError::NoWorkers);
    }
    handles.sort_by_key(|h| h.id);
    Ok(handles)
}

async fn spawn_one_worker(
    id: usize,
    worker_binary: PathBuf,
    socket: PathBuf,
    peerlist: PathBuf,
    concurrency: usize,
    max_retries: usize,
    network_id: u64,
) -> Result<WorkerHandle, CoordinatorError> {
    let mut cmd = Command::new(&worker_binary);
    cmd.arg("--network-id")
        .arg(network_id.to_string())
        .arg("worker")
        .arg("--socket")
        .arg(&socket)
        .arg("--peerlist")
        .arg(&peerlist)
        .arg("--concurrency")
        .arg(concurrency.to_string())
        .arg("--max-retries")
        .arg(max_retries.to_string())
        .kill_on_drop(true);
    debug!(target: "isheika::coordinator",
        "spawning worker {id}: {:?}", cmd.as_std().get_args().collect::<Vec<_>>());
    let child = cmd.spawn()?;

    // Wait for the worker to bind its socket. Pool fill is typically
    // 1-10 s depending on peerlist health, so allow generous timeout.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if socket.exists() {
            break;
        }
        if Instant::now() > deadline {
            return Err(CoordinatorError::WorkerSocketTimeout { id, sock: socket });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut stream = UnixStream::connect(&socket).await?;
    write_frame(&mut stream, &Request::Hello { protocol: PROTOCOL_VERSION }).await?;
    match read_frame::<Response>(&mut stream).await? {
        Some(Response::HelloAck { protocol, worker_overlay_hex, sessions_open }) => {
            if protocol != PROTOCOL_VERSION {
                return Err(CoordinatorError::ProtocolMismatch {
                    id,
                    theirs: protocol,
                    ours: PROTOCOL_VERSION,
                });
            }
            info!(target: "isheika::coordinator",
                "worker {id}: overlay={} sessions={}",
                worker_overlay_hex, sessions_open);
            Ok(WorkerHandle {
                id,
                _child: child,
                stream,
                overlay_hex: worker_overlay_hex,
                sessions_open,
            })
        }
        Some(Response::ProtocolError { message }) => {
            Err(CoordinatorError::HandshakeFailed { id, msg: message })
        }
        Some(_) => Err(CoordinatorError::HandshakeFailed {
            id,
            msg: "unexpected response to Hello".into(),
        }),
        None => Err(CoordinatorError::HandshakeFailed {
            id,
            msg: "worker disconnected before HelloAck".into(),
        }),
    }
}

/// Drive the distribute-and-retry loop until every chunk has reached a
/// terminal outcome (durably pushed or permanently failed). Returns
/// the list of chunks that exhausted their cross-worker retry budget.
async fn distribute(
    workers: Vec<WorkerHandle>,
    work: Vec<StampedChunk>,
    cfg: &CoordinatorConfig,
    total: usize,
    progress: Option<ProgressFn>,
) -> Result<Vec<([u8; 32], String)>, CoordinatorError> {
    // Shared state across worker tasks:
    let queue: Arc<Mutex<VecDeque<StampedChunk>>> = Arc::new(Mutex::new(VecDeque::from(work)));
    let retry_count: Arc<Mutex<HashMap<[u8; 32], usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let failed: Arc<Mutex<Vec<([u8; 32], String)>>> = Arc::new(Mutex::new(Vec::new()));
    let pushed = Arc::new(AtomicUsize::new(0));

    // Periodic progress log. Workers don't share an indicatif bar
    // because each worker task is independent; instead this side-task
    // ticks every few seconds.
    let progress_handle = {
        let pushed = pushed.clone();
        let queue = queue.clone();
        let failed = failed.clone();
        tokio::spawn(async move {
            let mut last_pushed = 0usize;
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.tick().await; // skip the immediate first tick
            loop {
                tick.tick().await;
                let p = pushed.load(Ordering::Relaxed);
                let q = queue.lock().unwrap().len();
                let f = failed.lock().unwrap().len();
                if p != last_pushed || q > 0 {
                    info!(target: "isheika::coordinator",
                        "pushed {}/{} ({} queued, {} failed)", p, total, q, f);
                    last_pushed = p;
                }
                if p + f >= total && q == 0 {
                    return;
                }
            }
        })
    };

    let mut worker_tasks = FuturesUnordered::new();
    for handle in workers {
        let queue = queue.clone();
        let retry_count = retry_count.clone();
        let failed = failed.clone();
        let pushed = pushed.clone();
        let batch_size = cfg.batch_size;
        let max_retries = cfg.max_cross_worker_retries;
        let progress = progress.clone();
        worker_tasks.push(tokio::spawn(async move {
            worker_loop(
                handle,
                queue,
                retry_count,
                failed,
                pushed,
                batch_size,
                max_retries,
                total,
                progress,
            )
            .await
        }));
    }

    let mut first_error: Option<CoordinatorError> = None;
    while let Some(task_result) = worker_tasks.next().await {
        match task_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(target: "isheika::coordinator", "worker errored: {e}");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(join_err) => {
                warn!(target: "isheika::coordinator",
                    "worker task panicked: {join_err}");
            }
        }
    }
    progress_handle.abort();

    if let Some(e) = first_error {
        // If at least one worker errored AND there's still queued work
        // OR no chunks were pushed, this is a real failure.
        let queue_empty = queue.lock().unwrap().is_empty();
        let any_pushed = pushed.load(Ordering::Relaxed) > 0;
        if !queue_empty || !any_pushed {
            return Err(e);
        }
        // Otherwise: surviving workers drained the queue, just log.
        debug!(target: "isheika::coordinator",
            "some workers errored but the queue drained: {e}");
    }

    let failed_vec = std::mem::take(&mut *failed.lock().unwrap());
    info!(target: "isheika::coordinator",
        "distribution complete: {}/{} pushed, {} permanently failed",
        pushed.load(Ordering::Relaxed), total, failed_vec.len());
    Ok(failed_vec)
}

async fn worker_loop(
    mut handle: WorkerHandle,
    queue: Arc<Mutex<VecDeque<StampedChunk>>>,
    retry_count: Arc<Mutex<HashMap<[u8; 32], usize>>>,
    failed: Arc<Mutex<Vec<([u8; 32], String)>>>,
    pushed: Arc<AtomicUsize>,
    batch_size: usize,
    max_cross_worker_retries: usize,
    total: usize,
    progress: Option<ProgressFn>,
) -> Result<(), CoordinatorError> {
    let worker_id = handle.id;
    // Helper: re-queue all chunks in `batch` at the front of the work
    // queue (so the next idle worker picks them up immediately, before
    // any chunks that were popped after this one). Used on worker IO
    // failure where we don't know if any of the batch was processed.
    let requeue_front = |queue: &Arc<Mutex<VecDeque<StampedChunk>>>, batch: Vec<StampedChunk>| {
        let mut q = queue.lock().unwrap();
        // Push back in reverse so the original order is preserved.
        for chunk in batch.into_iter().rev() {
            q.push_front(chunk);
        }
    };
    loop {
        // Pop up to batch_size chunks. Holding the std mutex across
        // `drain` is fine — it's an in-memory operation.
        let batch: Vec<StampedChunk> = {
            let mut q = queue.lock().unwrap();
            let n = batch_size.min(q.len());
            q.drain(..n).collect()
        };
        if batch.is_empty() {
            // No more work to dispatch from this worker. Send
            // Shutdown and exit. Other workers may still be draining.
            debug!(target: "isheika::coordinator",
                "worker {worker_id}: queue empty, sending Shutdown");
            let _ = write_frame(&mut handle.stream, &Request::Shutdown).await;
            // Best-effort read of ShuttingDown ack; ignore failures.
            let _ = read_frame::<Response>(&mut handle.stream).await;
            return Ok(());
        }

        debug!(target: "isheika::coordinator",
            "worker {worker_id}: PushBatch x{}", batch.len());
        // If the worker disconnects mid-conversation, re-queue the
        // entire batch onto the front of the work queue so a
        // surviving worker picks it up immediately. Cross-worker
        // retry counts are NOT incremented here: from the
        // coordinator's POV none of these chunks were attempted (we
        // never saw a BatchDone for them).
        if let Err(e) = write_frame(
            &mut handle.stream,
            &Request::PushBatch { chunks: batch.clone() },
        )
        .await
        {
            warn!(target: "isheika::coordinator",
                "worker {worker_id}: PushBatch write failed ({e}); re-queueing {} chunks",
                batch.len());
            requeue_front(&queue, batch);
            return Err(CoordinatorError::WorkerDisconnected { id: worker_id });
        }
        let resp = match read_frame::<Response>(&mut handle.stream).await {
            Ok(r) => r,
            Err(e) => {
                warn!(target: "isheika::coordinator",
                    "worker {worker_id}: BatchDone read failed ({e}); re-queueing {} chunks",
                    batch.len());
                requeue_front(&queue, batch);
                return Err(CoordinatorError::WorkerDisconnected { id: worker_id });
            }
        };
        let results = match resp {
            Some(Response::BatchDone { results }) => results,
            Some(Response::ProtocolError { message }) => {
                warn!(target: "isheika::coordinator",
                    "worker {worker_id}: ProtocolError ({message}); re-queueing {} chunks",
                    batch.len());
                requeue_front(&queue, batch);
                return Err(CoordinatorError::HandshakeFailed {
                    id: worker_id,
                    msg: message,
                });
            }
            Some(_) => {
                warn!(target: "isheika::coordinator",
                    "worker {worker_id}: unexpected response; re-queueing {} chunks",
                    batch.len());
                requeue_front(&queue, batch);
                return Err(CoordinatorError::UnexpectedResponse { id: worker_id });
            }
            None => {
                warn!(target: "isheika::coordinator",
                    "worker {worker_id}: disconnected; re-queueing {} chunks",
                    batch.len());
                requeue_front(&queue, batch);
                return Err(CoordinatorError::WorkerDisconnected { id: worker_id });
            }
        };

        // Build an index from the batch we sent so we can re-queue
        // failed chunks (the worker only echoes addrs in results).
        let by_addr: HashMap<[u8; 32], StampedChunk> =
            batch.into_iter().map(|c| (c.addr, c)).collect();

        for r in results {
            match r.outcome {
                ChunkOutcome::Ok | ChunkOutcome::Shallow => {
                    let now = pushed.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(p) = progress.as_ref() {
                        p(now, total);
                    }
                }
                ChunkOutcome::Failed { error } => {
                    let attempts = {
                        let mut rc = retry_count.lock().unwrap();
                        let e = rc.entry(r.addr).or_insert(0);
                        *e += 1;
                        *e
                    };
                    if attempts >= max_cross_worker_retries {
                        warn!(target: "isheika::coordinator",
                            "chunk {} permanently failed after {} cross-worker attempts: {}",
                            hex::encode(r.addr), attempts, error);
                        failed.lock().unwrap().push((r.addr, error));
                    } else if let Some(chunk) = by_addr.get(&r.addr).cloned() {
                        debug!(target: "isheika::coordinator",
                            "chunk {} failed on worker {} (attempt {}/{}); re-queueing",
                            hex::encode(r.addr), worker_id, attempts, max_cross_worker_retries);
                        queue.lock().unwrap().push_back(chunk);
                    } else {
                        warn!(target: "isheika::coordinator",
                            "worker {} returned result for unknown addr {}",
                            worker_id, hex::encode(r.addr));
                    }
                }
            }
        }
    }
}
