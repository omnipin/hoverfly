//! `hoverfly pusher` — HTTP chunk-push relay, stages A+B implemented
//! (docs/pusher-design.md §11).
//!
//! Routes:
//!
//! - `POST /v1/push` — the real relay endpoint (stage B): pre-signed
//!   frames in (docs/pusher-design.md §3), streamed NDJSON acks out.
//!   Open mode: a chunk is accepted iff its stamp signature recovers to
//!   the on-chain owner of a **live** batch (owner + `remainingBalance >
//!   0`, both cached — one RPC pair per batch). Keys stay strictly
//!   client-side; the pusher only ever sees pre-signed material.
//! - `GET /v1/status` — health/advertisement JSON. Doubles as the
//!   platform health check on Render/Lambda-style hosts.
//! - `POST /v1/probe?size=N&concurrency=M&max_retries=R` — flag-gated
//!   (`--probe`) self-push experiment endpoint: generates `size` bytes of
//!   random data, stamps it with an env-provided throwaway key/batch
//!   (`HOVERFLY_PROBE_KEY`, `HOVERFLY_PROBE_BATCH`), runs the standard
//!   one-shot push path, and streams NDJSON progress lines followed by a
//!   final metrics report (throughput, `transport::diag` counter deltas,
//!   dial reachability split, per-host dial-failure clustering). This was
//!   the instrument for the shared-cloud-egress-IP gate experiment
//!   (stage A, results in the design doc); it stays as a diagnostics
//!   endpoint.
//! - `POST /v1/tcpcheck?targets=…` — flag-gated raw TCP connect tester
//!   (network-layer vs application-layer throttling discriminator).
//!
//! Probe mode is the one sanctioned exception to "the pusher never
//! signs": it signs with its *own* env key against a dust batch, exists
//! only for self-testing, and is off by default.
//!
//! Still open from the design doc: stage C (weighted rendezvous is
//! client-side; `budget_remaining_gb` accounting here), and the deferred
//! `--push-quota` / `--push-challenge` / `--push-allow` hardening.
//!
//! Deliberately absent: IPC socket, retrieval-over-HTTP, any acceptance
//! of key material over the wire.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited, StreamBody, combinators::BoxBody};
use hyper::body::Frame;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use tracing::{info, warn};

use crate::client::{
    ProgressFn, SessionPool, StampedChunk, push_chunks_with_pool, upload_bytes_ex,
};
use crate::peers::{DialResult, PeerStore, apply_log};
use crate::pushframe;
use crate::signer::SwarmSigner;
use crate::transport::{Transport, TransportConfig, diag};

/// Max frames per POST /v1/push (also bounds decode allocation).
const PUSH_BATCH_MAX: usize = 512;
/// Max /v1/push body: PUSH_BATCH_MAX × max frame + slack.
const PUSH_MAX_BODY: usize = PUSH_BATCH_MAX * pushframe::MAX_FRAME_LEN + 4096;
/// Default warm-pool target for the push path, overridable via
/// `HOVERFLY_PUSH_POOL`. Deliberately modest because the *default*
/// deployment is free cloud (shared egress /32): bee rate-limits inbound
/// dials per /32 (10/s, burst 40 — docs/pusher-design.md §"Stage A
/// results"), so a shared-IP pool starves at ~10–35 live sessions and a
/// bigger target just burns dial churn that trips the limiter harder. On
/// a **dedicated** IP (VPS, Oracle free VM) the pool reaches 76+ and bee
/// itself sustains 137 — raise this to 128–256 there for much higher
/// throughput.
const PUSH_POOL_TARGET_DEFAULT: usize = 32;
/// Clamp for the env override.
const PUSH_POOL_TARGET_MAX: usize = 512;
/// Per-chunk retry budget on the push path.
const PUSH_MAX_RETRIES: usize = 20;

/// Hard cap on probe payload size — a probe is a measurement, not a
/// bulk upload, and free-tier egress is the budget being measured.
const PROBE_MAX_SIZE: usize = 128 * 1024 * 1024;
const PROBE_DEFAULT_SIZE: usize = 10 * 1024 * 1024;
/// Default matches the concurrency the VPS baseline numbers in
/// PERFORMANCE.md were measured at, so probe reports compare 1:1.
const PROBE_DEFAULT_CONCURRENCY: usize = 64;
/// Same default as `hoverfly upload --max-retries`.
const PROBE_DEFAULT_MAX_RETRIES: usize = 10;

pub struct PusherOpts {
    pub listen: SocketAddr,
    pub peerlist: PathBuf,
    pub probe_enabled: bool,
    /// Overlay nonce (same stable-identity story as the CLI's
    /// `--nonce-file`; see `signer::from_bytes_with_nonce`).
    pub nonce: [u8; 32],
    pub network_id: u64,
    /// Gnosis RPC for probe-mode batch depth/owner resolution.
    pub rpc_url: String,
    /// Optional node-identity secp256k1 key (hex), distinct from the
    /// stamp signer — drives the overlay + libp2p peer-id. From
    /// HOVERFLY_PUSHER_IDENTITY. `None` = reuse the stamp key.
    pub node_identity: Option<String>,
    pub transport: TransportConfig,
}

struct State {
    opts: PusherOpts,
    started: Instant,
    /// Serializes network ops (probe + push): concurrent runs would
    /// pollute each other's diag deltas and fight over the session pool.
    probe_lock: Arc<tokio::sync::Mutex<()>>,
    probe_seq: AtomicU64,
    peers_known: AtomicUsize,
    /// `batch_id → (depth, immutable)` from the on-chain read, so
    /// repeated probes cost one RPC total.
    batch_cache: std::sync::Mutex<HashMap<String, (u8, bool)>>,
    /// Push-path state, built once at startup: the node-identity
    /// transport, the peer cache, and a warm session pool reused across
    /// /v1/push requests (filled lazily on first push). `None` transport
    /// means the node key was unresolvable; /v1/push then 503s.
    push: Option<PushState>,
    /// `batch_id(hex) → on-chain owner`, so repeated pushes for one batch
    /// cost a single RPC.
    owner_cache: std::sync::Mutex<HashMap<String, [u8; 20]>>,
}

struct PushState {
    transport: Arc<Transport>,
    peers: Arc<PeerStore>,
    /// Warm pool, filled on first push and reused. `tokio::Mutex` because
    /// fills/pushes await; the pool itself is internally sharded.
    pool: tokio::sync::Mutex<Option<Arc<SessionPool>>>,
    /// Target warm-pool size (from `HOVERFLY_PUSH_POOL`).
    pool_target: usize,
    /// This node's Kademlia overlay (node eth address + nonce). Published
    /// in `/v1/status` so a multi-lane client can route each chunk to the
    /// relay whose overlay is nearest the chunk's destination neighborhood
    /// (proximity rendezvous, docs/pusher-design.md §7).
    overlay: [u8; 32],
}

type RespBody = BoxBody<Bytes, Infallible>;

pub async fn run(opts: PusherOpts) -> Result<(), Box<dyn std::error::Error>> {
    let peers_known = PeerStore::load_or_create(&opts.peerlist).len();
    if peers_known == 0 {
        warn!(
            "peerlist {} is empty — probes will fail until it is seeded",
            opts.peerlist.display()
        );
    }
    // Build the push-path node transport once, under the node identity
    // (HOVERFLY_PUSHER_IDENTITY, else a random ephemeral key — which gives
    // an unstable overlay and thus oversaturation drops, so a stable
    // premined identity is strongly recommended for real deployments).
    let push = build_push_state(&opts);
    if push.is_none() {
        warn!("push node identity unresolvable; /v1/push will 503 (probe/status still work)");
    }

    let listener = tokio::net::TcpListener::bind(opts.listen).await?;
    info!(
        "pusher listening on http://{} (probe {}; push {}; {} known peers from {})",
        opts.listen,
        if opts.probe_enabled { "ON" } else { "off" },
        if push.is_some() { "ON" } else { "off" },
        peers_known,
        opts.peerlist.display(),
    );
    let state = Arc::new(State {
        opts,
        started: Instant::now(),
        probe_lock: Arc::new(tokio::sync::Mutex::new(())),
        probe_seq: AtomicU64::new(0),
        peers_known: AtomicUsize::new(peers_known),
        batch_cache: std::sync::Mutex::new(HashMap::new()),
        push,
        owner_cache: std::sync::Mutex::new(HashMap::new()),
    });

    // Background warm-pool maintenance: fill on startup and keep the pool
    // topped up so /v1/push requests find live sessions ready and never
    // dial-burst inline. Gentle cadence stays under bee's per-/32 rate limit.
    if state.push.is_some() {
        let s = state.clone();
        tokio::spawn(async move { push_maintenance(s).await });
    }

    loop {
        let (stream, _remote) = listener.accept().await?;
        let io = hyper_util::rt::TokioIo::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle(state, req).await) }
            });
            // Streamed probe responses outlive any sane header timeout;
            // hyper's defaults are fine, errors here are just client
            // disconnects.
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        });
    }
}

async fn handle(state: Arc<State>, req: Request<hyper::body::Incoming>) -> Response<RespBody> {
    // Browsers push cross-origin (a dApp on some origin → this relay), with a
    // custom content-type that triggers a CORS preflight. Answer OPTIONS and
    // tag every response with permissive CORS headers — the relay serves no
    // credentialed/secret data, auth is per-frame stamp signatures, so `*` is
    // correct. Without this the browser blocks /v1/push entirely.
    if req.method() == Method::OPTIONS {
        return cors_preflight();
    }
    let mut resp = match (req.method(), req.uri().path()) {
        (&Method::GET, "/v1/status") => status_response(&state),
        (&Method::POST, "/v1/probe") => probe_response(state, req.uri().query()),
        (&Method::POST, "/v1/tcpcheck") => tcpcheck_response(state, req.uri().query()),
        (&Method::POST, "/v1/push") => push_response(state, req).await,
        (_, "/v1/probe") | (_, "/v1/status") | (_, "/v1/tcpcheck") | (_, "/v1/push") => {
            json_line_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        _ => json_line_response(StatusCode::NOT_FOUND, "not found"),
    };
    add_cors(resp.headers_mut());
    resp
}

/// Add permissive CORS headers to a response.
fn add_cors(h: &mut hyper::HeaderMap) {
    use hyper::header::HeaderValue;
    h.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    h.insert(
        "access-control-expose-headers",
        HeaderValue::from_static("*"),
    );
}

/// 204 response for a CORS preflight (`OPTIONS`).
fn cors_preflight() -> Response<RespBody> {
    use hyper::header::HeaderValue;
    let mut resp = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()).boxed())
        .expect("static response parts");
    let h = resp.headers_mut();
    add_cors(h);
    h.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    h.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("content-type"),
    );
    h.insert("access-control-max-age", HeaderValue::from_static("86400"));
    resp
}

fn status_response(state: &State) -> Response<RespBody> {
    let body = serde_json::json!({
        "version": crate::VERSION,
        "profile": "persistent",
        "probe": state.opts.probe_enabled,
        "push": state.push.is_some(),
        "peers_known": state.peers_known.load(Ordering::Relaxed),
        "uptime_secs": state.started.elapsed().as_secs(),
        "batch_max": if state.push.is_some() { serde_json::json!(PUSH_BATCH_MAX) } else { serde_json::Value::Null },
        // The node's Kademlia overlay, so a multi-lane client can route
        // each chunk to the nearest relay (proximity rendezvous, §7).
        "overlay": state.push.as_ref().map(|p| format!("0x{}", hex::encode(p.overlay))),
        // Metered-budget accounting lands with the client scheduler (§7).
        "budget_remaining_gb": serde_json::Value::Null,
    });
    json_response(StatusCode::OK, &body)
}

fn probe_response(state: Arc<State>, query: Option<&str>) -> Response<RespBody> {
    if !state.opts.probe_enabled {
        return json_line_response(StatusCode::NOT_FOUND, "probe endpoint disabled (--probe)");
    }
    let params = parse_query(query);
    let size = match param_usize(&params, "size", PROBE_DEFAULT_SIZE) {
        Ok(v) if (1..=PROBE_MAX_SIZE).contains(&v) => v,
        Ok(v) => {
            return json_line_response(
                StatusCode::BAD_REQUEST,
                &format!("size {v} out of range (1..={PROBE_MAX_SIZE})"),
            );
        }
        Err(e) => return json_line_response(StatusCode::BAD_REQUEST, &e),
    };
    let concurrency = match param_usize(&params, "concurrency", PROBE_DEFAULT_CONCURRENCY) {
        Ok(v) if (1..=1024).contains(&v) => v,
        Ok(v) => {
            return json_line_response(
                StatusCode::BAD_REQUEST,
                &format!("concurrency {v} out of range (1..=1024)"),
            );
        }
        Err(e) => return json_line_response(StatusCode::BAD_REQUEST, &e),
    };
    let max_retries = match param_usize(&params, "max_retries", PROBE_DEFAULT_MAX_RETRIES) {
        Ok(v) if (1..=100).contains(&v) => v,
        Ok(v) => {
            return json_line_response(
                StatusCode::BAD_REQUEST,
                &format!("max_retries {v} out of range (1..=100)"),
            );
        }
        Err(e) => return json_line_response(StatusCode::BAD_REQUEST, &e),
    };

    let Ok(guard) = state.probe_lock.clone().try_lock_owned() else {
        return json_line_response(StatusCode::CONFLICT, "a probe is already running");
    };

    let (tx, rx) = futures::channel::mpsc::unbounded::<Result<Frame<Bytes>, Infallible>>();
    tokio::spawn(async move {
        let _guard = guard;
        run_probe(state, size, concurrency, max_retries, tx).await;
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .header("cache-control", "no-store")
        // Tell buffering reverse proxies (nginx-style) to pass NDJSON
        // lines through as they are flushed.
        .header("x-accel-buffering", "no")
        .body(BoxBody::new(StreamBody::new(rx)))
        .expect("static response parts")
}

/// `POST /v1/tcpcheck?targets=host:port,…&n=20&timeout_ms=3000` — raw
/// TCP connect tester, the discriminator between "our egress path is
/// broken" and "peers throttle this source IP". No libp2p, no
/// handshake: just `TcpStream::connect` × `n` per target with error-kind
/// classification (refused = RST reached us, so packets flow; timeout =
/// dropped somewhere; unreachable = routing/NAT). Targets run in
/// parallel, attempts per target sequentially with a small gap so one
/// target never sees a SYN flood. One NDJSON line per target as it
/// finishes. Gated behind `--probe` like the push probe.
fn tcpcheck_response(state: Arc<State>, query: Option<&str>) -> Response<RespBody> {
    if !state.opts.probe_enabled {
        return json_line_response(StatusCode::NOT_FOUND, "probe endpoint disabled (--probe)");
    }
    let params = parse_query(query);
    let targets: Vec<String> = params
        .get("targets")
        .map(|t| {
            t.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    if targets.is_empty() || targets.len() > 16 {
        return json_line_response(StatusCode::BAD_REQUEST, "need 1..=16 targets=host:port,…");
    }
    let n = match param_usize(&params, "n", 20) {
        Ok(v) if (1..=100).contains(&v) => v,
        Ok(v) => {
            return json_line_response(
                StatusCode::BAD_REQUEST,
                &format!("n {v} out of range (1..=100)"),
            );
        }
        Err(e) => return json_line_response(StatusCode::BAD_REQUEST, &e),
    };
    let timeout_ms = match param_usize(&params, "timeout_ms", 3000) {
        Ok(v) if (100..=10_000).contains(&v) => v as u64,
        Ok(v) => {
            return json_line_response(
                StatusCode::BAD_REQUEST,
                &format!("timeout_ms {v} out of range (100..=10000)"),
            );
        }
        Err(e) => return json_line_response(StatusCode::BAD_REQUEST, &e),
    };

    let (tx, rx) = futures::channel::mpsc::unbounded::<Result<Frame<Bytes>, Infallible>>();
    tokio::spawn(async move {
        let mut handles = Vec::with_capacity(targets.len());
        for target in targets {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let line = tcpcheck_target(&target, n, timeout_ms).await;
                let mut s = serde_json::json!({"tcpcheck": line}).to_string();
                s.push('\n');
                let _ = tx.unbounded_send(Ok(Frame::data(Bytes::from(s))));
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        let _ = tx.unbounded_send(Ok(Frame::data(Bytes::from(
            serde_json::json!({"done": true}).to_string() + "\n",
        ))));
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .header("cache-control", "no-store")
        .header("x-accel-buffering", "no")
        .body(BoxBody::new(StreamBody::new(rx)))
        .expect("static response parts")
}

async fn tcpcheck_target(target: &str, n: usize, timeout_ms: u64) -> serde_json::Value {
    use std::io::ErrorKind;
    let mut ok = 0usize;
    let mut connect_ms: Vec<u64> = Vec::new();
    let mut errors: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut sample_error: Option<String> = None;
    for i in 0..n {
        if i > 0 {
            // Pace attempts so a single target never sees a SYN burst —
            // we are measuring policy, not provoking it.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let started = Instant::now();
        match tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            tokio::net::TcpStream::connect(target),
        )
        .await
        {
            Ok(Ok(_stream)) => {
                ok += 1;
                connect_ms.push(started.elapsed().as_millis() as u64);
            }
            Ok(Err(e)) => {
                let class = match e.kind() {
                    ErrorKind::ConnectionRefused => "refused",
                    ErrorKind::ConnectionReset => "reset",
                    ErrorKind::TimedOut => "timeout",
                    ErrorKind::HostUnreachable | ErrorKind::NetworkUnreachable => "unreachable",
                    _ => "other",
                };
                *errors.entry(class).or_insert(0) += 1;
                sample_error.get_or_insert_with(|| e.to_string());
            }
            Err(_) => {
                *errors.entry("timeout").or_insert(0) += 1;
            }
        }
    }
    connect_ms.sort_unstable();
    let med = connect_ms.get(connect_ms.len() / 2).copied();
    let mut v = serde_json::json!({
        "target": target,
        "n": n,
        "ok": ok,
        "connect_ms": {
            "min": connect_ms.first().copied(),
            "median": med,
            "max": connect_ms.last().copied(),
        },
        "errors": errors,
    });
    if let Some(s) = sample_error {
        v["sample_error"] = serde_json::Value::String(s);
    }
    v
}

/// Build the push-path transport + peer cache from the node identity.
/// Returns `None` if the node key can't be resolved.
fn build_push_state(opts: &PusherOpts) -> Option<PushState> {
    let nonce_hex = format!("0x{}", hex::encode(opts.nonce));
    let node_signer = match opts.node_identity.as_deref() {
        Some(k) => SwarmSigner::from_hex_with_nonce(k, &nonce_hex, opts.network_id).ok()?,
        None => {
            let mut kb = [0u8; 32];
            getrandom::fill(&mut kb).ok()?;
            SwarmSigner::from_hex_with_nonce(
                &format!("0x{}", hex::encode(kb)),
                &nonce_hex,
                opts.network_id,
            )
            .ok()?
        }
    };
    let keypair = crate::inbound::libp2p_keypair_from_identity(&node_signer);
    let overlay = *node_signer.overlay();
    let snapshot = crate::protocols::status::StatusSnapshot::default();
    let transport = Transport::new_with_keypair(node_signer, opts.transport.clone(), keypair)
        .with_status_snapshot(snapshot);
    info!("push node overlay = 0x{}", hex::encode(overlay));
    let peers = PeerStore::load_or_create(&opts.peerlist);
    let pool_target = std::env::var("HOVERFLY_PUSH_POOL")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.clamp(1, PUSH_POOL_TARGET_MAX))
        .unwrap_or(PUSH_POOL_TARGET_DEFAULT);
    info!("push warm-pool target = {pool_target} (HOVERFLY_PUSH_POOL to override)");
    Some(PushState {
        transport: Arc::new(transport),
        peers: Arc::new(peers),
        pool: tokio::sync::Mutex::new(None),
        pool_target,
        overlay,
    })
}

/// `POST /v1/push` — the real relay endpoint. Body = frames
/// (`docs/pusher-design.md` §3); response = streamed NDJSON acks. Open
/// mode: a chunk is accepted iff its stamp signature recovers to the
/// on-chain owner of the stamp's batch AND the batch is alive
/// (`remainingBalance > 0`) — that pair *is* the auth (§5). No keys ever
/// cross the wire — the client stamps locally and ships only pre-signed
/// frames.
async fn push_response(
    state: Arc<State>,
    req: Request<hyper::body::Incoming>,
) -> Response<RespBody> {
    if state.push.is_none() {
        return json_line_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "push disabled (no node identity resolvable)",
        );
    }
    // Bounded body read — a whole batch, not a stream.
    let bytes = match Limited::new(req.into_body(), PUSH_MAX_BODY).collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => {
            return json_line_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body exceeds limit or read error",
            );
        }
    };
    let chunks = match pushframe::decode_batch(&bytes, PUSH_BATCH_MAX) {
        Ok(c) => c,
        Err(e) => {
            return json_line_response(StatusCode::BAD_REQUEST, &format!("frame decode: {e}"));
        }
    };
    if chunks.is_empty() {
        return json_line_response(StatusCode::BAD_REQUEST, "empty batch");
    }

    // Pushes run CONCURRENTLY over the shared warm pool — clients pipeline
    // several batches at once, so serializing them (the old 409-on-contention
    // behavior) forced needless failover churn. The pool is Arc/RwLock-shared
    // and kept filled by the background maintenance loop; each push only reads
    // sessions from it (maintain=false), so no per-push dial burst.
    let (tx, rx) = futures::channel::mpsc::unbounded::<Result<Frame<Bytes>, Infallible>>();
    tokio::spawn(async move {
        run_push(state, chunks, tx).await;
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .header("cache-control", "no-store")
        .header("x-accel-buffering", "no")
        .body(BoxBody::new(StreamBody::new(rx)))
        .expect("static response parts")
}

async fn run_push(
    state: Arc<State>,
    chunks: Vec<StampedChunk>,
    tx: futures::channel::mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>,
) {
    let send_line = |v: &serde_json::Value| {
        let mut s = v.to_string();
        s.push('\n');
        let _ = tx.unbounded_send(Ok(Frame::data(Bytes::from(s))));
    };
    let ack = |addr: &[u8; 32], status: &str, err: Option<&str>| {
        let mut v = serde_json::json!({"a": hex::encode(addr), "s": status});
        if let Some(e) = err {
            v["e"] = serde_json::Value::String(e.to_string());
        }
        send_line(&v);
    };

    // The stamp's batch_id must match the on-chain owner the signature
    // recovers to. All chunks in one upload share a batch; verify the
    // batch once, then check each chunk's recovered signer against it.
    let mut accepted: Vec<StampedChunk> = Vec::with_capacity(chunks.len());
    let mut batch_owner: Option<[u8; 20]> = None;
    let mut batch_hex: Option<String> = None;

    for chunk in chunks {
        let vs = match crate::stamp::validate(&chunk.addr, &chunk.stamp) {
            Ok(v) => v,
            Err(e) => {
                ack(&chunk.addr, "err", Some(&format!("bad stamp: {e}")));
                continue;
            }
        };
        let bid = hex::encode(vs.batch_id);
        // Resolve the owner for this batch (cached, one RPC per batch).
        let owner = if batch_hex.as_deref() == Some(bid.as_str()) {
            batch_owner
        } else {
            match resolve_owner(&state, &bid).await {
                Ok(o) => {
                    batch_hex = Some(bid.clone());
                    batch_owner = Some(o);
                    Some(o)
                }
                Err(e) => {
                    ack(&chunk.addr, "err", Some(&e));
                    continue;
                }
            }
        };
        match owner {
            Some(o) if o == vs.signer => accepted.push(chunk),
            Some(o) => ack(
                &chunk.addr,
                "err",
                Some(&format!(
                    "stamp signer 0x{} is not the on-chain batch owner 0x{}",
                    hex::encode(vs.signer),
                    hex::encode(o)
                )),
            ),
            None => ack(&chunk.addr, "err", Some("batch owner unresolved")),
        }
    }

    if accepted.is_empty() {
        send_line(&serde_json::json!({"done": {"pushed": 0, "rejected": true}}));
        return;
    }

    // Grab the warm pool (kept filled by the maintenance loop) and push.
    let push = state.push.as_ref().expect("push state present");
    let pool = match get_pool(push).await {
        Ok(p) => p,
        Err(e) => {
            for c in &accepted {
                ack(&c.addr, "err", Some(&format!("pool: {e}")));
            }
            return;
        }
    };

    let addrs: Vec<[u8; 32]> = accepted.iter().map(|c| c.addr).collect();
    let total = accepted.len();
    let result = push_chunks_with_pool(
        &push.transport,
        &pool,
        &push.peers,
        accepted,
        PUSH_MAX_RETRIES,
        false, // the background maintenance loop owns pool upkeep — no per-push
        // top-up (concurrent pushes would otherwise each dial-burst and trip
        // bee's per-/32 rate limiter).
        None,
    )
    .await;

    match result {
        Ok(()) => {
            for a in &addrs {
                ack(a, "ok", None);
            }
            send_line(&serde_json::json!({"done": {"pushed": total}}));
        }
        Err(e) => {
            // All-or-nothing: on failure the client re-POSTs the batch.
            let msg = e.to_string();
            for a in &addrs {
                ack(a, "err", Some(&msg));
            }
            send_line(&serde_json::json!({"done": {"pushed": 0, "error": msg}}));
        }
    }
}

/// On-chain batch owner for `batch_id_hex`, cached. Errors (string) on
/// RPC failure, unknown batch, or an **expired** batch: open-mode auth
/// is "the batch is alive" (docs/pusher-design.md §5), so a batch whose
/// `remainingBalance` has drained to zero is rejected — bee nodes would
/// refuse its stamps anyway, and pushing them just burns relay egress.
/// The aliveness read happens once per batch (the cache never expires);
/// a batch that dies *while cached* only wastes its own push attempts —
/// bees reject the stamps downstream — and a pusher restart re-checks.
async fn resolve_owner(state: &State, batch_id_hex: &str) -> Result<[u8; 20], String> {
    if let Some(o) = state
        .owner_cache
        .lock()
        .expect("owner cache poisoned")
        .get(batch_id_hex)
    {
        return Ok(*o);
    }
    let stamp_addr: alloy_primitives::Address = crate::batch::MAINNET_POSTAGE_STAMP
        .parse()
        .expect("hardcoded valid");
    let info = crate::batch::read_batch(&state.opts.rpc_url, stamp_addr, batch_id_hex)
        .await
        .map_err(|e| format!("batch owner RPC: {e}"))?;
    if info.not_found {
        return Err(format!("batch {batch_id_hex} not found on-chain"));
    }
    let remaining =
        crate::batch::read_remaining_balance(&state.opts.rpc_url, stamp_addr, batch_id_hex)
            .await
            .map_err(|e| format!("batch balance RPC: {e}"))?;
    if remaining.is_zero() {
        return Err(format!(
            "batch {batch_id_hex} has expired (zero remaining balance) — bees would reject every stamp"
        ));
    }
    let owner = info.owner.into_array();
    state
        .owner_cache
        .lock()
        .expect("owner cache poisoned")
        .insert(batch_id_hex.to_string(), owner);
    Ok(owner)
}

/// Return the warm pool, filling/topping it up to `push.pool_target`.
async fn ensure_pool(push: &PushState) -> Result<Arc<SessionPool>, String> {
    // Get-or-install the pool handle under a brief lock, then top up WITHOUT
    // holding it — dialing takes seconds and must not block concurrent pushes
    // reading the pool.
    let pool = {
        let mut guard = push.pool.lock().await;
        match guard.as_ref() {
            Some(p) => p.clone(),
            None => {
                let p = Arc::new(SessionPool::new());
                *guard = Some(p.clone());
                p
            }
        }
    };
    pool.top_up(&push.transport, &push.peers, push.pool_target)
        .await;
    if pool.len() == 0 {
        return Err("could not open any sessions from the peer cache".into());
    }
    Ok(pool)
}

/// The warm pool for a push: the background loop keeps it filled, so this is
/// normally a lock-free read. Only on a cold first request (before the
/// maintenance loop has filled it) does it fall back to building/dialing.
async fn get_pool(push: &PushState) -> Result<Arc<SessionPool>, String> {
    {
        let guard = push.pool.lock().await;
        if let Some(p) = guard.as_ref() {
            if p.len() > 0 {
                return Ok(p.clone());
            }
        }
    }
    ensure_pool(push).await
}

/// Background loop: fill the warm pool on startup and keep it topped up to
/// target on a gentle cadence, so /v1/push never dials inline.
async fn push_maintenance(state: Arc<State>) {
    let Some(push) = state.push.as_ref() else {
        return;
    };
    loop {
        match ensure_pool(push).await {
            Ok(p) => info!(target: "hoverfly::pusher", "warm pool: {} session(s)", p.len()),
            Err(e) => warn!(target: "hoverfly::pusher", "warm pool maintenance: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// The probe itself. Every early exit sends a terminal `report` line
/// with `ok:false` — an errored probe still carries measurement data,
/// which is the whole point of the gate experiment.
async fn run_probe(
    state: Arc<State>,
    size: usize,
    concurrency: usize,
    max_retries: usize,
    tx: futures::channel::mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>,
) {
    let send_line = |v: &serde_json::Value| {
        let mut s = v.to_string();
        s.push('\n');
        // A closed channel means the client hung up; the push keeps
        // running to completion so the probe still lands in the log.
        let _ = tx.unbounded_send(Ok(Frame::data(Bytes::from(s))));
    };

    let key = match std::env::var("HOVERFLY_PROBE_KEY") {
        Ok(k) if !k.trim().is_empty() => k,
        _ => {
            send_line(&serde_json::json!({
                "report": {"ok": false, "error": "HOVERFLY_PROBE_KEY not set in the pusher's environment"}
            }));
            return;
        }
    };
    let batch = match std::env::var("HOVERFLY_PROBE_BATCH") {
        Ok(b) if !b.trim().is_empty() => b,
        _ => {
            send_line(&serde_json::json!({
                "report": {"ok": false, "error": "HOVERFLY_PROBE_BATCH not set in the pusher's environment"}
            }));
            return;
        }
    };

    let signer = match SwarmSigner::from_hex_with_nonce(
        &key,
        &format!("0x{}", hex::encode(state.opts.nonce)),
        state.opts.network_id,
    ) {
        Ok(s) => s,
        Err(e) => {
            send_line(&serde_json::json!({
                "report": {"ok": false, "error": format!("HOVERFLY_PROBE_KEY: {e}")}
            }));
            return;
        }
    };

    // Depth + mutability: env override, else the cached on-chain read
    // (which also owner-checks the env key — the classic misconfig that
    // otherwise burns the whole probe on "could not push chunk").
    let (depth, immutable) = match resolve_batch(&state, &signer, &batch).await {
        Ok(v) => v,
        Err(e) => {
            send_line(&serde_json::json!({"report": {"ok": false, "error": e}}));
            return;
        }
    };

    let mut peers = PeerStore::load_or_create(&state.opts.peerlist);
    if peers.is_empty() {
        send_line(&serde_json::json!({
            "report": {"ok": false, "error": format!("peerlist {} is empty", state.opts.peerlist.display())}
        }));
        return;
    }

    let seq = state.probe_seq.fetch_add(1, Ordering::Relaxed);
    let data = random_data(size, seq);
    send_line(&serde_json::json!({
        "probe": {
            "seq": seq, "size": size, "concurrency": concurrency,
            "max_retries": max_retries, "depth": depth, "immutable": immutable,
            "peers_known": peers.len(),
        }
    }));

    // Node identity is separate from the stamp signer. The stamp key
    // (`signer`) only signs postage; the *network* identity — overlay +
    // libp2p peer-id — comes from HOVERFLY_PUSHER_IDENTITY when set, so
    // multiple pushers sharing one batch owner key still present as
    // distinct bee citizens (required to run them concurrently without a
    // peer-id collision). Falls back to the stamp key when unset. This is
    // the coordinator-stamps / workers-push split from
    // `prepare_upload_bytes`'s docs.
    let node_signer = match state.opts.node_identity.as_deref() {
        Some(nk) => match SwarmSigner::from_hex_with_nonce(
            nk,
            &format!("0x{}", hex::encode(state.opts.nonce)),
            state.opts.network_id,
        ) {
            Ok(s) => s,
            Err(e) => {
                send_line(&serde_json::json!({
                    "report": {"ok": false, "error": format!("HOVERFLY_PUSHER_IDENTITY: {e}")}
                }));
                return;
            }
        },
        None => signer.clone(),
    };

    let snapshot = crate::protocols::status::StatusSnapshot::default();
    // Stable, premined libp2p identity derived deterministically from the
    // node key — not a fresh random keypair per boot. A stable peer-id lets
    // bees recognize reconnections as one peer instead of a flood of
    // strangers; the overlay (node eth address + nonce) governs bin
    // placement / oversaturation.
    let keypair = crate::inbound::libp2p_keypair_from_identity(&node_signer);
    let transport = Transport::new_with_keypair(node_signer, state.opts.transport.clone(), keypair)
        .with_status_snapshot(snapshot);

    let before = diag_snapshot();
    let started = Instant::now();

    // Throttled progress stream: at most ~1 line/s keeps the response
    // flowing (and proxies un-idle) without drowning small probes.
    let progress_tx = tx.clone();
    let progress_started = started;
    let last_sent = std::sync::Mutex::new(Instant::now() - std::time::Duration::from_secs(2));
    let progress: ProgressFn = Arc::new(move |done, total| {
        let mut last = last_sent.lock().expect("progress throttle poisoned");
        if last.elapsed() < std::time::Duration::from_secs(1) && done != total {
            return;
        }
        *last = Instant::now();
        let mut s = serde_json::json!({
            "progress": {
                "done": done, "total": total,
                "elapsed_ms": progress_started.elapsed().as_millis() as u64,
            }
        })
        .to_string();
        s.push('\n');
        let _ = progress_tx.unbounded_send(Ok(Frame::data(Bytes::from(s))));
    });

    let result = upload_bytes_ex(
        &transport,
        &peers,
        &signer,
        &batch,
        depth,
        immutable,
        &data,
        max_retries,
        concurrency,
        Some(&progress),
    )
    .await;

    let elapsed = started.elapsed();
    let after = diag_snapshot();
    let diag_delta: BTreeMap<&'static str, u64> = after
        .iter()
        .filter_map(|(k, v)| {
            let d = v - before.get(k).copied().unwrap_or(0);
            (d > 0).then_some((*k, d))
        })
        .collect();

    // Dial reachability: overall split plus per-host failure clustering —
    // the per-/32 signature is the primary read-out of the cloud-egress
    // gate experiment (a farm refusing cloud IPs shows up as its hosts
    // dominating this map while the VPS baseline dials them fine).
    let log = transport.reachability_log();
    let (dial_ok, dial_fail, failed_hosts) = {
        let by_overlay: HashMap<String, &crate::peers::Peer> = peers
            .iter()
            .map(|p| (p.overlay.to_lowercase(), p))
            .collect();
        let entries = log.lock().expect("reachability log poisoned");
        let mut ok = 0u64;
        let mut fail = 0u64;
        let mut hosts: BTreeMap<String, u64> = BTreeMap::new();
        for (overlay, res) in entries.iter() {
            match res {
                DialResult::Success { .. } => ok += 1,
                DialResult::Failure => {
                    fail += 1;
                    let host = by_overlay
                        .get(overlay.as_str())
                        .and_then(|p| p.underlays.first())
                        .and_then(|u| multiaddr_host(u))
                        .unwrap_or_else(|| "unknown".into());
                    *hosts.entry(host).or_insert(0) += 1;
                }
            }
        }
        (ok, fail, hosts)
    };

    // Feed the observations back into the peerlist (same citizenship as
    // the one-shot CLI) so consecutive probes start from a warmer cache.
    apply_log(&mut peers, &log);
    if let Err(e) = peers.save(&state.opts.peerlist) {
        warn!("could not save peerlist: {e}");
    }
    state.peers_known.store(peers.len(), Ordering::Relaxed);

    let mib_s = (size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(1e-9);
    let mut report = serde_json::json!({
        "ok": result.is_ok(),
        "seq": seq,
        "size": size,
        "elapsed_ms": elapsed.as_millis() as u64,
        "mib_per_sec": (mib_s * 1000.0).round() / 1000.0,
        "dials": {"ok": dial_ok, "failed": dial_fail, "failed_hosts": failed_hosts},
        "diag": diag_delta,
    });
    match result {
        Ok(root) => {
            report["root"] = serde_json::Value::String(hex::encode(root.as_bytes()));
        }
        Err(e) => {
            report["error"] = serde_json::Value::String(e.to_string());
        }
    }
    send_line(&serde_json::json!({"report": report}));
}

/// Depth/immutability for the probe batch: `HOVERFLY_PROBE_DEPTH` (with
/// optional `HOVERFLY_PROBE_IMMUTABLE=1`) skips the chain entirely,
/// otherwise one cached on-chain read that also owner-checks the key.
async fn resolve_batch(
    state: &State,
    signer: &SwarmSigner,
    batch: &str,
) -> Result<(u8, bool), String> {
    if let Ok(d) = std::env::var("HOVERFLY_PROBE_DEPTH") {
        let depth: u8 = d
            .trim()
            .parse()
            .map_err(|e| format!("HOVERFLY_PROBE_DEPTH: {e}"))?;
        let immutable = std::env::var("HOVERFLY_PROBE_IMMUTABLE").is_ok_and(|v| v == "1");
        return Ok((depth, immutable));
    }
    if let Some(hit) = state
        .batch_cache
        .lock()
        .expect("batch cache poisoned")
        .get(batch)
    {
        return Ok(*hit);
    }
    let stamp_addr: alloy_primitives::Address = crate::batch::MAINNET_POSTAGE_STAMP
        .parse()
        .expect("hardcoded valid");
    let info = crate::batch::read_batch(&state.opts.rpc_url, stamp_addr, batch)
        .await
        .map_err(|e| {
            format!("could not read batch on-chain (set HOVERFLY_PROBE_DEPTH to skip): {e}")
        })?;
    if info.not_found {
        return Err(format!("batch {batch} not found on-chain"));
    }
    let signer_addr = alloy_primitives::Address::from(*signer.eth_address());
    if signer_addr != info.owner {
        return Err(format!(
            "batch owner mismatch: on-chain owner {} vs HOVERFLY_PROBE_KEY address {} — \
             bee would reject every stamp",
            info.owner, signer_addr
        ));
    }
    state
        .batch_cache
        .lock()
        .expect("batch cache poisoned")
        .insert(batch.to_string(), (info.depth, info.immutable));
    Ok((info.depth, info.immutable))
}

/// Deterministic-per-seed pseudo-random payload (xorshift64). Seeded
/// with wall-clock + probe sequence so consecutive probes never re-push
/// identical chunk addresses (which bees would dedupe, and which would
/// double-spend stamp bucket slots on immutable batches).
fn random_data(size: usize, seq: u64) -> Vec<u8> {
    let mut x: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x243F_6A88_85A3_08D3)
        ^ (seq.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut data = vec![0u8; size];
    for chunk in data.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = (x >> (8 * i)) as u8;
        }
    }
    data
}

/// Snapshot every `transport::diag` counter relevant to a push run.
fn diag_snapshot() -> BTreeMap<&'static str, u64> {
    let m: &[(&'static str, &AtomicU64)] = &[
        ("push_ok", &diag::PUSH_OUTCOME_OK),
        ("push_shallow", &diag::PUSH_OUTCOME_SHALLOW),
        ("push_overdraft", &diag::PUSH_OUTCOME_OVERDRAFT),
        ("push_error", &diag::PUSH_OUTCOME_ERROR),
        ("push_lat_lt_100ms", &diag::PUSH_LATENCY_LT_100MS),
        ("push_lat_100_500ms", &diag::PUSH_LATENCY_100_500MS),
        ("push_lat_500ms_2s", &diag::PUSH_LATENCY_500MS_2S),
        ("push_lat_2_5s", &diag::PUSH_LATENCY_2_5S),
        ("push_lat_5_10s", &diag::PUSH_LATENCY_5_10S),
        ("push_lat_gt_10s", &diag::PUSH_LATENCY_GT_10S),
        ("chunk_lat_lt_500ms", &diag::CHUNK_LATENCY_LT_500MS),
        ("chunk_lat_500ms_2s", &diag::CHUNK_LATENCY_500MS_2S),
        ("chunk_lat_2_5s", &diag::CHUNK_LATENCY_2_5S),
        ("chunk_lat_5_15s", &diag::CHUNK_LATENCY_5_15S),
        ("chunk_lat_gt_15s", &diag::CHUNK_LATENCY_GT_15S),
        ("open_stream_lt_10ms", &diag::OPEN_STREAM_LT_10MS),
        ("open_stream_10_100ms", &diag::OPEN_STREAM_10_100MS),
        ("open_stream_100_500ms", &diag::OPEN_STREAM_100_500MS),
        ("open_stream_gt_500ms", &diag::OPEN_STREAM_GT_500MS),
        ("conn_closed_io", &diag::CONN_CLOSED_IO),
        ("conn_closed_keepalive", &diag::CONN_CLOSED_KEEPALIVE),
        ("conn_closed_clean", &diag::CONN_CLOSED_CLEAN),
        ("retire_dead_low_ghost", &diag::DEAD_RETIRE_LOW_GHOST),
        (
            "retire_dead_prewarm_ghost",
            &diag::DEAD_RETIRE_PREWARM_GHOST,
        ),
        ("retire_dead_high_ghost", &diag::DEAD_RETIRE_HIGH_GHOST),
        ("retire_ghost", &diag::GHOST_RETIRE),
        ("retire_max_pushes", &diag::MAX_PUSHES_RETIRE),
        ("prewarm_on_dead", &diag::PREWARM_ON_DEAD),
        ("prewarm_on_ghost", &diag::PREWARM_ON_GHOST),
        ("hive_announce_ok", &diag::HIVE_ANNOUNCE_OK),
        ("hive_announce_fail", &diag::HIVE_ANNOUNCE_FAIL),
    ];
    m.iter()
        .map(|(k, v)| (*k, v.load(Ordering::Relaxed)))
        .collect()
}

/// Host component of a text multiaddr (`/ip4/1.2.3.4/tcp/…`,
/// `/dns4/host/…`). Good enough for failure clustering; not a parser.
fn multiaddr_host(underlay: &str) -> Option<String> {
    let mut parts = underlay.split('/').filter(|s| !s.is_empty());
    while let Some(proto) = parts.next() {
        match proto {
            "ip4" | "ip6" | "dns" | "dns4" | "dns6" => return parts.next().map(str::to_string),
            _ => {
                // Every multiaddr protocol we expect here carries one
                // value component; skip it.
                parts.next();
            }
        }
    }
    None
}

fn parse_query(query: Option<&str>) -> HashMap<String, String> {
    query
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

fn param_usize(
    params: &HashMap<String, String>,
    key: &str,
    default: usize,
) -> Result<usize, String> {
    match params.get(key) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|e| format!("{key}: {e}")),
    }
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<RespBody> {
    let mut s = body.to_string();
    s.push('\n');
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(s)).boxed())
        .expect("static response parts")
}

fn json_line_response(status: StatusCode, message: &str) -> Response<RespBody> {
    json_response(status, &serde_json::json!({"error": message}))
}
