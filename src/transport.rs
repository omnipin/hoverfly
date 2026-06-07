//! libp2p transport built around bee's bidirectional handshake/pricing dance.
//!
//! Each public method (`fetch_chunk`, `pushsync_chunk`, `discover_peers`)
//! builds a fresh `Swarm`, dials the peer, drives the bidirectional
//! handshake + pricing exchange, then performs its operation and drops the
//! swarm. The transport only accepts ws/wss multiaddrs.

use core::time::Duration;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
    identity::Keypair,
    noise,
    swarm::{NetworkBehaviour, SwarmEvent, dial_opts::DialOpts},
    yamux,
};
use thiserror::Error;
use tracing::{debug, info};

use crate::dnsaddr::is_dialable_multiaddr;
use crate::peers::Peer;
use crate::protocols::handshake::{self, HandshakeError};
use crate::protocols::hive;
use crate::protocols::pricing;
use crate::protocols::pushsync::{self, PushsyncReceipt};
use crate::protocols::retrieval::{self, ChunkDelivery};
use crate::protocols::status as status_proto;
use crate::signer::{SignerError, SwarmSigner};

pub(crate) const HANDSHAKE_PROTO_V15: StreamProtocol =
    StreamProtocol::new("/swarm/handshake/15.0.0/handshake");
pub(crate) const HANDSHAKE_PROTO_V14: StreamProtocol =
    StreamProtocol::new("/swarm/handshake/14.0.0/handshake");
pub(crate) const PRICING_PROTO: StreamProtocol =
    StreamProtocol::new("/swarm/pricing/1.0.0/pricing");
pub(crate) const HIVE_PROTO_V2: StreamProtocol = StreamProtocol::new("/swarm/hive/2.0.0/peers");
pub(crate) const HIVE_PROTO_V1: StreamProtocol = StreamProtocol::new("/swarm/hive/1.1.0/peers");
pub(crate) const RETRIEVAL_PROTO: StreamProtocol =
    StreamProtocol::new("/swarm/retrieval/1.4.0/retrieval");
pub(crate) const PUSHSYNC_PROTO: StreamProtocol =
    StreamProtocol::new("/swarm/pushsync/1.3.1/pushsync");
pub(crate) const PSEUDOSETTLE_PROTO: StreamProtocol =
    StreamProtocol::new("/swarm/pseudosettle/1.0.0/pseudosettle");
#[cfg(not(target_arch = "wasm32"))]
pub(crate) const SWAP_PROTO: StreamProtocol = StreamProtocol::new("/swarm/swap/1.0.0/swap");
/// Bee status protocol. Bee opens this stream periodically from its
/// `pkg/salud` worker over any connection it has to us (including
/// outbound connections we initiated for pushsync), so EVERY session
/// — not just the daemon's inbound listener — has to accept it.
/// See `protocols::status` docs for the rationale.
pub(crate) const STATUS_PROTO: StreamProtocol = StreamProtocol::new("/swarm/status/1.1.3/status");
/// Minimum interval between successive dials to the same peer-id.
/// Bee's libp2p connection rate limiter
/// (`pkg/p2p/libp2p/libp2p.go::connLimiter`) allows 10 RPS / burst 40
/// per /32 source IP per bee node. Once the burst is exhausted the
/// limiter drops further dial attempts silently, which manifests as
/// the bee node closing the next connection mid-push.
///
/// The dispatcher's session-rotation pattern (popular high-PO peers
/// are rotated on essentially every connection-dead event) can hit
/// each top-tier peer 100+ times per upload otherwise — sustained
/// well above bee's 10 RPS limit even spread across many bees, and
/// concentrated on the few we keep wanting back.
///
/// 1 second is comfortably under any of bee's per-IP /32 limits
/// while still leaving the rotation responsive enough that a freshly
/// retired session's chunk can be pushed via that peer again within
/// a chunk's typical wall-clock window.
pub const DIAL_COOLDOWN: Duration = Duration::from_secs(1);

/// Bee's per-second refresh rate granted by pseudosettle.
/// See `pkg/node/node.go::refreshRate`.
pub const REFRESH_RATE_PLUR: u64 = 4_500_000;

/// Per-peer balance ceiling we enforce client-side, matching weeb-3's
/// `accounting::set_payment_threshold` (capped at `REFRESH_RATE * 2`).
/// Bee disconnects at `payment_threshold × 1.25` (≥ 16.875M default), so
/// 9M PLUR leaves plenty of headroom for in-flight rounds.
pub const SAFE_PEER_THRESHOLD_PLUR: u64 = REFRESH_RATE_PLUR * 2;

/// Bee's per-PO chunk price (`pkg/pricer/pricer.go::PO_PRICE`).
pub const PO_PRICE_PLUR: u64 = 10_000;

/// Maximum proximity order (`pkg/swarm::MaxPO`).
pub const MAX_PO: u8 = 31;

/// Compute `pricer.PeerPrice(peer, chunk)` — the PLUR cost of pushing
/// `chunk` to `peer`: `(MaxPO − proximity(peer, chunk) + 1) × PO_PRICE`.
pub fn peer_price(peer_overlay: &[u8; 32], chunk_addr: &[u8; 32]) -> u64 {
    let po = proximity(peer_overlay, chunk_addr);
    (u64::from(MAX_PO) - u64::from(po) + 1) * PO_PRICE_PLUR
}

/// Number of leading matching bits between two 32-byte addresses, capped
/// at `MAX_PO`. Mirrors nectar's `SwarmAddress::proximity`.
pub fn proximity(a: &[u8; 32], b: &[u8; 32]) -> u8 {
    for i in 0..32 {
        let xor = a[i] ^ b[i];
        if xor != 0 {
            let po = (i as u8) * 8 + (xor.leading_zeros() as u8);
            return po.min(MAX_PO);
        }
    }
    MAX_PO
}

/// Result of a price-aware push attempt.
#[derive(Debug)]
pub enum PushOutcome {
    /// Reserve succeeded, push delivered, peer acknowledged with a
    /// signed receipt whose signing overlay is *within* the chunk's
    /// AOR. The chunk has durably landed in a neighborhood reserve.
    Receipt(PushsyncReceipt),
    /// Reserve would have exceeded the peer's threshold even after an
    /// in-line settlement attempt. The push was not made; try a different
    /// peer or wait for refresh to free credit.
    Overdraft,
    /// Pushsync returned a receipt, but the signing peer's overlay is
    /// not within the chunk's storage radius — meaning the chunk was
    /// only forwarded, not stored in any peer's reserve. Bee mirrors
    /// this via `ErrShallowReceipt`; the upload should retry against
    /// a different (closer) peer so the chunk actually lands. The
    /// receipt is included so callers can log it for diagnostics.
    Shallow(PushsyncReceipt),
}

/// Heuristic: does this error mean the underlying libp2p connection is
/// dead and the caller should rotate to a fresh session? `Pushsync::Peer`
/// errors come from bee's pushsync handler returning a `Receipt{err}` —
/// the connection is fine. Frame / stream-control / IO / explicit
/// `ConnectionClosed` errors all indicate the swarm is gone.
///
/// `Timeout` is deliberately *not* included: a single slow pushsync
/// substream doesn't mean the yamux connection is broken. Treating it
/// as dead retires the whole session on one slow chunk, which at high
/// `--concurrency` (mass-correlated retirement across the pool) triggers
/// the rotation-dial cascade that collapses the live pool. The chunk
/// whose op timed out still surfaces an error to the dispatcher, which
/// advances to the next peer; the session stays useful for everything
/// else in flight on it. Ghost-balance accounting still increments on
/// timeouts (`push()` in `SessionState`), so a session that keeps
/// timing out retires naturally via the ghost-balance threshold.
/// Bucket an `io::Error` from `ConnectionError::IO` into a short
/// human-readable cause string. Walks the `source()` chain so wrapped
/// errors (most often `yamux::ConnectionError`) are visible. Returns
/// a short string suitable as a histogram key — repeated errors
/// collapse to the same bucket. Unrecognised errors get bucketed as
/// `other:<first 80 chars of Debug>` so we can iterate the classifier
/// without dropping data.
///
/// Common buckets we expect on mainnet:
/// - `tcp:reset`            — bee or an intermediate closed via TCP RST
/// - `tcp:broken-pipe`      — local side wrote to a closed socket
/// - `tcp:eof`              — peer closed cleanly; read returned 0 bytes
/// - `tcp:timed-out`        — local TCP send/recv timeout fired
/// - `yamux:closed`         — yamux connection closed (clean muxer shutdown)
/// - `yamux:decode`         — yamux frame decode failed (protocol error)
/// - `yamux:io:<kind>`      — yamux saw an underlying io error of `<kind>`
/// - `yamux:streams-exhausted` — `NoMoreStreamIds` or `TooManyStreams`
pub fn classify_io_error(e: &std::io::Error) -> String {
    // libp2p wraps connection errors through several layers
    // (`Error<>`, `Either<L,R>`, multistream-select, yamux,
    // `io::Error::Custom`). The innermost cause is what we actually
    // want — usually an OS errno or a yamux frame-protocol error.
    //
    // Empirically the debug repr of an `io::Error` from a dropped
    // connection looks like:
    //   Custom { kind: Other, error:
    //     Custom { kind: Other, error:
    //       Error(Right(Io(Os { code: 104, kind: ConnectionReset, message: "..." })))
    //     }
    //   }
    // or, for yamux protocol failures:
    //   Custom { kind: Other, error:
    //     Custom { kind: Other, error:
    //       Error(Right(Decode(Io(Os { code: 104, ... })))
    //     }
    //   }
    // or, for `multistream-select` negotiation failures:
    //   Custom { kind: Other, error: NegotiationError(...) }
    //
    // We pattern-match on the full debug string rather than walking
    // the source chain because the wrapping types are private to
    // libp2p / multistream-select / yamux. String matching is fragile
    // across libp2p versions but the alternative is downcasting against
    // a moving target, which is worse.
    let debug = format!("{:?}", e);

    // 1. OS errno path (`Os { code: N, kind: KindName, ... }`).
    if let Some(idx) = debug.find("Os { code: ") {
        let rest = &debug[idx + "Os { code: ".len()..];
        let code: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !code.is_empty() {
            let kind_s = if let Some(k_idx) = rest.find("kind: ") {
                rest[k_idx + 6..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphabetic())
                    .collect::<String>()
                    .to_lowercase()
            } else {
                "unknown".into()
            };
            // Was the Os error reached through a yamux decode?
            // `Error(Right(Decode(Io(Os { ... ))))` is yamux's
            // FrameDecodeError, which is "we tried to parse a yamux
            // frame and the underlying socket already died mid-read".
            // Tag those separately so they aren't conflated with raw
            // socket resets that happened between yamux frames.
            if debug.contains("Decode(Io(") {
                return format!("yamux-decode-mid-frame:{}:{}", kind_s, code);
            }
            // Raw socket error reached through yamux's normal Io path.
            return format!("yamux-io:{}:{}", kind_s, code);
        }
    }

    // 2. libp2p multistream-select negotiation failure (no Os errno).
    if debug.contains("NegotiationError") {
        return "multistream:negotiation-failed".into();
    }
    if debug.contains("ProtocolError") {
        return "multistream:protocol-error".into();
    }

    // 3. Yamux semantic errors (no inner Io).
    if debug.contains("Closed") {
        return "yamux:closed".into();
    }
    if debug.contains("NoMoreStreamIds") || debug.contains("TooManyStreams") {
        return "yamux:streams-exhausted".into();
    }
    if debug.contains("InvalidWindowUpdate") {
        return "yamux:invalid-window-update".into();
    }

    // 4. Unknown — record a truncated prefix so we can extend the
    // classifier without dropping data.
    let trimmed: String = debug.chars().take(120).collect();
    format!("other:{}", trimmed)
}

pub fn is_connection_dead(e: &TransportError) -> bool {
    use crate::protocols::pushsync::PushsyncError;
    match e {
        TransportError::ConnectionClosed => true,
        TransportError::StreamControl(_) => true,
        TransportError::Framing(_) => true,
        TransportError::Pushsync(PushsyncError::Frame(_)) => true,
        _ => false,
    }
}

/// Hard upper bound on pushes per session. Acts as a defence-in-depth
/// safety net for the [`GHOST_BALANCE_LIMIT_PLUR`] accounting; under
/// normal operation sessions retire on ghost balance long before they
/// hit this. Raised from the earlier conservative `25` because that
/// counted *all* pushes (successful or not), which doesn't reflect
/// bee's actual ghostBalance behaviour.
pub const MAX_PUSHES_PER_SESSION: u32 = 10_000;

/// Client-side mirror of bee's `ghostBalance` disconnect threshold.
/// Bee's `accounting.go` adds the chunk price to `ghostBalance` on
/// every push it *can't* forward (`debitAction.Cleanup()`), and
/// blocklists our overlay when `ghostBalance > ~16.875M PLUR`. Only a
/// fresh `Connect()` resets it. Successful pushes don't increment.
///
/// We rotate the session at 12M PLUR — well under bee's limit, leaves
/// headroom for in-flight pushes that haven't been counted yet, and
/// for any per-bee variation in the actual disconnect threshold.
pub const GHOST_BALANCE_LIMIT_PLUR: u64 = 12_000_000;

/// Pre-warm watermark as a fraction of [`GHOST_BALANCE_LIMIT_PLUR`].
/// We start dialing a replacement session once ghost balance reaches 2/3
/// of the retirement limit so the dial usually completes before the
/// active session has to be rotated.
pub const GHOST_BALANCE_PREWARM_NUMERATOR: u64 = 1;
pub const GHOST_BALANCE_PREWARM_DENOMINATOR: u64 = 2;

/// Diagnostic counters for session-retirement causes, surfaced at upload
/// end. Used to evaluate whether a per-peer reconnect strategy (à la
/// weeb-3) would recover meaningful throughput vs. the current
/// "rotation to a fresh peer" pattern. Specifically, we want to know how
/// often a session retires via [`is_connection_dead`] **before** its
/// ghost balance crossed the prewarm watermark — those are the cases
/// where bee likely didn't blocklist us and a reconnect to the same
/// peer would succeed.
pub mod diag {
    use std::sync::atomic::AtomicU64;
    /// Sessions that ended because a push task surfaced an `is_connection_dead`
    /// error AND the session's ghost balance at the moment of retirement was
    /// below [`super::GHOST_BALANCE_LIMIT_PLUR`] × [`super::GHOST_BALANCE_PREWARM_NUMERATOR`] /
    /// [`super::GHOST_BALANCE_PREWARM_DENOMINATOR`] (≈8M PLUR). I.e. the connection
    /// died for non-accounting reasons — network jitter, bee restart, NAT
    /// keepalive expiry, etc. Candidates for reconnect-to-same-peer.
    pub static DEAD_RETIRE_LOW_GHOST: AtomicU64 = AtomicU64::new(0);
    /// Sessions that ended via `is_connection_dead` with ghost balance
    /// above the prewarm watermark but below the retirement limit.
    /// Ambiguous: bee may have started blocklisting us on the dying connection.
    pub static DEAD_RETIRE_PREWARM_GHOST: AtomicU64 = AtomicU64::new(0);
    /// Sessions that ended via `is_connection_dead` at-or-above the
    /// retirement limit. Bee almost certainly blocklisted us; reconnect
    /// would bounce.
    pub static DEAD_RETIRE_HIGH_GHOST: AtomicU64 = AtomicU64::new(0);
    /// Sessions that ended cleanly via the ghost-balance retirement
    /// threshold (the prewarm path; rotation as designed).
    pub static GHOST_RETIRE: AtomicU64 = AtomicU64::new(0);
    /// Sessions that ended via [`super::MAX_PUSHES_PER_SESSION`].
    pub static MAX_PUSHES_RETIRE: AtomicU64 = AtomicU64::new(0);
    /// Prewarm dials triggered because a session's driver had already
    /// exited (`PeerSession::is_alive() == false`) by the time the
    /// dispatcher swept for prewarm candidates. These are the dials
    /// that would otherwise have been paid synchronously inside
    /// `try_push_with_rotation` on the next chunk routed through this
    /// entry — each one represents one dial RTT (~500-1500 ms on
    /// mainnet) of dispatcher wall time hidden in the background.
    pub static PREWARM_ON_DEAD: AtomicU64 = AtomicU64::new(0);
    /// Prewarm dials triggered by the ghost-balance watermark (the
    /// pre-existing path). Reported alongside `PREWARM_ON_DEAD` so the
    /// two prewarm causes can be told apart in the diag output.
    pub static PREWARM_ON_GHOST: AtomicU64 = AtomicU64::new(0);
    /// Cheques successfully written to a peer's swap substream. A
    /// non-zero value confirms the SWAP path is firing at runtime
    /// (the hypothesis the chequebook feature was added to test).
    /// Zero on an upload that did substantial pushes means
    /// pseudosettle is covering everything — sessions retire before
    /// debt grows large enough to need a cheque.
    pub static CHEQUE_EMITTED: AtomicU64 = AtomicU64::new(0);
    /// Cheque emission attempts that failed (bee reset the stream,
    /// the peer never sent an exchange rate, our cumulative cap was
    /// reached, etc.). Counted alongside `CHEQUE_EMITTED`; the ratio
    /// is the SWAP-path failure rate.
    pub static CHEQUE_FAILED: AtomicU64 = AtomicU64::new(0);

    // ─── ConnectionClosed cause classification ──────────────────────────
    //
    // libp2p's `SwarmEvent::ConnectionClosed.cause: Option<ConnectionError>`
    // has three observable states:
    // - `Some(IO(_))`  → TCP/yamux IO error (reset by peer, broken pipe,
    //                    muxer protocol error). Almost certainly bee
    //                    closed on us for a non-accounting reason.
    // - `Some(KeepAliveTimeout)` → libp2p connection-level keepalive
    //                              expired. Only fires if a swarm-level
    //                              keepalive is configured — we have not
    //                              configured one (we rely on yamux's
    //                              internal keepalive instead), so this
    //                              should always be zero. If it isn't,
    //                              that's a real finding.
    // - `None` → clean close. Bee deliberately disconnected (e.g.
    //            accounting blocklist) or the close was driven from our
    //            side (e.g. session driver dropped the swarm).
    //
    // Counted only in the SessionDriver loop (running sessions), not in
    // prep_connection — failures there are dial-time and already
    // accounted for separately.
    pub static CONN_CLOSED_IO: AtomicU64 = AtomicU64::new(0);
    pub static CONN_CLOSED_KEEPALIVE: AtomicU64 = AtomicU64::new(0);
    pub static CONN_CLOSED_CLEAN: AtomicU64 = AtomicU64::new(0);

    /// Detailed breakdown of `ConnectionError::IO(io::Error)` causes,
    /// keyed by `format!("{:?}", io_err)` truncated/normalised to one
    /// of the canonical bucket strings used here. Populated alongside
    /// `CONN_CLOSED_IO` so that bucket-by-cause counts always sum to
    /// the total IO count. See the match in `SessionDriver` for the
    /// mapping. Lock-protected (not atomic) because we mutate a
    /// `BTreeMap` rather than a fixed set of counters — the cause
    /// strings are open-ended and we discover them at runtime.
    pub static CONN_CLOSED_IO_DETAIL: std::sync::Mutex<std::collections::BTreeMap<String, u64>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());

    /// Outbound hive self-announcements that succeeded (bee accepted
    /// the `Peers` envelope without resetting the stream). Bee may
    /// still reject us at the dial-probe stage if our advertised
    /// underlay isn't reachable — that happens asynchronously after
    /// our stream closes and we don't see it. So a high count here
    /// means we *tried* to register, not that bee actually added us
    /// to `knownPeers`. Confirm via long-term metrics: incoming
    /// connection rate from new peers over many sessions.
    pub static HIVE_ANNOUNCE_OK: AtomicU64 = AtomicU64::new(0);
    /// Outbound hive announcements where the stream open or the
    /// `Peers` write itself failed. Usually means the connection died
    /// between handshake and the hive substream open — see
    /// `CONN_CLOSED_*` for the cause classification.
    pub static HIVE_ANNOUNCE_FAIL: AtomicU64 = AtomicU64::new(0);

    // ─── Per-push wall-clock histogram ──────────────────────────────────
    //
    // Coarse 4-bucket histogram of `do_pushsync` outer timing,
    // bucketed by total wall-clock spent on (open_stream + push +
    // receipt). Used to find where chunks actually spend their time —
    // we've been guessing this is yamux flow control vs. multistream
    // negotiation vs. bee-side forwarding latency without measuring.
    //
    // Bucket boundaries are deliberately broad so single-trial runs
    // produce a clear shape. Tighten if you find a specific cliff to
    // investigate.
    pub static PUSH_LATENCY_LT_100MS: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_LATENCY_100_500MS: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_LATENCY_500MS_2S: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_LATENCY_2_5S: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_LATENCY_5_10S: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_LATENCY_GT_10S: AtomicU64 = AtomicU64::new(0);

    // ─── Open-stream wall-clock histogram ───────────────────────────────
    //
    // Same buckets, but for just the open_stream portion. If pushes
    // are slow but open_stream is fast, the bottleneck is yamux
    // window / receipt RTT. If open_stream itself is slow, the
    // bottleneck is the Control → Handler rendezvous channel or
    // multistream-select negotiation.
    pub static OPEN_STREAM_LT_10MS: AtomicU64 = AtomicU64::new(0);
    pub static OPEN_STREAM_10_100MS: AtomicU64 = AtomicU64::new(0);
    pub static OPEN_STREAM_100_500MS: AtomicU64 = AtomicU64::new(0);
    pub static OPEN_STREAM_GT_500MS: AtomicU64 = AtomicU64::new(0);

    // ─── Push outcome counters (bumped inside do_pushsync result handling) ───
    //
    // Compare with bee's `bee_pushsync_shallow_receipt` and similar
    // counters available at bee's `/metrics` endpoint. If our shallow
    // rate is far above bee's (5.9% in observation), that's direct
    // evidence we route to peers further from chunk AORs than bee
    // does, which is the structural advantage bee gets from having
    // 133 stable kademlia neighbors.
    pub static PUSH_OUTCOME_OK: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_OUTCOME_SHALLOW: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_OUTCOME_OVERDRAFT: AtomicU64 = AtomicU64::new(0);
    pub static PUSH_OUTCOME_ERROR: AtomicU64 = AtomicU64::new(0);

    // ─── Chunk-completion latency histogram ─────────────────────────────
    //
    // From "this chunk enters the dispatcher" → "this chunk gets its
    // first non-shallow receipt (or the fallback shallow accept)".
    // This is the per-chunk wall-clock that actually determines
    // throughput (with `buffer` chunks in flight, we push
    // `buffer / mean(chunk_latency)` chunks per second).
    //
    // Compare against bee: bee's wall-time for a 5 MiB upload is
    // ~7s with `ConcurrentPushes=128`. That implies mean chunk
    // latency ≈ 128 / (1280 / 7) ≈ 700ms. We've seen ~50-100s wall
    // time, so our mean chunk latency is much higher despite mean
    // push latency being similar — the difference must be in
    // dispatcher overhead or chunk-level retries.
    pub static CHUNK_LATENCY_LT_500MS: AtomicU64 = AtomicU64::new(0);
    pub static CHUNK_LATENCY_500MS_2S: AtomicU64 = AtomicU64::new(0);
    pub static CHUNK_LATENCY_2_5S: AtomicU64 = AtomicU64::new(0);
    pub static CHUNK_LATENCY_5_15S: AtomicU64 = AtomicU64::new(0);
    pub static CHUNK_LATENCY_GT_15S: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("multiaddr is not ws/wss: {0}")]
    NotWebSocket(String),
    #[error("multiaddr missing peer id")]
    MissingPeerId,
    #[error("dial failed: {0}")]
    DialFailed(String),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("timeout")]
    Timeout,
    #[error("handshake: {0}")]
    Handshake(#[from] HandshakeError),
    #[error("hive: {0}")]
    Hive(#[from] crate::protocols::hive::HiveError),
    #[error("pricing: {0}")]
    Pricing(#[from] crate::protocols::pricing::PricingError),
    #[error("retrieval: {0}")]
    Retrieval(#[from] crate::protocols::retrieval::RetrievalError),
    #[error("pushsync: {0}")]
    Pushsync(#[from] crate::protocols::pushsync::PushsyncError),
    #[error("stream control: {0}")]
    StreamControl(String),
    #[error("framing: {0}")]
    Framing(#[from] crate::protocols::framing::FrameError),
    #[error("signer: {0}")]
    Signer(#[from] SignerError),
    #[error("network mismatch: expected {ours}, got {theirs}")]
    NetworkMismatch { ours: u64, theirs: u64 },
    #[error("pseudosettle: {0}")]
    PseudoSettle(String),
    #[error("swap: {0}")]
    Swap(String),
    #[error("status: {0}")]
    Status(String),
    /// The caller asked to dial a peer that we dialed too recently.
    /// Surfaced by [`PeerSession::connect`] when the per-peer cooldown
    /// hasn't elapsed yet (see [`Transport::dial_cooldown_remaining`]).
    /// The dispatcher catches this and tries a different peer for the
    /// chunk that triggered the rotation, so the upload doesn't stall
    /// waiting on a single peer's rate-limit window.
    #[error("dial too soon (try again in {wait:?})")]
    DialTooSoon { wait: Duration },
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    /// Per-operation timeout (one pushsync substream, one retrieval
    /// substream, one pseudosettle round-trip). Sized for round-trip
    /// latency + bee handler processing, not for connection setup.
    pub timeout: Duration,
    /// Wall-clock budget for the entire `PeerSession::connect()` —
    /// libp2p dial + identify + handshake + pricing. Healthy peers
    /// finish in ≪ 1 s; dead/NAT'd ones eat the full budget. Kept
    /// separate from `timeout` so we can fail dial-attempts on dead
    /// peers in 2-3 s while still allowing 10+ s for slow push/fetch
    /// round trips on live peers.
    pub dial_timeout: Duration,
    pub network_id: u64,
    /// Underlay multiaddr we advertise to bee peers in the handshake.
    /// When `None` (the default, used by ephemeral clients), we
    /// advertise a synthetic 127.0.0.1 loopback that bee accepts but
    /// can't dial back. The daemon's inbound-serving mode should set
    /// this to its real, externally-routable listen address so bee
    /// peers we connect to learn our underlay, add us to their
    /// kademlia tables, and route subsequent retrieval lookups for our
    /// uploaded chunks straight to us. Must include the `/p2p/<id>` tail.
    pub advertise: Option<Multiaddr>,
    /// Per-connection cap on concurrent outbound substream upgrades
    /// (`/swarm/pushsync/…`, `/swarm/retrieval/…`, etc.). Forwarded to
    /// every [`crate::protocols::stream_pool::Behaviour`] this transport
    /// builds. Default is
    /// [`crate::protocols::stream_pool::DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES`].
    /// Lower values reduce per-push yamux flow-control contention at
    /// the cost of less parallel substream-open throughput; higher
    /// values do the reverse. Sweet spot is workload-dependent; see
    /// `PERFORMANCE.md`.
    pub max_concurrent_substream_upgrades: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            dial_timeout: Duration::from_secs(3),
            network_id: 1,
            advertise: None,
            max_concurrent_substream_upgrades:
                crate::protocols::stream_pool::DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES,
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub stream: crate::protocols::stream_pool::Behaviour,
    pub identify: libp2p::identify::Behaviour,
    /// libp2p ping (`/ipfs/ping/1.0.0`). Bee's `pkg/p2p/libp2p/internal/reacher`
    /// uses this to verify that a peer's advertised underlay is dial-able.
    /// If we don't respond to pings, bee marks us
    /// `ReachabilityStatusPrivate` and kademlia prunes us from
    /// oversaturated bins every 5 minutes
    /// (`defaultPruneWakeup` in `pkg/topology/kademlia/kademlia.go`).
    /// Without ping support a long-running daemon's entire pool gets
    /// disconnected every prune tick — observed empirically as
    /// `pool maintenance: pruned 32 dead, 0 live` on every 5-min cycle.
    pub ping: libp2p::ping::Behaviour,
}

fn behaviour(keypair: &Keypair, max_concurrent_substream_upgrades: usize) -> Behaviour {
    Behaviour {
        stream: crate::protocols::stream_pool::Behaviour::with_max_concurrent_upgrades(
            max_concurrent_substream_upgrades,
        ),
        identify: libp2p::identify::Behaviour::new(
            libp2p::identify::Config::new("/swarm/0.1.0".to_string(), keypair.public())
                .with_agent_version(format!("hoverfly/{}", crate::VERSION)),
        ),
        ping: libp2p::ping::Behaviour::new(libp2p::ping::Config::default()),
    }
}

/// SWAP / chequebook configuration shared across all sessions of a
/// `Transport`. When `Some`, every newly-opened session performs a
/// one-shot swap handshake (advertising our beneficiary = our
/// signer's Ethereum address) and then the auto-settle path may emit
/// cheques after pseudosettle when remaining debt > minimumPayment.
///
/// When `None` (default), behaviour is unchanged from the original
/// pseudosettle-only client: sessions retire by ghost-balance or
/// connection-death rotation instead of paying.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct SwapConfig {
    /// 20-byte chequebook contract address on `chain_id`. MUST be a
    /// chequebook deployed by bee's official factory
    /// (`pkg/config/chain.go::CurrentFactoryAddress`), MUST have its
    /// `issuer()` equal to the signer's Ethereum address. We don't
    /// verify either of those here — bee will reject cheques otherwise,
    /// surfacing as session resets in metrics.
    pub chequebook: [u8; 20],
    /// EVM chain id used in the EIP-712 domain of the cheque
    /// signature. Mainnet Swarm (Gnosis) = 100, Sepolia testnet =
    /// 11155111.
    pub chain_id: u64,
    /// Max cumulative payout (BZZ-wei) we'll ever issue to any single
    /// peer. Once a peer's cumulative reaches this, we revert to
    /// pseudosettle-only for that peer. Bee's
    /// `chequestore.go:176` rejects cheques whose
    /// `CumulativePayout - alreadyPaidOut` exceeds the chequebook
    /// balance, so the user should set this <= chequebook deposited
    /// balance to avoid bouncing cheques.
    pub max_cumulative_per_peer_bzz: alloy_primitives::U256,
    /// Persistent per-peer cumulative-payout state. Shared across all
    /// sessions; must be loaded from disk before first session and
    /// flushed on shutdown.
    pub store: std::sync::Arc<tokio::sync::Mutex<crate::cheques::ChequeStore>>,
}

pub struct Transport {
    keypair: Keypair,
    signer: SwarmSigner,
    config: TransportConfig,
    #[cfg(not(target_arch = "wasm32"))]
    swap: Option<SwapConfig>,
    /// Optional status-snapshot responder. When `Some`, every
    /// outbound session accepts `/swarm/status/1.1.0/status` and
    /// returns this snapshot in response to bee's salud probes.
    /// When `None`, the protocol is still accepted at the
    /// multistream-select level (so bee doesn't log "protocol not
    /// supported") but we drop inbound status streams without
    /// responding — same Unhealthy outcome but without the
    /// protocol-negotiation noise. See `protocols::status` docs.
    status_snapshot: Option<std::sync::Arc<crate::protocols::status::StatusSnapshot>>,
    /// Shared reachability log: every dial / session open / healthcheck
    /// records its (overlay → success/failure + rtt) here. The CLI drains
    /// the log after an operation completes and writes the observations
    /// back to `peers.json`, so future runs skip recently-failed peers
    /// up-front. Always present so callers don't need to check.
    reachability_log: crate::peers::ReachabilityLog,
    /// Per-peer-id last-dial timestamp. Used to enforce a minimum
    /// interval between successive dials to the same peer so we stay
    /// under bee's per-IP libp2p connection rate limiter
    /// (`pkg/p2p/libp2p/libp2p.go::connLimiter` — 10 RPS, burst 40
    /// per /32). Without this, the pool rotation pattern (popular
    /// high-PO peers re-dialed every time their session retires)
    /// produces 100+ dials/peer per upload and bee starts silently
    /// dropping our connections mid-push.
    dial_cooldown:
        std::sync::Arc<std::sync::Mutex<std::collections::HashMap<PeerId, web_time::Instant>>>,
}

impl Transport {
    pub fn new(signer: SwarmSigner, config: TransportConfig) -> Self {
        let keypair = Keypair::generate_ed25519();
        Self {
            keypair,
            signer,
            config,
            #[cfg(not(target_arch = "wasm32"))]
            swap: None,
            status_snapshot: None,
            reachability_log: crate::peers::new_log(),
            dial_cooldown: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    /// Attach a status snapshot used to respond to bee's salud probes.
    /// Required for the kademlia-prune mitigation: without responding,
    /// we stay marked Unhealthy and get preferentially disconnected.
    /// See `protocols::status` and PERFORMANCE.md "Public reachability".
    pub fn with_status_snapshot(
        mut self,
        snapshot: crate::protocols::status::StatusSnapshot,
    ) -> Self {
        self.status_snapshot = Some(std::sync::Arc::new(snapshot));
        self
    }

    /// Attach SWAP/chequebook configuration. Native-only. When set,
    /// sessions automatically advertise the chequebook beneficiary and
    /// emit signed cheques as part of the settle path. See
    /// [`SwapConfig`] for the prerequisites the caller is responsible
    /// for (chequebook deployed by bee's factory, etc.).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_swap(mut self, swap: SwapConfig) -> Self {
        self.swap = Some(swap);
        self
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn swap(&self) -> Option<&SwapConfig> {
        self.swap.as_ref()
    }

    /// Like [`Self::new`] but pins the libp2p keypair to a caller-
    /// supplied value. The daemon uses this to keep outbound dials and
    /// the inbound listener under the same libp2p peer-id — without
    /// that, bee peers dial back to our advertised underlay, hit the
    /// listener under a different peer-id, and reject the connection.
    pub fn new_with_keypair(
        signer: SwarmSigner,
        config: TransportConfig,
        keypair: Keypair,
    ) -> Self {
        Self {
            keypair,
            signer,
            config,
            #[cfg(not(target_arch = "wasm32"))]
            swap: None,
            status_snapshot: None,
            reachability_log: crate::peers::new_log(),
            dial_cooldown: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    pub const fn signer(&self) -> &SwarmSigner {
        &self.signer
    }

    pub const fn config(&self) -> &TransportConfig {
        &self.config
    }

    /// Reachability observations collected by recent dial attempts.
    /// Drain with [`crate::peers::apply_log`] to update a `PeerStore`.
    pub fn reachability_log(&self) -> &crate::peers::ReachabilityLog {
        &self.reachability_log
    }

    /// How long the caller must wait before the next dial to `peer_id`
    /// to stay under bee's per-IP connection rate limit (10 RPS / burst
    /// 40 per /32, see `pkg/p2p/libp2p/libp2p.go`). `None` if we're
    /// clear to dial now.
    pub(crate) fn dial_cooldown_remaining(&self, peer_id: &PeerId) -> Option<Duration> {
        let last = self.dial_cooldown.lock().ok()?.get(peer_id).copied()?;
        let since = web_time::Instant::now().saturating_duration_since(last);
        if since >= DIAL_COOLDOWN {
            None
        } else {
            Some(DIAL_COOLDOWN - since)
        }
    }

    /// Same as `dial_cooldown_remaining` but takes a multiaddr.
    /// Returns `None` if the multiaddr lacks a `/p2p/<peer_id>`
    /// component (`open_session` would fail with `MissingPeerId`
    /// in that case — caller can either skip or proceed and let
    /// `open_session` surface the error).
    pub fn dial_cooldown_for_underlay(&self, underlay: &Multiaddr) -> Option<Duration> {
        let peer_id = extract_peer_id(underlay)?;
        self.dial_cooldown_remaining(&peer_id)
    }

    /// Record that we are about to dial `peer_id`. Called from
    /// [`PeerSession::connect`] after the cooldown check passes so
    /// concurrent callers can't race past it.
    fn note_dial(&self, peer_id: &PeerId) {
        if let Ok(mut map) = self.dial_cooldown.lock() {
            map.insert(*peer_id, web_time::Instant::now());
        }
    }

    /// Fetch a single chunk by address. Convenience for single-shot fetches —
    /// opens a fresh connection, does the handshake/pricing dance, fetches one
    /// chunk, and tears down. For multi-chunk workloads use `PeerSession`.
    pub async fn fetch_chunk(
        &self,
        peer_addr: &Multiaddr,
        chunk_addr: &[u8; 32],
    ) -> Result<ChunkDelivery, TransportError> {
        let session = PeerSession::connect(self, peer_addr).await?;
        session.fetch_chunk(chunk_addr).await
    }

    /// Open a long-lived session to a peer. The handshake and pricing dance
    /// happen once; subsequent `pushsync_chunk` / `fetch_chunk` calls reuse
    /// the underlying libp2p connection (each opens a fresh yamux substream).
    pub async fn open_session(&self, peer_addr: &Multiaddr) -> Result<PeerSession, TransportError> {
        PeerSession::connect(self, peer_addr).await
    }

    /// Discover peers from one node by listening on the hive stream.
    ///
    /// Bee's `BroadcastPeers` opens a fresh stream per 30-peer batch
    /// (see `pkg/hive/hive.go`), so a query against a well-connected
    /// peer typically yields 2-5 batches back-to-back. We drain all
    /// batches that arrive before `wait` elapses, with an early-exit
    /// after 750 ms of post-batch silence to avoid sitting idle on
    /// the deadline once gossip has stopped.
    pub async fn discover_peers(
        &self,
        peer_addr: &Multiaddr,
        wait: Duration,
    ) -> Result<Vec<Peer>, TransportError> {
        let peer_id = ensure_ws(peer_addr)?;
        let mut swarm = build_swarm(self).await?;
        let mut control = swarm.behaviour().stream.new_control();
        // Accept both handshake & hive versions. Bee's inbound
        // dial-back mirrors our outbound choice; we don't yet know
        // which version we'll negotiate.
        let mut hs_in_v15 = accept(&mut control, HANDSHAKE_PROTO_V15)?;
        let mut hs_in_v14 = accept(&mut control, HANDSHAKE_PROTO_V14)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let mut hive_in_v2 = accept(&mut control, HIVE_PROTO_V2)?;
        let mut hive_in_v1 = accept(&mut control, HIVE_PROTO_V1)?;
        dial(&mut swarm, peer_id, peer_addr)?;

        let underlay = prep_connection(&mut swarm, peer_id, self.config.timeout).await?;
        let hs = do_handshake(
            &mut swarm,
            peer_id,
            &mut control,
            &mut hs_in_v15,
            &mut hs_in_v14,
            &underlay,
            &self.signer,
            self.config.advertise.as_ref(),
        )
        .await?;
        let _peer_threshold = do_pricing(
            &mut swarm,
            peer_id,
            &mut control,
            &mut pr_in,
            self.config.timeout,
        )
        .await?;

        // Drain hive `peers` envelopes until either the hard deadline
        // (`wait`) elapses, the connection drops, or we hit `QUIET_FOR`
        // of silence after a batch.
        //
        // Bee's `BroadcastPeers` (`pkg/hive/hive.go::BroadcastPeers`)
        // sends one batch of at most `maxBatchSize = 30` peers per
        // stream and opens a fresh stream per batch. Bee's `Announce`
        // (`pkg/topology/kademlia/kademlia.go::Announce`) aggregates
        // up to `BroadcastBinSize × MaxBins = 64` peers across
        // kademlia bins, plus the full neighborhood if we land
        // in-radius, so a well-connected peer typically sends 2-5
        // batches back-to-back within ~500 ms of the handshake
        // completing. Breaking after the first batch (the prior
        // behaviour) systematically lost 50-80% of the peers a single
        // query could yield.
        //
        // Bee does not proactively close the connection after the
        // gossip finishes, so without the quiet-window short-circuit
        // we would always idle out the full `wait` window — which
        // makes the obvious "set `wait` to capture the slowest peer"
        // tuning unattractively expensive (per-round wall clock =
        // `ceil(peers / concurrency) × wait`).
        const QUIET_FOR: Duration = Duration::from_millis(750);
        // Seed the result with the dialed peer itself: it just completed a full
        // bee handshake, so it's a known-reachable forwarder. Bootnodes in
        // particular are highly-connected and (being /ws-dialable via AutoTLS)
        // are among the very few peers a browser build can reach — keeping them
        // in the retrieval set markedly improves browser fetch reliability,
        // which would otherwise depend only on the sparse /ws peers that hive
        // happens to gossip. `peer_addr` carries the `/p2p/<id>` tail so the
        // retrieval dialer can extract the peer-id.
        let mut peers: Vec<Peer> = vec![Peer {
            overlay: hex::encode(hs.peer_overlay),
            underlays: vec![peer_addr.to_string()],
            ..Default::default()
        }];
        let mut batches_read = 0usize;
        let deadline = web_time::Instant::now() + wait;
        let mut last_batch_at: Option<web_time::Instant> = None;
        loop {
            let now = web_time::Instant::now();
            if now >= deadline {
                break;
            }
            // Short-circuit once the gossip burst stops.
            if let Some(t) = last_batch_at {
                if now.duration_since(t) >= QUIET_FOR {
                    break;
                }
            }
            let hard_remaining = deadline - now;
            let soft_remaining = last_batch_at
                .map(|t| QUIET_FOR.saturating_sub(now.duration_since(t)))
                .unwrap_or(hard_remaining);
            let remaining = hard_remaining.min(soft_remaining);
            tokio::select! {
                _ = tokio::time::sleep(remaining) => continue,
                ev = hive_in_v2.next() => {
                    match ev {
                        Some((pid, mut stream)) if pid == peer_id => {
                            debug!(target: "hoverfly::hive",
                                "inbound hive v2 stream opened (batch {})", batches_read + 1);
                            match poll_until(&mut swarm, hive::read_peers(&mut stream)).await {
                                Ok(mut batch) => {
                                    let n = batch.len();
                                    peers.append(&mut batch);
                                    batches_read += 1;
                                    last_batch_at = Some(web_time::Instant::now());
                                    info!(target: "hoverfly::hive",
                                        "read {} peers (batch {}, total {})",
                                        n, batches_read, peers.len());
                                }
                                Err(e) => debug!(target: "hoverfly::hive", "read_peers err: {}", e),
                            }
                        }
                        Some(_) => {}
                        None => break,
                    }
                }
                ev = hive_in_v1.next() => {
                    match ev {
                        Some((pid, mut stream)) if pid == peer_id => {
                            debug!(target: "hoverfly::hive",
                                "inbound hive v1 stream opened (batch {})", batches_read + 1);
                            match poll_until(&mut swarm, hive::read_peers(&mut stream)).await {
                                Ok(mut batch) => {
                                    let n = batch.len();
                                    peers.append(&mut batch);
                                    batches_read += 1;
                                    last_batch_at = Some(web_time::Instant::now());
                                    info!(target: "hoverfly::hive",
                                        "read {} peers (batch {}, total {})",
                                        n, batches_read, peers.len());
                                }
                                Err(e) => debug!(target: "hoverfly::hive", "read_peers err: {}", e),
                            }
                        }
                        Some(_) => {}
                        None => break,
                    }
                }
                ev = swarm.select_next_some() => {
                    if let SwarmEvent::ConnectionClosed { peer_id: pid, .. } = ev {
                        if pid == peer_id { break; }
                    }
                }
            }
        }
        Ok(peers)
    }
}

/// A long-lived libp2p connection to one peer, with handshake + pricing
/// already completed. Each `pushsync_chunk` / `fetch_chunk` call opens a
/// fresh yamux substream over the existing connection — far cheaper than
/// redialing and re-handshaking per chunk (the dominant cost for large
/// uploads on bee, where the per-connection setup is ~150-300ms).
///
/// The session owns its `Swarm` on a dedicated background task; this is
/// essential because libp2p stalls (yamux pings, identify, noise, the
/// connection itself) if no one polls the swarm. The handle is `Clone +
/// Send + Sync` so many concurrent callers can submit work through it.
#[derive(Clone)]
pub struct PeerSession {
    cmd_tx: tokio::sync::mpsc::Sender<SessionCommand>,
    peer_id: PeerId,
    /// Shared with the driver so callers can observe the push counter
    /// without round-tripping through the command channel — used by
    /// the upload layer to pre-warm a replacement session before this
    /// one hits its rotation limit.
    state: std::sync::Arc<SessionState>,
}

enum SessionCommand {
    PushSync {
        addr: [u8; 32],
        wire: Vec<u8>,
        stamp: Vec<u8>,
        /// Price (PLUR) we'll be debited if the push succeeds. Used for
        /// the client-side overdraft check before we touch the wire.
        price_plur: u64,
        reply: tokio::sync::oneshot::Sender<Result<PushOutcome, TransportError>>,
    },
    Fetch {
        addr: [u8; 32],
        reply: tokio::sync::oneshot::Sender<Result<ChunkDelivery, TransportError>>,
    },
    /// Outbound salud-style probe — measure this peer's response
    /// latency on the status protocol so the caller can decide
    /// whether to keep using them. No accounting, no settlement.
    StatusProbe {
        reply: tokio::sync::oneshot::Sender<Result<Duration, TransportError>>,
    },
}

impl PeerSession {
    /// Dial `peer_addr`, complete identify + handshake + pricing, and spawn
    /// the swarm driver task. The returned handle stays usable until either
    /// the driver task exits (connection dropped, peer crashed) or every
    /// handle is dropped.
    pub async fn connect(
        transport: &Transport,
        peer_addr: &Multiaddr,
    ) -> Result<Self, TransportError> {
        // Enforce a per-peer-id minimum gap between dials. Bee
        // libp2p (pkg/p2p/libp2p/libp2p.go::connLimiter) rate-limits
        // inbound connections from a single /32 IP to 10 RPS / burst
        // 40. Without this gate, the dispatcher's session-rotation
        // pattern (popular high-PO peers retired + re-dialed on
        // every chunk push) typically triggers 100+ dials per peer
        // per upload — well past the bee rate limit, after which bee
        // silently drops our connections mid-push and we cascade
        // into more retries.
        //
        // Caller surfaces this as `DialTooSoon` so the dispatcher
        // can pick a different peer for the chunk that triggered
        // the rotation, instead of stalling on a sleep.
        let peer_id = ensure_ws(peer_addr)?;
        if let Some(wait) = transport.dial_cooldown_remaining(&peer_id) {
            return Err(TransportError::DialTooSoon { wait });
        }
        transport.note_dial(&peer_id);

        // The dial phase (connect + identify + handshake + pricing) is
        // bounded by `dial_timeout`. Healthy peers finish well under 1 s;
        // dead peers used to eat the full per-op timeout (10+ s) before
        // we'd give up. Once the session is open, per-substream work uses
        // `config.timeout` instead — see the SessionState below.
        let dial_budget = transport.config.dial_timeout;
        tokio::time::timeout(dial_budget, Self::connect_inner(transport, peer_addr))
            .await
            .map_err(|_| TransportError::Timeout)?
    }

    async fn connect_inner(
        transport: &Transport,
        peer_addr: &Multiaddr,
    ) -> Result<Self, TransportError> {
        let peer_id = ensure_ws(peer_addr)?;
        let mut swarm = build_swarm(transport).await?;
        let mut control = swarm.behaviour().stream.new_control();
        // Accept handshake & hive on both protocol versions. The
        // outbound `do_handshake` picks v15 first and falls back to
        // v14, so a peer's inbound dial-back will normally land on
        // whichever version we just used. The unused v14 acceptor
        // sits idle when talking to a v15 peer (and vice versa)
        // costing nothing but an `IncomingStreams` future.
        let mut hs_in_v15 = accept(&mut control, HANDSHAKE_PROTO_V15)?;
        let mut hs_in_v14 = accept(&mut control, HANDSHAKE_PROTO_V14)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let hive_in_v2 = accept(&mut control, HIVE_PROTO_V2)?;
        let hive_in_v1 = accept(&mut control, HIVE_PROTO_V1)?;
        // Accept STATUS even if no snapshot is configured — bee opens
        // this stream from its `pkg/salud` worker on every connection,
        // and a multistream-select NACK on this protocol gets logged
        // on bee's side as if we don't speak it (which would
        // contribute to the Unhealthy classification). When the
        // session driver has no snapshot configured it will simply
        // drop the inbound stream after accepting it; bee sees an
        // EOF, not a protocol NACK.
        let status_in = accept(&mut control, STATUS_PROTO)?;
        // We deliberately do NOT accept pullsync substreams (cursors
        // or pullsync). hoverfly is an upload client, not a reserve
        // maintainer — we have no chunks to offer back. Earlier we
        // accepted both and responded with empty cursors / empty
        // offers to "look like a citizen", but:
        //
        //  - Salud doesn't probe pullsync (only the status protocol),
        //    so empty offers don't help health checks.
        //  - Bee's puller runs a live-sync worker per bin per
        //    connected peer that loops as fast as our response
        //    arrives. Empty offers are cheap to respond to but bee
        //    immediately re-asks → constant noise, no rate limit
        //    backpressure on bee's side because `WaitN(ctx, 0)` is
        //    instant.
        //  - Honest rejection (no accept → bee gets multistream
        //    ErrNotSupported → bee's `IncompatibleStreamError`) is
        //    handled by bee's puller as a regular error and just
        //    logged. Bee doesn't escalate to disconnect/blocklist,
        //    so our outbound session survives.
        //
        // Net: dropping pullsync acceptance saves CPU and memory
        // (no per-substream tokio task) without throughput penalty.
        // The bee-light pattern (`p2p.WithBlocklistStreams` at
        // pkg/node/node.go:1057-1059) doesn't apply directly because
        // bee-light DISCONNECTS the requester; we can't afford to
        // burn our outbound connection for each unwanted pullsync.
        dial(&mut swarm, peer_id, peer_addr)?;
        let underlay = prep_connection(&mut swarm, peer_id, transport.config.dial_timeout).await?;
        #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
        let hs_result = do_handshake(
            &mut swarm,
            peer_id,
            &mut control,
            &mut hs_in_v15,
            &mut hs_in_v14,
            &underlay,
            &transport.signer,
            transport.config.advertise.as_ref(),
        )
        .await?;
        let _peer_threshold = do_pricing(
            &mut swarm,
            peer_id,
            &mut control,
            &mut pr_in,
            transport.config.dial_timeout,
        )
        .await?;
        // Note: we deliberately keep `SAFE_PEER_THRESHOLD_PLUR` as the
        // local cap even though `_peer_threshold` is typically much
        // larger. Lifting the cap led to thundering-herd contention on
        // the per-session accounting mutex (overdrafts on 50 MiB shot
        // from ~1.6k to ~51k once every session held ~10M PLUR of
        // pending pushes simultaneously). The narrower cap forces
        // pseudosettles to happen more often but keeps the dispatch
        // queue from piling up.
        let threshold_plur = SAFE_PEER_THRESHOLD_PLUR;

        // Hive self-announcement (best-effort). Tell bee about
        // ourselves by sending a `Peers` envelope containing our own
        // BzzAddress over a fresh `/swarm/hive/1.1.0/peers` substream.
        // Bee's `peersHandler` reads the envelope, dial-probes each
        // announced address to verify reachability (works because we
        // run `--listen --advertise`), and adds successfully verified
        // entries to `knownPeers`. From there, bee's manage loop may
        // dial us OUTBOUND in the future, admitting us to kademlia
        // with `forceConnection=true` — bypassing the bin-saturation
        // check that rejects normal inbound clients.
        //
        // The benefit is cumulative across sessions: each bee we
        // announce ourselves to remembers our overlay (provided
        // overlay stability via `SwarmSigner::from_bytes_with_nonce`)
        // and may re-establish the connection later. Over time this
        // grows our kademlia presence beyond what any single session
        // could achieve, which feeds back into bee's salud probes
        // marking us Healthy, which further reduces our prune-priority.
        //
        // Failure here is silent: no announcement means bee's
        // peersHandler never sees us, but the session still works
        // for outbound pushes. We log at debug and move on.
        #[cfg(not(target_arch = "wasm32"))]
        if !hs_result.our_underlay.is_empty() {
            let our_overlay = transport.signer.overlay().to_vec();
            let our_nonce = transport.signer.nonce().to_vec();
            // Hive announce uses the same version family as the
            // handshake we just completed: v14 handshake ⇒ v1 hive,
            // v15 ⇒ v2. Bee gossiped records must include a non-zero
            // timestamp on v2; we replay the timestamp we signed in
            // the handshake (`hs_result.our_timestamp`).
            let hive_version = match hs_result.version {
                handshake::Version::V14 => crate::protocols::hive::Version::V1,
                handshake::Version::V15 => crate::protocols::hive::Version::V2,
            };
            let me = crate::protocols::hive::OwnBzzAddress {
                overlay: our_overlay,
                underlay: hs_result.our_underlay.clone(),
                signature: hs_result.our_signature.clone(),
                nonce: our_nonce,
                timestamp: hs_result.our_timestamp,
                // Light client, no chequebook on chain.
                chequebook_address: [0u8; 20],
            };
            match do_hive_announce(
                &mut swarm,
                peer_id,
                &mut control,
                transport.config.dial_timeout,
                &me,
                hive_version,
            )
            .await
            {
                Ok(()) => diag::HIVE_ANNOUNCE_OK.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                Err(e) => {
                    diag::HIVE_ANNOUNCE_FAIL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::debug!(
                        target: "hoverfly::transport",
                        "hive announce to {} failed: {} — session continues",
                        peer_id, e
                    );
                    0
                }
            };
        }

        // SWAP handshake (best-effort). If anything fails, fall back
        // to pseudosettle-only and let the session run without cheque
        // emission. We don't want a swap-stream hiccup to abort a
        // session that's otherwise perfectly capable of pushing.
        #[cfg(not(target_arch = "wasm32"))]
        let swap = match (&transport.swap, hs_result.peer_eth_address) {
            (Some(cfg), Some(peer_eth)) => {
                let our_beneficiary = *transport.signer.eth_address();
                match do_swap_handshake(
                    &mut swarm,
                    peer_id,
                    &mut control,
                    transport.config.dial_timeout,
                    &our_beneficiary,
                )
                .await
                {
                    Ok(()) => Some(SessionSwap {
                        peer_beneficiary: peer_eth,
                        peer_overlay_hex: hex::encode(hs_result.peer_overlay),
                        signer: transport.signer.clone(),
                        config: cfg.clone(),
                    }),
                    Err(e) => {
                        tracing::debug!(
                            target: "hoverfly::transport",
                            "swap handshake failed with {}: {} — falling back to pseudosettle-only",
                            peer_id, e
                        );
                        None
                    }
                }
            }
            _ => None,
        };

        let timeout = transport.config.timeout;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<SessionCommand>(64);
        let state = std::sync::Arc::new(SessionState {
            control,
            peer_id,
            timeout,
            accounting: tokio::sync::Mutex::new(AccountingState {
                reserve_plur: 0,
                balance_plur: 0,
                threshold_plur,
                last_settle: None,
            }),
            settle_lock: tokio::sync::Mutex::new(()),
            pushes_used: std::sync::atomic::AtomicU32::new(0),
            ghost_balance_plur: std::sync::atomic::AtomicU64::new(0),
            connection_dead: std::sync::atomic::AtomicBool::new(false),
            dead_notify: tokio::sync::Notify::new(),
            #[cfg(not(target_arch = "wasm32"))]
            swap,
        });
        let session_state = state.clone();
        spawn_session_driver(SessionDriver {
            swarm,
            state,
            cmd_rx,
            _hs_in_v15: hs_in_v15,
            _hs_in_v14: hs_in_v14,
            _pr_in: pr_in,
            _hive_in_v2: hive_in_v2,
            _hive_in_v1: hive_in_v1,
            status_in,
            status_snapshot: transport.status_snapshot.clone(),
        });
        Ok(Self {
            cmd_tx,
            peer_id,
            state: session_state,
        })
    }

    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Pushes attempted on this session's underlying connection so far.
    /// This is only a defence-in-depth metric; normal rotation is driven
    /// by [`Self::ghost_balance_plur`].
    pub fn pushes_used(&self) -> u32 {
        self.state
            .pushes_used
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Client-side mirror of bee's per-overlay `ghostBalance`.
    pub fn ghost_balance_plur(&self) -> u64 {
        self.state
            .ghost_balance_plur
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Push one chunk over a fresh substream on this session's connection,
    /// honouring client-side per-peer accounting.
    ///
    /// `price_plur` is what bee will debit our balance on success (compute
    /// via [`peer_price`]). The session refuses the push if accepting it
    /// would push its tracked balance past [`SAFE_PEER_THRESHOLD_PLUR`]
    /// (even after an in-line pseudosettle), returning
    /// [`PushOutcome::Overdraft`] so the caller can try another peer.
    pub async fn pushsync_chunk_priced(
        &self,
        chunk_addr: &[u8; 32],
        chunk_data: &[u8],
        stamp: &[u8],
        price_plur: u64,
    ) -> Result<PushOutcome, TransportError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::PushSync {
            addr: *chunk_addr,
            wire: chunk_data.to_vec(),
            stamp: stamp.to_vec(),
            price_plur,
            reply: tx,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        rx.await.map_err(|_| TransportError::ConnectionClosed)?
    }

    /// True if the session's driver task is still accepting commands.
    /// False once the driver has exited (e.g. underlying libp2p
    /// connection died, or ghost-balance / max-pushes retirement
    /// completed and the in-flight tasks have drained).
    ///
    /// Used by the upload dispatcher to decide whether to enqueue a
    /// prewarm dial for an entry even when its ghost balance is below
    /// the prewarm watermark — a `dead_low_ghost` retirement was
    /// empirically the dominant retirement cause on mainnet (see the
    /// `transport::diag` counters), and waiting for the next chunk's
    /// `pushsync_chunk_priced` call to surface the failure (and burn
    /// a synchronous re-dial inside `try_push_with_rotation`) costs
    /// 500-1500 ms of dispatcher wall time per dead session.
    pub fn is_alive(&self) -> bool {
        !self.cmd_tx.is_closed()
    }

    /// Probe the peer's status-protocol responsiveness. Returns the
    /// observed round-trip duration on success. Used by the pool to
    /// pre-filter slow peers, mirroring bee's salud→Healthy pipeline.
    pub async fn status_probe(&self) -> Result<Duration, TransportError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::StatusProbe { reply: tx };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        rx.await.map_err(|_| TransportError::ConnectionClosed)?
    }

    /// Fetch one chunk over a fresh substream on this session's connection.
    pub async fn fetch_chunk(
        &self,
        chunk_addr: &[u8; 32],
    ) -> Result<ChunkDelivery, TransportError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::Fetch {
            addr: *chunk_addr,
            reply: tx,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        rx.await.map_err(|_| TransportError::ConnectionClosed)?
    }
}

/// Mutable, lock-protected accounting state shared across concurrent
/// pushes on a single session. Mirrors `pkg/accounting/accounting.go`:
/// - `reserve_plur` is PLUR locked in by in-flight pushes (not yet
///   committed against the peer's balance);
/// - `balance_plur` is PLUR we've committed but haven't yet settled.
/// A push is refused if `reserve + balance + price > threshold`.
struct AccountingState {
    reserve_plur: u64,
    balance_plur: u64,
    threshold_plur: u64,
    /// `Instant` of our last successful pseudosettle. Bee rejects two
    /// settles within the same wall-second on its end, so we serialize
    /// settles per peer and gate them on this.
    last_settle: Option<web_time::Instant>,
}

impl AccountingState {
    /// `weeb-3::accounting::reserve`: atomically check
    /// `reserve + balance + price ≤ threshold` and, if so, add to reserve.
    fn try_reserve(&mut self, price: u64) -> bool {
        let Some(new_reserve) = self.reserve_plur.checked_add(price) else {
            return false;
        };
        let Some(committed) = self.balance_plur.checked_add(new_reserve) else {
            return false;
        };
        if committed > self.threshold_plur {
            return false;
        }
        self.reserve_plur = new_reserve;
        true
    }
}

/// Shared session state. Cloned (via `Arc`) into every concurrent push /
/// fetch task spawned on a session so they can race over the same
/// libp2p connection.
struct SessionState {
    control: crate::protocols::stream_pool::Control,
    peer_id: PeerId,
    timeout: Duration,
    accounting: tokio::sync::Mutex<AccountingState>,
    /// Serializes pseudosettle attempts on this peer — bee rejects
    /// two settles within the same wall-second, and back-to-back
    /// concurrent settles would just both re-settle the same balance.
    settle_lock: tokio::sync::Mutex<()>,
    /// Pushes attempted on this connection so far. Tracked only as a
    /// safety net against [`MAX_PUSHES_PER_SESSION`]; under normal
    /// operation retirement is driven by [`ghost_balance_plur`].
    pushes_used: std::sync::atomic::AtomicU32,
    /// Client-side mirror of bee's per-overlay `ghostBalance`. Bee
    /// increments this on every push it can't forward; we increment
    /// on every push that returns Err (timeout, dial fail, peer-side
    /// receipt error). When it crosses [`GHOST_BALANCE_LIMIT_PLUR`]
    /// the session retires and the upload loop dials a replacement.
    /// Successful pushes don't increment — bee doesn't burn ghost
    /// balance on them.
    ghost_balance_plur: std::sync::atomic::AtomicU64,
    /// Connection-death signal. Set to `true` by the SessionDriver
    /// the moment it observes `SwarmEvent::ConnectionClosed` for our
    /// peer. The `dead_notify` Notify wakes all in-flight push and
    /// fetch tasks so they can fail fast instead of waiting up to
    /// `--timeout` (default 10 s) for their substreams to error.
    ///
    /// Why this matters: when bee's kademlia prunes us, the
    /// connection dies via `SO_LINGER=0` RST (see PERFORMANCE.md
    /// "Session-death cause"). Every pushsync substream that's
    /// mid-write or mid-read at that moment stalls until libp2p
    /// surfaces the error, which can take seconds. Multiplied
    /// across our ~256 concurrent sessions and the 100+ sessions
    /// that die during a 5 MiB upload, this stall time is the
    /// dominant contributor to our per-stream 2-5 s tail.
    /// Surfacing the dead flag immediately lets the dispatcher
    /// re-route those chunks within milliseconds.
    connection_dead: std::sync::atomic::AtomicBool,
    dead_notify: tokio::sync::Notify,
    /// Per-session SWAP state. `None` if SWAP is disabled at the
    /// `Transport` level or if the peer's BzzAddress signature
    /// couldn't be recovered to an Ethereum address (rare; bee
    /// always signs correctly). When `Some`, the auto-settle path
    /// will emit cheques after pseudosettle if remaining debt warrants
    /// one.
    #[cfg(not(target_arch = "wasm32"))]
    swap: Option<SessionSwap>,
}

#[cfg(not(target_arch = "wasm32"))]
struct SessionSwap {
    /// Beneficiary (peer's Ethereum address recovered from their
    /// BzzAddress signature in our handshake). Stays fixed for the
    /// life of the session — bee re-derives our beneficiary from our
    /// handshake signature on each connection, and our derivation of
    /// their address is symmetric.
    peer_beneficiary: [u8; 20],
    /// Peer's overlay (hex, lowercase). Used to key into the cheque
    /// store sidecar.
    peer_overlay_hex: String,
    /// Cloned from `Transport.signer`. Used to sign cheques —
    /// `signer.eth_address()` MUST equal `config.chequebook`'s
    /// on-chain `issuer()` for cheques to validate. We hold a clone
    /// per session so the settle path doesn't need to thread the
    /// transport's signer through `Arc<SessionState>`.
    signer: SwarmSigner,
    config: SwapConfig,
}

impl SessionState {
    /// One push, with accounting. Mirrors `weeb-3::upload::push_chunk` +
    /// `weeb-3::accounting::{reserve, apply_credit, cancel_reserve}`.
    /// Safe to call concurrently — accounting is mutex-protected, and
    /// each call opens its own yamux substream via the cloned `Control`.
    async fn push(
        self: &std::sync::Arc<Self>,
        addr: &[u8; 32],
        wire: &[u8],
        stamp: &[u8],
        price: u64,
    ) -> Result<PushOutcome, TransportError> {
        // 1. Try to reserve. If we'd exceed threshold, try an in-line
        // settlement to recover credit, then re-check.
        {
            let mut acc = self.accounting.lock().await;
            if !acc.try_reserve(price) {
                drop(acc);
                let _ = self.try_settle_once().await;
                let mut acc = self.accounting.lock().await;
                if !acc.try_reserve(price) {
                    return Ok(PushOutcome::Overdraft);
                }
            }
        }

        // 2. Do the actual push over a fresh substream.
        let result = self.do_pushsync(addr, wire, stamp).await;

        // 3. Account for the outcome.
        let should_settle = {
            let mut acc = self.accounting.lock().await;
            match &result {
                Ok(_) => {
                    acc.reserve_plur = acc.reserve_plur.saturating_sub(price);
                    acc.balance_plur = acc.balance_plur.saturating_add(price);
                    acc.balance_plur >= REFRESH_RATE_PLUR
                }
                Err(_) => {
                    acc.reserve_plur = acc.reserve_plur.saturating_sub(price);
                    self.ghost_balance_plur
                        .fetch_add(price, std::sync::atomic::Ordering::Relaxed);
                    false
                }
            }
        };

        // Auto-settle when balance reaches one refresh-rate (weeb-3:
        // `apply_credit` triggers the refresh channel at this level).
        // Outside the accounting lock so concurrent pushes don't stall.
        if should_settle {
            let _ = self.try_settle_once().await;
        }

        result.map(PushOutcome::Receipt)
    }

    async fn fetch(
        self: &std::sync::Arc<Self>,
        addr: &[u8; 32],
    ) -> Result<ChunkDelivery, TransportError> {
        self.do_fetch(addr).await
    }

    async fn do_pushsync(
        &self,
        addr: &[u8; 32],
        wire: &[u8],
        stamp: &[u8],
    ) -> Result<PushsyncReceipt, TransportError> {
        let t_start = web_time::Instant::now();
        // Fail fast if the session's already known dead. Saves the
        // open_stream round-trip on an entry whose connection died
        // between when the dispatcher picked it and when we got
        // scheduled.
        if self
            .connection_dead
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(TransportError::ConnectionClosed);
        }
        let mut control = self.control.clone();
        // Race the stream open against the dead-notify so we bail
        // immediately if the connection dies mid-negotiation.
        let open = tokio::select! {
            r = tokio::time::timeout(
                self.timeout,
                control.open_stream(self.peer_id, PUSHSYNC_PROTO),
            ) => {
                r.map_err(|_| TransportError::Timeout)?
                    .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?
            }
            _ = self.dead_notify.notified() => {
                return Err(TransportError::ConnectionClosed);
            }
        };
        let t_opened = web_time::Instant::now();
        let mut stream = open;
        // Same race for the push body: if the connection dies after
        // we've opened the stream but before we've finished the
        // read-receipt phase, we want to error immediately rather
        // than waiting up to `--timeout` (default 10 s) for the
        // substream to surface the closed-connection error.
        let result = tokio::select! {
            r = tokio::time::timeout(
                self.timeout,
                pushsync::push(&mut stream, addr, wire, stamp),
            ) => {
                r.map_err(|_| TransportError::Timeout)?
                    .map_err(Into::<TransportError>::into)
            }
            _ = self.dead_notify.notified() => {
                Err(TransportError::ConnectionClosed)
            }
        };
        let t_pushed = web_time::Instant::now();
        let open_us = (t_opened - t_start).as_micros() as u64;
        let total_us = (t_pushed - t_start).as_micros() as u64;
        tracing::trace!(
            target: "hoverfly::profile",
            peer = %self.peer_id,
            open_stream_us = open_us,
            push_total_us = (t_pushed - t_opened).as_micros() as u64,
            ok = result.is_ok(),
            "do_pushsync_outer",
        );
        // Histogram bumping for end-of-upload diagnostic. We bucket by
        // *successful* push only to avoid measuring error-retry noise.
        if result.is_ok() {
            let total_ms = total_us / 1_000;
            if total_ms < 100 {
                diag::PUSH_LATENCY_LT_100MS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if total_ms < 500 {
                diag::PUSH_LATENCY_100_500MS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if total_ms < 2000 {
                diag::PUSH_LATENCY_500MS_2S.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if total_ms < 5000 {
                diag::PUSH_LATENCY_2_5S.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if total_ms < 10000 {
                diag::PUSH_LATENCY_5_10S.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                diag::PUSH_LATENCY_GT_10S.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let open_ms = open_us / 1_000;
            if open_ms < 10 {
                diag::OPEN_STREAM_LT_10MS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if open_ms < 100 {
                diag::OPEN_STREAM_10_100MS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if open_ms < 500 {
                diag::OPEN_STREAM_100_500MS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                diag::OPEN_STREAM_GT_500MS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        result
    }

    async fn do_fetch(&self, addr: &[u8; 32]) -> Result<ChunkDelivery, TransportError> {
        if self
            .connection_dead
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(TransportError::ConnectionClosed);
        }
        let mut control = self.control.clone();
        let open = tokio::select! {
            r = tokio::time::timeout(
                self.timeout,
                control.open_stream(self.peer_id, RETRIEVAL_PROTO),
            ) => {
                r.map_err(|_| TransportError::Timeout)?
                    .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?
            }
            _ = self.dead_notify.notified() => {
                return Err(TransportError::ConnectionClosed);
            }
        };
        let mut stream = open;
        tokio::select! {
            r = tokio::time::timeout(self.timeout, retrieval::fetch(&mut stream, addr)) => {
                r.map_err(|_| TransportError::Timeout)?.map_err(Into::into)
            }
            _ = self.dead_notify.notified() => {
                Err(TransportError::ConnectionClosed)
            }
        }
    }

    /// One outbound salud-style status probe. Mirrors what bee's
    /// `pkg/salud::probe` does periodically against every connected
    /// peer: open a `/swarm/status/1.1.0/status` substream, send Get,
    /// read Snapshot, measure wall-clock round-trip. Caller uses the
    /// elapsed time as a per-peer responsiveness signal; the response
    /// payload is discarded.
    async fn do_status_probe(&self) -> Result<Duration, TransportError> {
        if self
            .connection_dead
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(TransportError::ConnectionClosed);
        }
        let mut control = self.control.clone();
        let started = web_time::Instant::now();
        let open = tokio::select! {
            r = tokio::time::timeout(
                self.timeout,
                control.open_stream(self.peer_id, STATUS_PROTO),
            ) => {
                r.map_err(|_| TransportError::Timeout)?
                    .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?
            }
            _ = self.dead_notify.notified() => {
                return Err(TransportError::ConnectionClosed);
            }
        };
        let mut stream = open;
        let result = tokio::select! {
            r = tokio::time::timeout(self.timeout, status_proto::request(&mut stream)) => {
                r.map_err(|_| TransportError::Timeout)?
                    .map_err(|e| TransportError::Status(e.to_string()))
            }
            _ = self.dead_notify.notified() => {
                Err(TransportError::ConnectionClosed)
            }
        };
        result.map(|_| started.elapsed())
    }

    /// Issue one pseudosettle Payment, then (if SWAP is configured and
    /// the pseudosettle didn't clear our debt) issue one cheque.
    /// Serialized across concurrent pushes via `settle_lock`. Mirrors
    /// bee's `pkg/accounting/accounting.go::settle`, which runs the
    /// refresh and the monetary settlement *in parallel* but always
    /// triggers refresh first. We serialize for simplicity — the
    /// total wall time is at most one extra substream open per
    /// settle which is dominated by accounting RTT anyway.
    ///
    /// Best-effort: errors are swallowed because failure to settle just
    /// means the next reserve attempt will report overdraft.
    async fn try_settle_once(&self) -> Result<(), TransportError> {
        let _guard = self.settle_lock.lock().await;

        // Bee rejects two pseudosettles within the same wall-second.
        let needs_wait = {
            let acc = self.accounting.lock().await;
            acc.last_settle
                .map(|t| t.elapsed())
                .filter(|d| *d < Duration::from_millis(1100))
                .map(|d| Duration::from_millis(1100) - d)
        };
        if let Some(wait) = needs_wait {
            tokio::time::sleep(wait).await;
        }

        let owed = {
            let acc = self.accounting.lock().await;
            acc.balance_plur.saturating_add(acc.reserve_plur)
        };
        if owed == 0 {
            return Ok(());
        }
        let ack = self.do_pseudosettle(u128::from(owed)).await?;
        let accepted = ack.amount_plur.min(u128::from(u64::MAX)) as u64;
        let remaining_after_pseudo = {
            let mut acc = self.accounting.lock().await;
            acc.last_settle = Some(web_time::Instant::now());
            acc.balance_plur = acc.balance_plur.saturating_sub(accepted);
            debug!(
                target: "hoverfly::transport",
                "pseudosettle with {}: asked={} accepted={} balance={} reserve={}",
                self.peer_id, owed, accepted, acc.balance_plur, acc.reserve_plur,
            );
            acc.balance_plur
        };

        // If pseudosettle didn't cover everything and SWAP is configured
        // for this session, emit a cheque for the remainder.
        #[cfg(not(target_arch = "wasm32"))]
        if remaining_after_pseudo > 0 && self.swap.is_some() {
            if let Err(e) = self.try_emit_cheque(remaining_after_pseudo).await {
                diag::CHEQUE_FAILED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(
                    target: "hoverfly::transport",
                    "cheque emission to {} failed: {} — leaving balance in place",
                    self.peer_id, e
                );
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = remaining_after_pseudo;

        Ok(())
    }

    /// Sign and emit one SWAP cheque covering `plur_to_settle` PLUR
    /// of debt. The cheque's BZZ-wei amount is
    /// `plur_to_settle * exchange_rate + deduction`, read from the
    /// per-stream `exchange`/`deduction` headers bee derives from its
    /// on-chain priceoracle. No-op if SWAP isn't configured for this
    /// session, if the peer doesn't send a usable exchange rate, or
    /// if our per-peer cumulative cap would be exceeded.
    #[cfg(not(target_arch = "wasm32"))]
    async fn try_emit_cheque(&self, plur_to_settle: u64) -> Result<(), TransportError> {
        use crate::protocols::swap;
        use alloy_primitives::U256;

        let Some(swap_state) = self.swap.as_ref() else {
            return Ok(());
        };
        if plur_to_settle == 0 {
            return Ok(());
        }

        // Open the swap stream and read the per-stream exchange rate.
        let mut control = self.control.clone();
        let mut stream =
            tokio::time::timeout(self.timeout, control.open_stream(self.peer_id, SWAP_PROTO))
                .await
                .map_err(|_| TransportError::Timeout)?
                .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;

        let rates = tokio::time::timeout(self.timeout, swap::read_settlement_rates(&mut stream))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|e| TransportError::Swap(e.to_string()))?;

        // Compute BZZ-wei amount: bee's chequestore.go:139-143 enforces
        // `amount - deduction >= exchange`, i.e. amount >= exchange +
        // deduction. We compute amount = plur * exchange + deduction
        // (mirrors swapprotocol.go:224-225, where sentAmount =
        // paymentAmount*exchangeRate + deduction).
        let plur_u256 = U256::from(plur_to_settle);
        let amount_bzz = plur_u256
            .checked_mul(rates.exchange_rate)
            .and_then(|p| p.checked_add(rates.deduction))
            .ok_or_else(|| TransportError::Swap("BZZ amount overflows u256".into()))?;
        if amount_bzz < rates.exchange_rate.saturating_add(rates.deduction) {
            // Pathological: deduction == 0 and amount < exchange would
            // bounce as ErrChequeValueTooLow. Defensive guard for the
            // arithmetic above.
            return Err(TransportError::Swap("amount below minimum payable".into()));
        }

        // Bump per-peer cumulative under the store lock, then sign.
        // The store mutation has to happen *before* we sign because
        // we sign the new cumulative, but it has to happen *after*
        // we know we have a usable rate so we don't bump on a
        // doomed attempt.
        let (cumulative, capped) = {
            let mut store = swap_state.config.store.lock().await;
            let cur = store.cumulative(&swap_state.peer_overlay_hex);
            let new = U256::from(cur).saturating_add(amount_bzz);
            if new > swap_state.config.max_cumulative_per_peer_bzz {
                (U256::from(cur), true)
            } else {
                let delta: u128 = amount_bzz
                    .try_into()
                    .map_err(|_| TransportError::Swap("cheque amount exceeds u128".into()))?;
                let next = store
                    .bump_and_get(&swap_state.peer_overlay_hex, delta)
                    .map_err(|e| TransportError::Swap(e.to_string()))?;
                if let Err(e) = store.save() {
                    tracing::warn!(
                        target: "hoverfly::transport",
                        "cheques.json save failed: {} — continuing in-memory only", e
                    );
                }
                (U256::from(next), false)
            }
        };
        if capped {
            return Err(TransportError::Swap(
                "per-peer cumulative cap reached".into(),
            ));
        }

        // Sign the cheque (EIP-712, our overlay signer must == chequebook issuer).
        let chequebook = swap_state.config.chequebook;
        let signature = swap_state
            .signer
            .sign_cheque(
                &chequebook,
                &swap_state.peer_beneficiary,
                cumulative,
                swap_state.config.chain_id,
            )
            .map_err(|e| TransportError::Swap(e.to_string()))?;

        // Emit.
        tokio::time::timeout(
            self.timeout,
            swap::emit_cheque(
                &mut stream,
                &chequebook,
                &swap_state.peer_beneficiary,
                cumulative,
                &signature,
            ),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::Swap(e.to_string()))?;

        // Clear the PLUR portion we just paid off from balance_plur.
        // We optimistically assume bee credits the full amount; if
        // not, our next pseudosettle/cheque will see a stale balance
        // but that's recoverable.
        {
            let mut acc = self.accounting.lock().await;
            acc.balance_plur = acc.balance_plur.saturating_sub(plur_to_settle);
        }
        diag::CHEQUE_EMITTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            target: "hoverfly::transport",
            "cheque emitted to {}: {} PLUR → {} BZZ-wei (cumulative {})",
            self.peer_id, plur_to_settle, amount_bzz, cumulative,
        );
        Ok(())
    }

    async fn do_pseudosettle(
        &self,
        amount_plur: u128,
    ) -> Result<crate::protocols::pseudosettle::PaymentAck, TransportError> {
        let mut control = self.control.clone();
        let open = tokio::time::timeout(
            self.timeout,
            control.open_stream(self.peer_id, PSEUDOSETTLE_PROTO),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
        let mut stream = open;
        tokio::time::timeout(self.timeout, write_then_read_empty_headers(&mut stream))
            .await
            .map_err(|_| TransportError::Timeout)??;
        let ack = tokio::time::timeout(
            self.timeout,
            crate::protocols::pseudosettle::pay(&mut stream, amount_plur),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::PseudoSettle(e.to_string()))?;
        Ok(ack)
    }
}

struct SessionDriver {
    swarm: Swarm<Behaviour>,
    state: std::sync::Arc<SessionState>,
    cmd_rx: tokio::sync::mpsc::Receiver<SessionCommand>,
    // Held to keep the accept registrations alive for the lifetime
    // of the session. Two of each because we accept handshake/hive on
    // both protocol versions (v14 + v15 / v1 + v2) during the bee
    // 2.8.0 network-wide upgrade.
    _hs_in_v15: crate::protocols::stream_pool::IncomingStreams,
    _hs_in_v14: crate::protocols::stream_pool::IncomingStreams,
    _pr_in: crate::protocols::stream_pool::IncomingStreams,
    _hive_in_v2: crate::protocols::stream_pool::IncomingStreams,
    _hive_in_v1: crate::protocols::stream_pool::IncomingStreams,
    /// Inbound `/swarm/status/1.1.0/status` streams. Bee's `pkg/salud`
    /// opens this stream periodically (10s..5min backoff) over any
    /// connection it has to us — including the outbound connections
    /// our session pool maintains. NOT responding leaves us marked
    /// `Unhealthy` (the zero-value default), which makes us prime
    /// candidates for kademlia bin-prune disconnection. So unlike the
    /// other three `_*_in` slots, we actively poll this one and
    /// respond. See `protocols::status` module docs.
    ///
    /// `None` when the daemon's status responder isn't configured;
    /// the stream pool still accepts the protocol so multistream-select
    /// negotiation succeeds (bee won't see "protocol not supported"),
    /// but we drop the stream without writing a snapshot. That's still
    /// better than not accepting at all (we get an EOF-style close
    /// rather than a multistream NACK).
    status_in: crate::protocols::stream_pool::IncomingStreams,
    status_snapshot: Option<std::sync::Arc<crate::protocols::status::StatusSnapshot>>,
}

impl SessionDriver {
    /// Drive the libp2p swarm forever, accepting commands and spawning
    /// each push/fetch as a concurrent sub-future. Commands only borrow
    /// the cheap-to-clone `Control` (via the shared `Arc<SessionState>`),
    /// so the swarm can run alongside many in-flight requests without
    /// having to wait for one to finish before starting the next.
    ///
    /// libp2p's stream behaviour communicates with `Control` via internal
    /// channels — as long as the swarm is being polled (the `select_next_some`
    /// arm below), the streams returned by `Control::open_stream` make
    /// progress in parallel.
    async fn run(mut self) {
        use futures::stream::FuturesUnordered;
        use std::sync::atomic::Ordering;

        // Native tokio::spawn requires Send; wasm spawn_local doesn't,
        // and tokio_with_wasm::time::Sleep isn't Send anyway.
        #[cfg(not(target_arch = "wasm32"))]
        type TaskFuture = std::pin::Pin<Box<dyn core::future::Future<Output = bool> + Send>>;
        #[cfg(target_arch = "wasm32")]
        type TaskFuture = std::pin::Pin<Box<dyn core::future::Future<Output = bool>>>;
        let mut tasks: FuturesUnordered<TaskFuture> = FuturesUnordered::new();
        let mut accept_new = true;

        loop {
            tokio::select! {
                biased;

                cmd = self.cmd_rx.recv(), if accept_new => {
                    let Some(cmd) = cmd else { break };
                    match cmd {
                        SessionCommand::PushSync { addr, wire, stamp, price_plur, reply } => {
                            let used = self.state.pushes_used.fetch_add(1, Ordering::Relaxed) + 1;
                            if used > MAX_PUSHES_PER_SESSION {
                                // Decline new pushes once we've hit the rotation
                                // limit; reply with ConnectionClosed so the caller
                                // dials a fresh session (resets bee's ghostBalance).
                                let _ = reply.send(Err(TransportError::ConnectionClosed));
                                diag::MAX_PUSHES_RETIRE.fetch_add(1, Ordering::Relaxed);
                                accept_new = false;
                                continue;
                            }
                            let state = self.state.clone();
                            tasks.push(Box::pin(async move {
                                let res = state.push(&addr, &wire, &stamp, price_plur).await;
                                let dead = matches!(&res, Err(e) if is_connection_dead(e));
                                let _ = reply.send(res);
                                dead
                            }));
                            let ghost = self.state.ghost_balance_plur.load(Ordering::Relaxed);
                            if ghost >= GHOST_BALANCE_LIMIT_PLUR && accept_new {
                                debug!(target: "hoverfly::transport",
                                    "session {} retiring at ghost_balance={} after {} pushes",
                                    self.state.peer_id, ghost, used);
                                diag::GHOST_RETIRE.fetch_add(1, Ordering::Relaxed);
                                accept_new = false;
                            }
                        }
                        SessionCommand::Fetch { addr, reply } => {
                            let state = self.state.clone();
                            tasks.push(Box::pin(async move {
                                let res = state.fetch(&addr).await;
                                let _ = reply.send(res);
                                false
                            }));
                        }
                        SessionCommand::StatusProbe { reply } => {
                            let state = self.state.clone();
                            tasks.push(Box::pin(async move {
                                let res = state.do_status_probe().await;
                                let dead = matches!(&res, Err(e) if is_connection_dead(e));
                                let _ = reply.send(res);
                                dead
                            }));
                        }
                    }
                }

                Some(dead) = tasks.next(), if !tasks.is_empty() => {
                    if dead && accept_new {
                        // Only count the first dead task per session.
                        // Subsequent in-flight tasks on the same dying
                        // connection will also surface dead errors, but
                        // the session is already retiring — counting
                        // them would over-report N-fold.
                        let ghost = self.state.ghost_balance_plur.load(Ordering::Relaxed);
                        let prewarm = GHOST_BALANCE_LIMIT_PLUR
                            .saturating_mul(GHOST_BALANCE_PREWARM_NUMERATOR)
                            / GHOST_BALANCE_PREWARM_DENOMINATOR;
                        if ghost >= GHOST_BALANCE_LIMIT_PLUR {
                            diag::DEAD_RETIRE_HIGH_GHOST.fetch_add(1, Ordering::Relaxed);
                        } else if ghost >= prewarm {
                            diag::DEAD_RETIRE_PREWARM_GHOST.fetch_add(1, Ordering::Relaxed);
                        } else {
                            diag::DEAD_RETIRE_LOW_GHOST.fetch_add(1, Ordering::Relaxed);
                        }
                        debug!(target: "hoverfly::transport",
                            "session {} retiring: underlying connection dead, ghost_balance={}",
                            self.state.peer_id, ghost);
                        accept_new = false;
                    } else if dead {
                        // Subsequent dead task on already-retiring session;
                        // not counted but log at trace for diagnostics.
                        let ghost = self.state.ghost_balance_plur.load(Ordering::Relaxed);
                        tracing::trace!(target: "hoverfly::transport",
                            "session {} additional dead task on retiring session, ghost_balance={}",
                            self.state.peer_id, ghost);
                    } else {
                        let ghost = self.state.ghost_balance_plur.load(Ordering::Relaxed);
                        if ghost >= GHOST_BALANCE_LIMIT_PLUR && accept_new {
                            debug!(target: "hoverfly::transport",
                                "session {} retiring at ghost_balance={}",
                                self.state.peer_id, ghost);
                            diag::GHOST_RETIRE.fetch_add(1, Ordering::Relaxed);
                            accept_new = false;
                        }
                    }
                }

                // Inbound status streams from bee's salud worker. Bee
                // probes us periodically (10s..5min backoff) over the
                // outbound connection this session owns. We must
                // respond fast and with plausible values, else bee
                // marks us Unhealthy and we become the top
                // bin-prune target — losing the session to
                // ECONNRESET. The handler runs in a spawned task so
                // it can't block the dispatcher; it's bounded by
                // `self.state.timeout` against a slow/dead stream.
                Some((_pid, mut stream)) = self.status_in.next(), if self.status_snapshot.is_some() => {
                    let snapshot = self.status_snapshot.clone().expect("guarded by if");
                    let timeout = self.state.timeout;
                    let task: TaskFuture = Box::pin(async move {
                        let _ = tokio::time::timeout(
                            timeout,
                            crate::protocols::status::respond(&mut stream, &snapshot),
                        ).await;
                        true
                    });
                    tasks.push(task);
                }

                ev = self.swarm.select_next_some() => {
                    // We don't care about most events here (identify
                    // pushes, behaviour events, dial outcomes etc.
                    // are all handled either at session-setup time or
                    // are no-ops at this stage). The one we DO care
                    // about is `ConnectionClosed`, because its `cause`
                    // field is the only place libp2p tells us *why*
                    // the underlying connection died — and the
                    // `dead_low_ghost` retirement counter has been
                    // showing this is the dominant retirement cause
                    // at high concurrency without telling us whether
                    // bee closed us (`IO`), libp2p timed us out
                    // (`KeepAliveTimeout`), or someone clean-closed
                    // (`None`). See `diag::CONN_CLOSED_*`.
                    if let libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, cause, .. } = ev {
                        if peer_id == self.state.peer_id {
                            use libp2p::swarm::ConnectionError;
                            match cause {
                                Some(ConnectionError::IO(io_err)) => {
                                    diag::CONN_CLOSED_IO.fetch_add(1, Ordering::Relaxed);
                                    let bucket = classify_io_error(&io_err);
                                    if let Ok(mut map) = diag::CONN_CLOSED_IO_DETAIL.lock() {
                                        *map.entry(bucket).or_insert(0) += 1;
                                    }
                                }
                                Some(ConnectionError::KeepAliveTimeout) => {
                                    diag::CONN_CLOSED_KEEPALIVE.fetch_add(1, Ordering::Relaxed);
                                }
                                None => {
                                    diag::CONN_CLOSED_CLEAN.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            // Wake in-flight push/fetch tasks so they
                            // bail out immediately rather than waiting
                            // up to `--timeout` (default 10 s) for
                            // their pushsync substreams to error.
                            // `notify_waiters()` (NOT `notify_one()`)
                            // wakes every currently-awaiting task —
                            // we may have dozens of concurrent pushes
                            // mid-stream on this session.
                            self.state
                                .connection_dead
                                .store(true, Ordering::Relaxed);
                            self.state.dead_notify.notify_waiters();
                            // Also stop the driver from accepting new
                            // commands. Without this, the dispatcher
                            // may queue a new push onto a known-dead
                            // session if the snapshot used here
                            // hasn't refreshed yet.
                            accept_new = false;
                        }
                    }
                }
            }

            // Once we've stopped accepting new commands and drained all
            // in-flight tasks, the session is fully retired.
            if !accept_new && tasks.is_empty() {
                break;
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_session_driver(driver: SessionDriver) {
    tokio::spawn(driver.run());
}

#[cfg(target_arch = "wasm32")]
fn spawn_session_driver(driver: SessionDriver) {
    wasm_bindgen_futures::spawn_local(driver.run());
}

/// Poll the swarm in parallel with `fut` so that libp2p behaviours
/// (identify, libp2p-stream) can make progress while we wait on stream IO.
async fn poll_until<T, F: core::future::Future<Output = T>>(
    swarm: &mut Swarm<Behaviour>,
    fut: F,
) -> T {
    tokio::pin!(fut);
    loop {
        tokio::select! {
            r = &mut fut => return r,
            _ = swarm.select_next_some() => {}
        }
    }
}

async fn close_stream_polled<S: futures::AsyncWrite + Unpin>(
    swarm: &mut Swarm<Behaviour>,
    stream: &mut S,
) {
    use futures::AsyncWriteExt;
    let _ = poll_until(swarm, stream.close()).await;
}

fn ensure_ws(ma: &Multiaddr) -> Result<PeerId, TransportError> {
    if !is_dialable_multiaddr(ma) {
        return Err(TransportError::NotWebSocket(ma.to_string()));
    }
    extract_peer_id(ma).ok_or(TransportError::MissingPeerId)
}

fn extract_peer_id(ma: &Multiaddr) -> Option<PeerId> {
    use libp2p::multiaddr::Protocol;
    for proto in ma.iter() {
        if let Protocol::P2p(pid) = proto {
            return Some(pid);
        }
    }
    None
}

fn dial(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    peer_addr: &Multiaddr,
) -> Result<(), TransportError> {
    // On wasm, rewrite bee AutoTLS underlays into a /dns4/<sni>/tcp/<port>/tls/ws
    // form so `websocket_websys` dials `wss://<sni-host>` and the *browser*
    // terminates TLS (it can't open raw TCP or do userspace TLS).
    #[cfg(target_arch = "wasm32")]
    let rewritten = crate::dnsaddr::browser_ws_addr(peer_addr);
    #[cfg(target_arch = "wasm32")]
    let peer_addr = &rewritten;
    swarm
        .dial(
            DialOpts::peer_id(peer_id)
                .addresses(vec![peer_addr.clone()])
                .build(),
        )
        .map_err(|e| TransportError::DialFailed(e.to_string()))
}

fn accept(
    control: &mut crate::protocols::stream_pool::Control,
    proto: StreamProtocol,
) -> Result<crate::protocols::stream_pool::IncomingStreams, TransportError> {
    control
        .accept(proto)
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))
}

/// Wait for the connection to establish, the inbound identify exchange to land,
/// add the observed external address, push our updated identify info back, and
/// wait for the push to complete. This is the magic that makes bee proceed
/// immediately instead of waiting ~10s for our liveness signal. Returns the
/// remote underlay multiaddr observed at connection time.
async fn prep_connection(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    timeout: Duration,
) -> Result<Multiaddr, TransportError> {
    let deadline = web_time::Instant::now() + timeout;
    let mut peer_underlay: Option<Multiaddr> = None;
    let mut identify_received = false;
    let mut push_in_flight = false;
    loop {
        let now = web_time::Instant::now();
        if now >= deadline {
            return Err(TransportError::Timeout);
        }
        match tokio::time::timeout(deadline - now, swarm.select_next_some()).await {
            Err(_) => return Err(TransportError::Timeout),
            Ok(ev) => match ev {
                SwarmEvent::ConnectionEstablished {
                    peer_id: pid,
                    endpoint,
                    ..
                } if pid == peer_id => {
                    info!(target: "hoverfly::transport", "connected to {}", pid);
                    peer_underlay = Some(endpoint.get_remote_address().clone());
                }
                SwarmEvent::OutgoingConnectionError {
                    peer_id: Some(pid),
                    error,
                    ..
                } if pid == peer_id => {
                    return Err(TransportError::DialFailed(error.to_string()));
                }
                SwarmEvent::ConnectionClosed { peer_id: pid, .. } if pid == peer_id => {
                    return Err(TransportError::ConnectionClosed);
                }
                SwarmEvent::Behaviour(BehaviourEvent::Identify(idev)) => match idev {
                    libp2p::identify::Event::Received {
                        peer_id: pid, info, ..
                    } if pid == peer_id && !identify_received => {
                        identify_received = true;
                        info!(target: "hoverfly::transport", "identify received; observed_addr={}", info.observed_addr);
                        swarm.add_external_address(info.observed_addr.clone());
                        swarm.behaviour_mut().identify.push([peer_id]);
                        push_in_flight = true;
                    }
                    libp2p::identify::Event::Pushed { peer_id: pid, .. }
                        if pid == peer_id && push_in_flight =>
                    {
                        info!(target: "hoverfly::transport", "identify push acknowledged");
                        return Ok(peer_underlay.unwrap_or_else(Multiaddr::empty));
                    }
                    _ => {}
                },
                _ => {}
            },
        }
    }
}

async fn do_handshake(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut crate::protocols::stream_pool::Control,
    hs_in_v15: &mut crate::protocols::stream_pool::IncomingStreams,
    hs_in_v14: &mut crate::protocols::stream_pool::IncomingStreams,
    underlay: &Multiaddr,
    signer: &SwarmSigner,
    advertised: Option<&Multiaddr>,
) -> Result<handshake::HandshakeResult, TransportError> {
    let local_peer_id = *swarm.local_peer_id();
    info!(target: "hoverfly::transport", "opening outbound handshake");

    // Try v15 first (bee 2.8.0+). Bee ≤ 2.7.x's libp2p multistream
    // negotiation will reject the v15 protocol id with
    // `UnsupportedProtocol`; on that specific failure we fall back to
    // v14. Any other open_stream error (connection closed, dial
    // cooldown, etc.) bubbles up — those aren't version-related.
    let (mut stream, version) =
        match poll_until(swarm, control.open_stream(peer_id, HANDSHAKE_PROTO_V15)).await {
            Ok(s) => (s, handshake::Version::V15),
            Err(e) if is_unsupported_protocol(&e) => {
                debug!(target: "hoverfly::transport",
                    "peer rejected handshake/15.0.0, falling back to 14.0.0");
                let s = poll_until(swarm, control.open_stream(peer_id, HANDSHAKE_PROTO_V14))
                    .await
                    .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
                (s, handshake::Version::V14)
            }
            Err(e) => return Err(TransportError::StreamControl(format!("{e:?}"))),
        };

    let hs_result = {
        let hs_run = handshake::run(
            &mut stream,
            signer,
            signer.network_id(),
            underlay,
            advertised,
            &local_peer_id,
            // Advertise as full_node. Tried light_node empirically TWICE:
            //
            // Attempt 1 (May 2026, kept the full `REFRESH_RATE_PLUR=4.5M`):
            // pool filled in ~300 ms (bee skips bin-saturation for
            // lights) and then collapsed inside 30s as peers blocklisted
            // us for over-debt — our outstanding balance (sized for full
            // node) immediately overshot bee's 1.125M light disconnect
            // limit.
            //
            // Attempt 2 (also May 2026, this time WITH the matched light
            // `REFRESH_RATE_PLUR=450K` + `SAFE_PEER_THRESHOLD_PLUR=900K`):
            // pool fills fast (<1s, confirms Pick bypass works), but
            // upload throughput regresses 5-6× (30-40 KiB/s vs the
            // full-node ~194 KiB/s baseline). The light refresh rate
            // (450K/s of credit per peer per second) is genuinely the
            // binding constraint at our buffer=128 concurrency: the
            // dispatcher concentrates pushes on top-PO peers, those
            // peers exhaust their per-second credit, and the dispatcher
            // rotates through credit-saturated peers with ~3× more
            // overdrafts than full-node mode.
            //
            // The fundamental trade-off: light mode bypasses the bin
            // saturation gate (`Kad.Pick` at kademlia.go:1135-1149
            // short-circuits to true for !FullNode) but accepts a 10×
            // narrower accounting budget. For our high-concurrency
            // upload workload the budget is the worse constraint.
            //
            // Conclusion: stay full_node=true. The earlier comment that
            // "the dialer forces" was wrong (Pick IS called against us
            // in the inbound-handshake handler at handshake.go:355), but
            // light_node is still net-worse for throughput.
            true,
            version,
        );
        // Run handshake while still draining inbound handshake/swarm
        // events. Inbound dial-back can land on EITHER substream id
        // (the peer chooses; usually matches the outbound version we
        // just used) so we poll both inbound channels.
        tokio::pin!(hs_run);
        loop {
            tokio::select! {
                r = &mut hs_run => { break r?; }
                ev = hs_in_v15.next() => {
                    if let Some((pid, mut s)) = ev {
                        if pid == peer_id {
                            let _ = poll_until(swarm,
                                respond_to_handshake(&mut s, signer, None, advertised, &local_peer_id, pid, true, handshake::Version::V15)
                            ).await;
                        }
                    }
                }
                ev = hs_in_v14.next() => {
                    if let Some((pid, mut s)) = ev {
                        if pid == peer_id {
                            let _ = poll_until(swarm,
                                respond_to_handshake(&mut s, signer, None, advertised, &local_peer_id, pid, true, handshake::Version::V14)
                            ).await;
                        }
                    }
                }
                _ = swarm.select_next_some() => {}
            }
        }
    };
    close_stream_polled(swarm, &mut stream).await;
    info!(target: "hoverfly::transport",
        "outbound handshake complete (version={:?})", hs_result.version);
    Ok(hs_result)
}

/// Heuristic: detect the "remote rejected our protocol id" error so
/// the version negotiator can fall back without misclassifying other
/// errors as version-mismatch. libp2p's multistream surface stringifies
/// `UnsupportedProtocol` consistently inside the error chain.
fn is_unsupported_protocol(err: &impl std::fmt::Debug) -> bool {
    let s = format!("{err:?}");
    s.contains("UnsupportedProtocol")
}

async fn do_pricing(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut crate::protocols::stream_pool::Control,
    pr_in: &mut crate::protocols::stream_pool::IncomingStreams,
    timeout: Duration,
) -> Result<u128, TransportError> {
    // Wait for inbound pricing first (peer announces threshold), then announce ours.
    let deadline = web_time::Instant::now() + timeout;
    let mut pr_in_done = false;
    let mut peer_threshold: u128 = u128::from(SAFE_PEER_THRESHOLD_PLUR);
    while !pr_in_done {
        let now = web_time::Instant::now();
        if now >= deadline {
            return Err(TransportError::Timeout);
        }
        tokio::select! {
            _ = tokio::time::sleep(deadline - now) => return Err(TransportError::Timeout),
            ev = pr_in.next() => {
                if let Some((pid, mut stream)) = ev {
                    if pid == peer_id {
                        let _ = poll_until(swarm, read_then_write_empty_headers(&mut stream)).await;
                        if let Ok(threshold) =
                            poll_until(swarm, pricing::read_announcement(&mut stream)).await
                        {
                            peer_threshold = threshold;
                        }
                        pr_in_done = true;
                    }
                }
            }
            _ = swarm.select_next_some() => {}
        }
    }
    info!(target: "hoverfly::transport", "opening outbound pricing");
    let mut stream = poll_until(swarm, control.open_stream(peer_id, PRICING_PROTO))
        .await
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    poll_until(swarm, write_then_read_empty_headers(&mut stream)).await?;
    poll_until(swarm, pricing::announce(&mut stream)).await?;
    close_stream_polled(swarm, &mut stream).await;
    info!(target: "hoverfly::transport",
        "outbound pricing complete (peer threshold {} PLUR)", peer_threshold);
    Ok(peer_threshold)
}

/// One-shot outbound SWAP handshake. Open `/swarm/swap/1.0.0/swap`,
/// exchange empty headers, send `Handshake { Beneficiary }`, close.
/// Bee's `swapprotocol.go::init` is called on every new connection
/// regardless of who dialed; bee's own outbound handshake to us is
/// served by the inbound stream-pool listener for `SWAP_PROTO` (which
/// we accept and discard, since this client is send-only — see
/// PERFORMANCE.md SWAP scope).
#[cfg(not(target_arch = "wasm32"))]
async fn do_swap_handshake(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut crate::protocols::stream_pool::Control,
    timeout: Duration,
    beneficiary: &[u8; 20],
) -> Result<(), TransportError> {
    use crate::protocols::swap;
    let mut stream = tokio::time::timeout(
        timeout,
        poll_until(swarm, control.open_stream(peer_id, SWAP_PROTO)),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    tokio::time::timeout(
        timeout,
        poll_until(swarm, swap::send_handshake(&mut stream, beneficiary)),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(|e| TransportError::Swap(e.to_string()))?;
    close_stream_polled(swarm, &mut stream).await;
    Ok(())
}

/// Open a fresh `/swarm/hive/1.1.0/peers` substream to `peer_id` and
/// announce ourselves via a `Peers` envelope. Best-effort — bee's
/// `peersHandler` reads the envelope, then bee verifies the
/// advertised address asynchronously via its reacher's dial-probe.
/// We don't wait for any acknowledgement; bee's hive doesn't reply.
///
/// See `protocols::hive::announce_self` for the wire-level detail
/// (empty-headers preamble, then one `Peers` message).
#[cfg(not(target_arch = "wasm32"))]
async fn do_hive_announce(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut crate::protocols::stream_pool::Control,
    timeout: Duration,
    me: &crate::protocols::hive::OwnBzzAddress,
    version: crate::protocols::hive::Version,
) -> Result<(), TransportError> {
    // `version` must match the handshake we performed — bee derives
    // each peer's signed BzzAddress format from the negotiated
    // handshake version, and our v2 sign payload includes timestamp
    // + chequebook fields that don't exist in v1. Mixing versions
    // would produce a signature bee rejects as invalid.
    let proto = match version {
        crate::protocols::hive::Version::V1 => HIVE_PROTO_V1,
        crate::protocols::hive::Version::V2 => HIVE_PROTO_V2,
    };
    let mut stream = tokio::time::timeout(
        timeout,
        poll_until(swarm, control.open_stream(peer_id, proto)),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    tokio::time::timeout(
        timeout,
        poll_until(
            swarm,
            crate::protocols::hive::announce_self(&mut stream, me, version),
        ),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(TransportError::Hive)?;
    close_stream_polled(swarm, &mut stream).await;
    Ok(())
}

pub(crate) async fn respond_to_handshake<S>(
    stream: &mut S,
    signer: &SwarmSigner,
    observed_underlay: Option<&Multiaddr>,
    advertised: Option<&Multiaddr>,
    our_peer_id: &PeerId,
    remote_peer_id: PeerId,
    full_node: bool,
    version: handshake::Version,
) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::handshake as pb;
    use crate::protocols::framing::{read_message, write_message};

    let _syn: pb::Syn = read_message(stream).await?;
    let _ = observed_underlay;
    let our_underlay = match advertised {
        Some(ma) => ma.to_vec(),
        None => {
            let s = format!("/ip4/127.0.0.1/tcp/1634/p2p/{our_peer_id}");
            s.parse::<Multiaddr>().unwrap().to_vec()
        }
    };
    // Bee (the dialer) verifies that the `observed_underlay` we put in
    // our SynAck contains *bee's* own peer-id — proving we know who
    // we're talking to. The address portion is parsed but not
    // validated against the connection, so any well-formed multiaddr
    // ending in `/p2p/<bee_peer_id>` is sufficient. See bee's
    // `pkg/p2p/libp2p/internal/handshake/handshake.go::Handshake` for
    // the `libp2pID != observedUnderlayAddrInfo.ID` check.
    let peer_observed = format!("/ip4/0.0.0.0/tcp/0/p2p/{remote_peer_id}")
        .parse::<Multiaddr>()
        .expect("synthetic peer observed multiaddr is valid")
        .to_vec();

    // Light client: no chequebook on chain. Bee skips the chequebook
    // gate when full_node=false anyway.
    let our_chequebook = [0u8; 20];

    let our_addr = match version {
        handshake::Version::V14 => {
            let signature = signer.sign_handshake(&our_underlay)?;
            pb::BzzAddress {
                underlay: our_underlay,
                signature: signature.to_vec(),
                overlay: signer.overlay().to_vec(),
                nonce: Vec::new(),
                timestamp: 0,
                chequebook_address: Vec::new(),
            }
        }
        handshake::Version::V15 => {
            // Use the cached `(timestamp, signature)` pair so inbound
            // and outbound handshakes for the same `(underlay,
            // chequebook)` present byte-identical records to bee. See
            // `SwarmSigner::sign_handshake_v15_cached` for the
            // gossip-update-interval rationale.
            let (timestamp, signature) =
                signer.sign_handshake_v15_cached(&our_underlay, &our_chequebook)?;
            pb::BzzAddress {
                underlay: our_underlay,
                signature: signature.to_vec(),
                overlay: signer.overlay().to_vec(),
                nonce: signer.nonce().to_vec(),
                timestamp,
                chequebook_address: our_chequebook.to_vec(),
            }
        }
    };
    let synack = pb::SynAck {
        syn: Some(pb::Syn {
            observed_underlay: peer_observed,
        }),
        ack: Some(pb::Ack {
            address: Some(our_addr),
            network_id: signer.network_id(),
            full_node,
            nonce: signer.nonce().to_vec(),
            welcome_message: String::new(),
        }),
    };
    write_message(stream, &synack).await?;
    let _peer_ack: pb::Ack = read_message(stream).await?;
    Ok(())
}

async fn write_then_read_empty_headers<S>(stream: &mut S) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::headers as hdr;
    use crate::protocols::framing::{read_message, write_message};
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let _: hdr::Headers = read_message(stream).await?;
    Ok(())
}

async fn read_then_write_empty_headers<S>(stream: &mut S) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::headers as hdr;
    use crate::protocols::framing::{read_message, write_message};
    let _: hdr::Headers = read_message(stream).await?;
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn build_swarm(t: &Transport) -> Result<Swarm<Behaviour>, TransportError> {
    let mut swarm = build_swarm_from(
        &t.keypair,
        t.config.timeout,
        t.config.max_concurrent_substream_upgrades,
    )
    .await?;
    // When the caller has set an advertise address (daemon serving
    // mode), push it as an external address on every outbound swarm so
    // our libp2p identify message tells the bee peer "you can dial me
    // back at <advertise>" — exactly what bee needs to add us to its
    // kademlia routing table after the verification dial-back.
    if let Some(addr) = t.config.advertise.as_ref() {
        swarm.add_external_address(addr.clone());
    }
    Ok(swarm)
}

/// Build a libp2p swarm with the standard hoverfly transport stack
/// (plain TCP + TCP-over-WS, noise auth, yamux multiplex, identify +
/// libp2p_stream behaviours). Exposed `pub(crate)` so the daemon's
/// inbound listener can share the same code path with a separately
/// owned keypair.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn build_swarm_from(
    keypair: &Keypair,
    timeout: Duration,
    max_concurrent_substream_upgrades: usize,
) -> Result<Swarm<Behaviour>, TransportError> {
    use libp2p_core::{Transport as _, upgrade};

    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_other_transport(|key| {
            // Plain TCP for `/ip4/.../tcp/.../p2p/...` (mainnet bootnodes).
            let tcp_plain = libp2p_tcp::tokio::Transport::new(libp2p_tcp::Config::default())
                .upgrade(upgrade::Version::V1)
                .authenticate(noise::Config::new(key).map_err(|e| {
                    libp2p::TransportError::Other(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?)
                .multiplex(yamux::Config::default())
                .boxed();

            // TCP-over-WebSocket for `/ip4/.../tcp/.../ws[s]/p2p/...`
            // (testnet bees that expose ws).
            let tcp_for_ws = libp2p_tcp::tokio::Transport::new(libp2p_tcp::Config::default());
            let ws = libp2p_websocket::Config::new(tcp_for_ws)
                .upgrade(upgrade::Version::V1)
                .authenticate(noise::Config::new(key).map_err(|e| {
                    libp2p::TransportError::Other(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?)
                .multiplex(yamux::Config::default())
                .boxed();

            // libp2p picks the right transport based on the multiaddr's
            // protocol stack (presence of `/ws` or `/wss`). `or_transport`
            // wraps the output in `Either`; map it back to a uniform
            // `(PeerId, StreamMuxerBox)` for SwarmBuilder.
            use futures::future::Either;
            Ok(ws
                .or_transport(tcp_plain)
                .map(|either, _| match either {
                    Either::Left(x) => x,
                    Either::Right(x) => x,
                })
                .boxed())
        })
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_behaviour(|key| behaviour(key, max_concurrent_substream_upgrades))
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();
    Ok(swarm)
}

#[cfg(target_arch = "wasm32")]
async fn build_swarm(t: &Transport) -> Result<Swarm<Behaviour>, TransportError> {
    // Vendored + patched websocket-websys (see src/wsws): sends non-shared
    // buffers so `WebSocket.send()` works under the shared-memory wasm build.
    use crate::wsws as ws_websys;
    use libp2p_core::{Transport as _, upgrade};

    let keypair = t.keypair.clone();
    let timeout = t.config.timeout;
    let max_concurrent_substream_upgrades = t.config.max_concurrent_substream_upgrades;

    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_wasm_bindgen()
        .with_other_transport(|key| {
            Ok(ws_websys::Transport::default()
                .upgrade(upgrade::Version::V1)
                .authenticate(noise::Config::new(key).map_err(|e| {
                    libp2p::TransportError::Other(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?)
                .multiplex(yamux::Config::default())
                .boxed())
        })
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_behaviour(|key| behaviour(key, max_concurrent_substream_upgrades))
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();
    Ok(swarm)
}
