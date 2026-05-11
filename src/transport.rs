//! libp2p transport built around bee's bidirectional handshake/pricing dance.
//!
//! Each public method (`fetch_chunk`, `pushsync_chunk`, `discover_peers`)
//! builds a fresh `Swarm`, dials the peer, drives the bidirectional
//! handshake + pricing exchange, then performs its operation and drops the
//! swarm. The transport only accepts ws/wss multiaddrs.

use core::time::Duration;
use futures::StreamExt;
use libp2p::{
    identity::Keypair,
    noise,
    swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmEvent},
    yamux, Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
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
use crate::signer::{SignerError, SwarmSigner};

const HANDSHAKE_PROTO: StreamProtocol = StreamProtocol::new("/swarm/handshake/14.0.0/handshake");
const PRICING_PROTO: StreamProtocol = StreamProtocol::new("/swarm/pricing/1.0.0/pricing");
const HIVE_PROTO: StreamProtocol = StreamProtocol::new("/swarm/hive/1.1.0/peers");
const RETRIEVAL_PROTO: StreamProtocol = StreamProtocol::new("/swarm/retrieval/1.4.0/retrieval");
const PUSHSYNC_PROTO: StreamProtocol = StreamProtocol::new("/swarm/pushsync/1.3.1/pushsync");
const PSEUDOSETTLE_PROTO: StreamProtocol =
    StreamProtocol::new("/swarm/pseudosettle/1.0.0/pseudosettle");

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
    /// Reserve succeeded, push delivered, peer acknowledged.
    Receipt(PushsyncReceipt),
    /// Reserve would have exceeded the peer's threshold even after an
    /// in-line settlement attempt. The push was not made; try a different
    /// peer or wait for refresh to free credit.
    Overdraft,
}

/// Heuristic: does this error mean the underlying libp2p connection is
/// dead and the caller should rotate to a fresh session? `Pushsync::Peer`
/// errors come from bee's pushsync handler returning a `Receipt{err}` —
/// the connection is fine. Frame / stream-control / IO / explicit
/// `ConnectionClosed` errors all indicate the swarm is gone.
pub fn is_connection_dead(e: &TransportError) -> bool {
    use crate::protocols::pushsync::PushsyncError;
    match e {
        TransportError::ConnectionClosed => true,
        TransportError::StreamControl(_) => true,
        TransportError::Framing(_) => true,
        TransportError::Pushsync(PushsyncError::Frame(_)) => true,
        TransportError::Timeout => true,
        _ => false,
    }
}

/// Maximum pushes a single libp2p connection handles before retiring.
///
/// Why: bee's `accounting.go` keeps a per-overlay `ghostBalance`. Every
/// push that bee can't forward (e.g. its neighbours all reject the chunk)
/// calls `debitAction.Cleanup()` which adds the chunk's price to
/// ghostBalance. Once `ghostBalance > disconnectLimit` (~16.875M PLUR),
/// bee blocklists our overlay and tears the connection down. The only
/// thing that resets ghostBalance is `Connect()` — i.e. a fresh libp2p
/// connection. So we rotate sessions before bee notices.
///
/// At 300K PLUR per chunk worst-case, 25 failed pushes is ~7.5M ghost,
/// well under the limit even if every push fails.
pub const MAX_PUSHES_PER_SESSION: u32 = 25;

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
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            dial_timeout: Duration::from_secs(3),
            network_id: 1,
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub stream: libp2p_stream::Behaviour,
    pub identify: libp2p::identify::Behaviour,
}

fn behaviour(keypair: &Keypair) -> Behaviour {
    Behaviour {
        stream: libp2p_stream::Behaviour::new(),
        identify: libp2p::identify::Behaviour::new(
            libp2p::identify::Config::new("/swarm/0.1.0".to_string(), keypair.public())
                .with_agent_version(format!("isheika/{}", crate::VERSION)),
        ),
    }
}

pub struct Transport {
    keypair: Keypair,
    signer: SwarmSigner,
    config: TransportConfig,
    /// Shared reachability log: every dial / session open / healthcheck
    /// records its (overlay → success/failure + rtt) here. The CLI drains
    /// the log after an operation completes and writes the observations
    /// back to `peers.json`, so future runs skip recently-failed peers
    /// up-front. Always present so callers don't need to check.
    reachability_log: crate::peers::ReachabilityLog,
}

impl Transport {
    pub fn new(signer: SwarmSigner, config: TransportConfig) -> Self {
        let keypair = Keypair::generate_ed25519();
        Self {
            keypair,
            signer,
            config,
            reachability_log: crate::peers::new_log(),
        }
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
    pub async fn open_session(
        &self,
        peer_addr: &Multiaddr,
    ) -> Result<PeerSession, TransportError> {
        PeerSession::connect(self, peer_addr).await
    }

    /// Discover peers from one node by listening on the hive stream.
    pub async fn discover_peers(
        &self,
        peer_addr: &Multiaddr,
        wait: Duration,
    ) -> Result<Vec<Peer>, TransportError> {
        let peer_id = ensure_ws(peer_addr)?;
        let mut swarm = build_swarm(self).await?;
        let mut control = swarm.behaviour().stream.new_control();
        let mut hs_in = accept(&mut control, HANDSHAKE_PROTO)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let mut hive_in = accept(&mut control, HIVE_PROTO)?;
        dial(&mut swarm, peer_id, peer_addr)?;

        let underlay = prep_connection(&mut swarm, peer_id, self.config.timeout).await?;
        do_handshake(&mut swarm, peer_id, &mut control, &mut hs_in, &underlay, &self.signer).await?;
        do_pricing(&mut swarm, peer_id, &mut control, &mut pr_in, self.config.timeout).await?;

        // Wait for the first hive envelope (bee sends a single one then stops),
        // bounded by `wait`. Exit as soon as we read it.
        let mut peers: Vec<Peer> = Vec::new();
        let deadline = web_time::Instant::now() + wait;
        loop {
            let now = web_time::Instant::now();
            if now >= deadline { break; }
            let remaining = deadline - now;
            tokio::select! {
                _ = tokio::time::sleep(remaining) => break,
                ev = hive_in.next() => {
                    match ev {
                        Some((pid, mut stream)) if pid == peer_id => {
                            info!(target: "isheika::hive", "inbound hive stream opened");
                            match poll_until(&mut swarm, hive::read_peers(&mut stream)).await {
                                Ok(mut batch) => {
                                    info!(target: "isheika::hive", "read {} peers", batch.len());
                                    peers.append(&mut batch);
                                    break;
                                }
                                Err(e) => debug!(target: "isheika::hive", "read_peers err: {}", e),
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
        let mut hs_in = accept(&mut control, HANDSHAKE_PROTO)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let hive_in = accept(&mut control, HIVE_PROTO)?;
        dial(&mut swarm, peer_id, peer_addr)?;
        let underlay = prep_connection(&mut swarm, peer_id, transport.config.dial_timeout).await?;
        do_handshake(
            &mut swarm,
            peer_id,
            &mut control,
            &mut hs_in,
            &underlay,
            &transport.signer,
        )
        .await?;
        do_pricing(
            &mut swarm,
            peer_id,
            &mut control,
            &mut pr_in,
            transport.config.dial_timeout,
        )
        .await?;

        let timeout = transport.config.timeout;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<SessionCommand>(64);
        let state = std::sync::Arc::new(SessionState {
            control,
            peer_id,
            timeout,
            accounting: tokio::sync::Mutex::new(AccountingState {
                reserve_plur: 0,
                balance_plur: 0,
                threshold_plur: SAFE_PEER_THRESHOLD_PLUR,
                last_settle: None,
            }),
            settle_lock: tokio::sync::Mutex::new(()),
            pushes_used: std::sync::atomic::AtomicU32::new(0),
        });
        let session_state = state.clone();
        spawn_session_driver(SessionDriver {
            swarm,
            state,
            cmd_rx,
            _hs_in: hs_in,
            _pr_in: pr_in,
            _hive_in: hive_in,
        });
        Ok(Self { cmd_tx, peer_id, state: session_state })
    }

    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Pushes attempted on this session's underlying connection so far.
    /// Approaches [`MAX_PUSHES_PER_SESSION`]; callers use this watermark
    /// to pre-warm a replacement session before the current one retires.
    pub fn pushes_used(&self) -> u32 {
        self.state
            .pushes_used
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
    control: libp2p_stream::Control,
    peer_id: PeerId,
    timeout: Duration,
    accounting: tokio::sync::Mutex<AccountingState>,
    /// Serializes pseudosettle attempts on this peer — bee rejects
    /// two settles within the same wall-second, and back-to-back
    /// concurrent settles would just both re-settle the same balance.
    settle_lock: tokio::sync::Mutex<()>,
    /// Pushes attempted on this connection so far. When this exceeds
    /// [`MAX_PUSHES_PER_SESSION`] the session is considered "dead" by
    /// the caller and rotated; bee's per-overlay `ghostBalance` resets
    /// only on a fresh `Connect()`. See `MAX_PUSHES_PER_SESSION` docs.
    pushes_used: std::sync::atomic::AtomicU32,
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
        let mut control = self.control.clone();
        let open = tokio::time::timeout(
            self.timeout,
            control.open_stream(self.peer_id, PUSHSYNC_PROTO),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
        let mut stream = open;
        tokio::time::timeout(self.timeout, pushsync::push(&mut stream, addr, wire, stamp))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(Into::into)
    }

    async fn do_fetch(&self, addr: &[u8; 32]) -> Result<ChunkDelivery, TransportError> {
        let mut control = self.control.clone();
        let open = tokio::time::timeout(
            self.timeout,
            control.open_stream(self.peer_id, RETRIEVAL_PROTO),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
        let mut stream = open;
        tokio::time::timeout(self.timeout, retrieval::fetch(&mut stream, addr))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(Into::into)
    }

    /// Issue one pseudosettle Payment. Serialized across concurrent
    /// pushes via `settle_lock`, gated to at most one per 1.1 seconds.
    /// Best-effort: errors are swallowed because failure to settle just
    /// means the next reserve attempt will report overdraft.
    async fn try_settle_once(&self) -> Result<(), TransportError> {
        let _guard = self.settle_lock.lock().await;

        // Bee rejects two settles within the same wall-second.
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
        {
            let mut acc = self.accounting.lock().await;
            acc.last_settle = Some(web_time::Instant::now());
            acc.balance_plur = acc.balance_plur.saturating_sub(accepted);
            debug!(
                target: "isheika::transport",
                "settled with {}: asked={} accepted={} balance={} reserve={}",
                self.peer_id, owed, accepted, acc.balance_plur, acc.reserve_plur,
            );
        }
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
    _hs_in: libp2p_stream::IncomingStreams,
    _pr_in: libp2p_stream::IncomingStreams,
    _hive_in: libp2p_stream::IncomingStreams,
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
                            if used >= MAX_PUSHES_PER_SESSION {
                                debug!(target: "isheika::transport",
                                    "session {} retiring after {} pushes \
                                     (avoiding bee ghostBalance disconnect)",
                                    self.state.peer_id, used);
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
                    }
                }

                Some(dead) = tasks.next(), if !tasks.is_empty() => {
                    if dead {
                        debug!(target: "isheika::transport",
                            "session {} retiring: underlying connection dead",
                            self.state.peer_id);
                        accept_new = false;
                    }
                }

                _ = self.swarm.select_next_some() => {}
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

/// Like `poll_until` but enforces an overall timeout on the inner future.
async fn poll_until_timeout<T, F: core::future::Future<Output = T>>(
    swarm: &mut Swarm<Behaviour>,
    fut: F,
    timeout: Duration,
) -> Result<T, TransportError> {
    tokio::pin!(fut);
    let sleep = tokio::time::sleep(timeout);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            r = &mut fut => return Ok(r),
            _ = &mut sleep => return Err(TransportError::Timeout),
            _ = swarm.select_next_some() => {}
        }
    }
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

async fn close_stream_polled<S: futures::AsyncWrite + Unpin>(swarm: &mut Swarm<Behaviour>, stream: &mut S) {
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

fn dial(swarm: &mut Swarm<Behaviour>, peer_id: PeerId, peer_addr: &Multiaddr) -> Result<(), TransportError> {
    swarm
        .dial(
            DialOpts::peer_id(peer_id)
                .addresses(vec![peer_addr.clone()])
                .build(),
        )
        .map_err(|e| TransportError::DialFailed(e.to_string()))
}

fn accept(
    control: &mut libp2p_stream::Control,
    proto: StreamProtocol,
) -> Result<libp2p_stream::IncomingStreams, TransportError> {
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
                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } if pid == peer_id => {
                    info!(target: "isheika::transport", "connected to {}", pid);
                    peer_underlay = Some(endpoint.get_remote_address().clone());
                }
                SwarmEvent::OutgoingConnectionError { peer_id: Some(pid), error, .. } if pid == peer_id => {
                    return Err(TransportError::DialFailed(error.to_string()));
                }
                SwarmEvent::ConnectionClosed { peer_id: pid, .. } if pid == peer_id => {
                    return Err(TransportError::ConnectionClosed);
                }
                SwarmEvent::Behaviour(BehaviourEvent::Identify(idev)) => match idev {
                    libp2p::identify::Event::Received { peer_id: pid, info, .. } if pid == peer_id && !identify_received => {
                        identify_received = true;
                        info!(target: "isheika::transport", "identify received; observed_addr={}", info.observed_addr);
                        swarm.add_external_address(info.observed_addr.clone());
                        swarm.behaviour_mut().identify.push([peer_id]);
                        push_in_flight = true;
                    }
                    libp2p::identify::Event::Pushed { peer_id: pid, .. } if pid == peer_id && push_in_flight => {
                        info!(target: "isheika::transport", "identify push acknowledged");
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
    control: &mut libp2p_stream::Control,
    hs_in: &mut libp2p_stream::IncomingStreams,
    underlay: &Multiaddr,
    signer: &SwarmSigner,
) -> Result<(), TransportError> {
    let local_peer_id = *swarm.local_peer_id();
    info!(target: "isheika::transport", "opening outbound handshake");
    let mut stream = poll_until(swarm, control.open_stream(peer_id, HANDSHAKE_PROTO))
        .await
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    {
        let hs_run = handshake::run(&mut stream, signer, signer.network_id(), underlay, &local_peer_id, true);
        // Run handshake while still draining inbound handshake/swarm events.
        tokio::pin!(hs_run);
        loop {
            tokio::select! {
                r = &mut hs_run => { r?; break; }
                ev = hs_in.next() => {
                    if let Some((pid, mut s)) = ev {
                        if pid == peer_id {
                            let _ = poll_until(swarm,
                                respond_to_handshake(&mut s, signer, None, &local_peer_id)
                            ).await;
                        }
                    }
                }
                _ = swarm.select_next_some() => {}
            }
        }
    }
    close_stream_polled(swarm, &mut stream).await;
    info!(target: "isheika::transport", "outbound handshake complete");
    Ok(())
}

async fn do_pricing(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut libp2p_stream::Control,
    pr_in: &mut libp2p_stream::IncomingStreams,
    timeout: Duration,
) -> Result<(), TransportError> {
    // Wait for inbound pricing first (peer announces threshold), then announce ours.
    let deadline = web_time::Instant::now() + timeout;
    let mut pr_in_done = false;
    while !pr_in_done {
        let now = web_time::Instant::now();
        if now >= deadline { return Err(TransportError::Timeout); }
        tokio::select! {
            _ = tokio::time::sleep(deadline - now) => return Err(TransportError::Timeout),
            ev = pr_in.next() => {
                if let Some((pid, mut stream)) = ev {
                    if pid == peer_id {
                        let _ = poll_until(swarm, read_then_write_empty_headers(&mut stream)).await;
                        let _ = poll_until(swarm, pricing::read_announcement(&mut stream)).await;
                        pr_in_done = true;
                    }
                }
            }
            _ = swarm.select_next_some() => {}
        }
    }
    info!(target: "isheika::transport", "opening outbound pricing");
    let mut stream = poll_until(swarm, control.open_stream(peer_id, PRICING_PROTO))
        .await
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    poll_until(swarm, write_then_read_empty_headers(&mut stream)).await?;
    poll_until(swarm, pricing::announce(&mut stream)).await?;
    close_stream_polled(swarm, &mut stream).await;
    info!(target: "isheika::transport", "outbound pricing complete");
    Ok(())
}

async fn respond_to_handshake<S>(
    stream: &mut S,
    signer: &SwarmSigner,
    observed_underlay: Option<&Multiaddr>,
    our_peer_id: &PeerId,
) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::handshake as pb;
    use crate::protocols::framing::{read_message, write_message};

    let syn: pb::Syn = read_message(stream).await?;
    let _ = observed_underlay; // ignored — clients don't listen
    let our_underlay = {
        let s = format!("/ip4/127.0.0.1/tcp/1634/p2p/{our_peer_id}");
        s.parse::<Multiaddr>().unwrap().to_vec()
    };
    let signature = signer.sign_handshake(&our_underlay)?;
    let our_addr = pb::BzzAddress {
        underlay: our_underlay,
        signature: signature.to_vec(),
        overlay: signer.overlay().to_vec(),
    };
    let synack = pb::SynAck {
        syn: Some(pb::Syn { observed_underlay: syn.observed_underlay }),
        ack: Some(pb::Ack {
            address: Some(our_addr),
            network_id: signer.network_id(),
            full_node: true,
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

async fn close_stream<S: futures::AsyncWrite + Unpin>(stream: &mut S) {
    use futures::AsyncWriteExt;
    let _ = stream.close().await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn build_swarm(t: &Transport) -> Result<Swarm<Behaviour>, TransportError> {
    use libp2p_core::{upgrade, Transport as _};

    let timeout = t.config.timeout;

    let swarm = SwarmBuilder::with_existing_identity(t.keypair.clone())
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
        .with_behaviour(|key| behaviour(key))
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();
    Ok(swarm)
}

#[cfg(target_arch = "wasm32")]
async fn build_swarm(t: &Transport) -> Result<Swarm<Behaviour>, TransportError> {
    use libp2p_core::{upgrade, Transport as _};
    use libp2p::websocket_websys as ws_websys;

    let keypair = t.keypair.clone();
    let timeout = t.config.timeout;

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
        .with_behaviour(|key| behaviour(key))
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();
    Ok(swarm)
}
