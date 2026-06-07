//! In-memory + (native) JSON-backed peer store.
//!
//! Underlay-dialability filter: peers are accepted only if they expose at
//! least one underlay this build can dial. On native that's any `/ip4/.../tcp/`
//! or `/ip4/.../ws[s]/` multiaddr; on `wasm32` (browser builds) it's
//! `/ip4/.../ws[s]/` only — browsers can't open raw TCP sockets. Both
//! variants exclude `/dns*/` and `/ip6/` because we don't ship a DNS resolver
//! and v6 reachability on residential / CI networks is unreliable. See
//! `dnsaddr.rs::is_dialable_multiaddr` for the canonical predicate.

use libp2p::Multiaddr;
use nectar_primitives::address::SwarmAddress;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PeerStoreError {
    #[error("io: {0}")]
    Io(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("multiaddr parse: {0}")]
    Multiaddr(String),
}

/// A discovered peer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Peer {
    /// 32-byte overlay (hex).
    pub overlay: String,
    /// All known underlays (only ws/wss are persisted).
    pub underlays: Vec<String>,
    /// Optional ethereum address (hex, 0x-prefixed).
    #[serde(default)]
    pub eth_address: Option<String>,
    /// 32-byte nonce used to derive overlay (hex). Zero by default.
    #[serde(default)]
    pub nonce: Option<String>,

    /// Reachability cache, updated by dial attempts (session pool open,
    /// healthcheck, etc.). Persists across CLI runs so we don't waste
    /// ~timeout seconds re-dialing peers we know are dead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dial_success_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dial_failure_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dial_rtt_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub consecutive_failures: u16,
}

fn is_zero_u16(x: &u16) -> bool {
    *x == 0
}

/// Window (seconds) within which a recent dial failure causes a peer to
/// be deprioritised. Tuned so that a peer down at the time of one upload
/// is given a chance again on the next run a few minutes later.
pub const RECENT_FAILURE_SECS: u64 = 300;

/// Outcome of a dial attempt, recorded in a [`ReachabilityLog`] so the
/// session-pool dial loop and healthcheck probe can feed observations
/// back to a writable [`PeerStore`] without holding a `&mut` reference
/// across many concurrent dials.
#[derive(Clone, Copy, Debug)]
pub enum DialResult {
    Success { rtt_ms: u32 },
    Failure,
}

/// Thread-safe map of `overlay_hex_lowercase → DialResult` populated by
/// concurrent dial loops. Apply to a [`PeerStore`] with [`apply_log`].
pub type ReachabilityLog =
    std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, DialResult>>>;

pub fn new_log() -> ReachabilityLog {
    std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Drain `log` into `store`, updating each peer's reachability fields.
pub fn apply_log(store: &mut PeerStore, log: &ReachabilityLog) {
    let entries: Vec<(String, DialResult)> = {
        let mut guard = log.lock().unwrap();
        guard.drain().collect()
    };
    for (overlay, result) in entries {
        match result {
            DialResult::Success { rtt_ms } => store.record_dial_success(&overlay, rtt_ms),
            DialResult::Failure => store.record_dial_failure(&overlay),
        }
    }
}

impl Peer {
    pub fn first_underlay(&self) -> Option<Multiaddr> {
        self.underlays.iter().find_map(|s| s.parse().ok())
    }

    /// First parseable ws/wss multiaddr, if any. Hive responses often
    /// list both TCP and ws underlays for the same peer.
    pub fn first_ws_underlay(&self) -> Option<Multiaddr> {
        self.underlays
            .iter()
            .filter(|s| s.contains("/ws") || s.contains("/wss"))
            .find_map(|s| s.parse().ok())
    }

    /// First underlay our transport stack can dial. On native this
    /// returns either a ws or plain-tcp address; on WASM it returns
    /// ws-only. Prefers ws/wss over plain tcp so that browser-portable
    /// nodes in the peerset always pick the ws path when one exists.
    pub fn first_dialable_underlay(&self) -> Option<Multiaddr> {
        if let Some(ws) = self.first_ws_underlay() {
            return Some(ws);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.underlays
                .iter()
                .filter(|s| s.contains("/tcp/") && !is_ws(s))
                .find_map(|s| s.parse().ok())
        }
        #[cfg(target_arch = "wasm32")]
        {
            None
        }
    }

    pub fn overlay_bytes(&self) -> Option<[u8; 32]> {
        let bytes = hex::decode(self.overlay.trim_start_matches("0x")).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(arr)
    }

    pub fn overlay_address(&self) -> Option<SwarmAddress> {
        Some(SwarmAddress::new(self.overlay_bytes()?))
    }

    /// `true` if this peer's last dial attempt failed within its
    /// backoff window AND we haven't had a successful dial since.
    ///
    /// The window grows with `consecutive_failures` (see
    /// [`Self::failure_backoff_secs`]) rather than being a flat
    /// [`RECENT_FAILURE_SECS`]: a peer that failed once (likely a
    /// transient dial timeout / momentary overload) is given another
    /// chance within ~1 min, while a peer that has failed repeatedly
    /// is parked for the full window. This keeps a single bad dial
    /// from exiling an otherwise-good peer from the candidate set for
    /// 5 minutes.
    pub fn is_recently_unreachable(&self, now_unix: u64) -> bool {
        let Some(fail) = self.last_dial_failure_unix else {
            return false;
        };
        if let Some(succ) = self.last_dial_success_unix {
            if succ >= fail {
                return false;
            }
        }
        now_unix.saturating_sub(fail) < self.failure_backoff_secs()
    }

    /// Backoff window (seconds) before a recently-failed peer is
    /// reconsidered, scaled by `consecutive_failures`:
    /// `60 × 2^(min(failures,3))` capped at [`RECENT_FAILURE_SECS`].
    /// So 1 failure → 120 s, 2 → 240 s, 3+ → 300 s. A first failure
    /// recovers far faster than the old flat 300 s.
    fn failure_backoff_secs(&self) -> u64 {
        let shift = u32::from(self.consecutive_failures).min(3);
        (60u64.saturating_mul(1u64 << shift)).min(RECENT_FAILURE_SECS)
    }

    /// Coarse reachability rank for candidate ordering — **lower is
    /// better**. Used to front-load the dial parade (and the session
    /// pool) with peers most likely to answer, mirroring bee's
    /// `topology.Select{Reachable, Healthy}` filter in
    /// `pkg/retrieval` / `pkg/topology/kademlia`. There bee excludes
    /// unreachable/unhealthy peers outright; we don't have continuous
    /// health probes, so we grade by dial history instead and keep
    /// even the worst peers as last-resort candidates.
    ///
    /// - `0` — a successful dial is on record with no more-recent
    ///   failure (known-good).
    /// - `1` — never attempted, or a stale failure outside the
    ///   backoff window (unknown; worth a fresh try).
    /// - `2` — failed within the backoff window, few strikes
    ///   (probably transient).
    /// - `3` — failed within the backoff window with ≥
    ///   [`DEAD_STRIKES`]-many consecutive failures (likely dead).
    pub fn dial_rank(&self, now_unix: u64) -> u8 {
        match (self.last_dial_success_unix, self.last_dial_failure_unix) {
            // Last attempt (or only attempts) succeeded.
            (Some(s), Some(f)) if s >= f => 0,
            (Some(_), None) => 0,
            // Never dialed.
            (None, None) => 1,
            // Last attempt failed (success older than failure, or only failures).
            (_, Some(f)) => {
                if now_unix.saturating_sub(f) >= self.failure_backoff_secs() {
                    1
                } else if self.consecutive_failures >= 3 {
                    3
                } else {
                    2
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(target_arch = "wasm32")]
pub fn now_unix() -> u64 {
    (web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)) as u64
}

/// In-memory peer store keyed by overlay hex.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PeerStore {
    #[serde(default)]
    peers: BTreeMap<String, Peer>,
}

impl PeerStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.values()
    }

    /// Insert or merge a peer. Underlays are filtered to those our transport
    /// stack can dial (ws/wss everywhere, plus plain `/tcp/...` on native).
    /// Reachability fields from `peer` overwrite the existing entry only
    /// if they're newer than what's already stored (so older hive-only
    /// announcements never wipe out recent dial-result observations).
    pub fn upsert(&mut self, mut peer: Peer) {
        peer.underlays.retain(|u| is_dialable_str(u));
        peer.underlays.sort();
        peer.underlays.dedup();
        if peer.underlays.is_empty() {
            return;
        }
        let key = peer.overlay.clone().to_lowercase();
        match self.peers.get_mut(&key) {
            Some(existing) => {
                for u in peer.underlays {
                    if !existing.underlays.contains(&u) {
                        existing.underlays.push(u);
                    }
                }
                existing.underlays.sort();
                existing.underlays.dedup();
                if existing.eth_address.is_none() {
                    existing.eth_address = peer.eth_address;
                }
                if existing.nonce.is_none() {
                    existing.nonce = peer.nonce;
                }
                // Reachability is monotonic on each field — take the newer.
                if peer.last_dial_success_unix > existing.last_dial_success_unix {
                    existing.last_dial_success_unix = peer.last_dial_success_unix;
                    existing.last_dial_rtt_ms = peer.last_dial_rtt_ms;
                    existing.consecutive_failures = 0;
                }
                if peer.last_dial_failure_unix > existing.last_dial_failure_unix {
                    existing.last_dial_failure_unix = peer.last_dial_failure_unix;
                    existing.consecutive_failures = existing
                        .consecutive_failures
                        .saturating_add(peer.consecutive_failures.max(1));
                }
            }
            None => {
                self.peers.insert(key, peer);
            }
        }
    }

    /// Record a successful dial for the peer with this overlay (hex).
    /// No-op if the peer isn't in the store.
    pub fn record_dial_success(&mut self, overlay_hex: &str, rtt_ms: u32) {
        let key = overlay_hex.to_lowercase();
        if let Some(p) = self.peers.get_mut(&key) {
            p.last_dial_success_unix = Some(now_unix());
            p.last_dial_rtt_ms = Some(rtt_ms);
            p.consecutive_failures = 0;
        }
    }

    /// Record a failed dial for the peer with this overlay (hex).
    /// No-op if the peer isn't in the store.
    pub fn record_dial_failure(&mut self, overlay_hex: &str) {
        let key = overlay_hex.to_lowercase();
        if let Some(p) = self.peers.get_mut(&key) {
            p.last_dial_failure_unix = Some(now_unix());
            p.consecutive_failures = p.consecutive_failures.saturating_add(1);
        }
    }

    /// Closest peers to a target address by proximity order (descending).
    pub fn closest(&self, target: &SwarmAddress, limit: usize) -> Vec<&Peer> {
        let mut scored: Vec<(u8, &Peer)> = self
            .peers
            .values()
            .filter_map(|p| {
                let overlay = p.overlay_address()?;
                let po = target.proximity(&overlay);
                Some((po, p))
            })
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));
        scored.into_iter().take(limit).map(|(_, p)| p).collect()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_create<P: AsRef<Path>>(path: P) -> Self {
        match std::fs::read_to_string(&path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), PeerStoreError> {
        let s = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, s).map_err(|e| PeerStoreError::Io(e.to_string()))
    }
}

fn is_ws(ma: &str) -> bool {
    ma.contains("/ws") || ma.contains("/wss")
}

/// Public form of [`is_ws`]: true if the underlay multiaddr carries a
/// `/ws` or `/wss` (WebSocket) hop — the only underlays a browser build can
/// dial. Used by the CLI's `discover --ws-only` to export a browser-ready seed.
pub fn is_ws_underlay(ma: &str) -> bool {
    is_ws(ma)
}

/// True if `ip` is not reachable across the public internet: private
/// (RFC1918), loopback, link-local, unspecified, broadcast, or carrier-grade
/// NAT (100.64.0.0/10). We can never dial these end-to-end.
pub fn is_unroutable_ip4(ip: std::net::Ipv4Addr) -> bool {
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        // 100.64.0.0/10 carrier-grade NAT (also unroutable end-to-end)
        || (ip.octets()[0] == 100 && (ip.octets()[1] & 0xC0) == 0x40)
}

/// Reject an underlay multiaddr (string form) whose `/ip4/<ip>` segment is an
/// unroutable address — see [`is_unroutable_ip4`]. Bee's AutoTLS sometimes
/// advertises a node's *internal* address (e.g. a Kubernetes pod IP
/// `10.233.x.x`), which is unreachable from the public internet — and whose
/// `libp2p.direct` SNI host resolves straight back to that private IP — so
/// every dial to it burns a full dial-timeout for nothing.
pub fn has_unroutable_ip4(ma: &str) -> bool {
    let Some(rest) = ma.split("/ip4/").nth(1) else {
        return false;
    };
    let ip_str = rest.split('/').next().unwrap_or("");
    let Ok(ip) = ip_str.parse::<std::net::Ipv4Addr>() else {
        return false;
    };
    is_unroutable_ip4(ip)
}

/// Cheap string-form check that matches `dnsaddr::is_dialable_multiaddr`
/// without parsing — used in `upsert` because peer entries arrive as
/// `Vec<String>`. Requires `/ip4/`: our transport has no DNS resolver
/// (`/dns4/`, `/dnsaddr/`) and most consumer networks lack outbound
/// IPv6, so filtering at peerlist-ingestion time avoids burning dial
/// timeouts on unreachable underlays later.
fn is_dialable_str(ma: &str) -> bool {
    if !ma.contains("/ip4/") {
        return false;
    }
    // Drop AutoTLS underlays that advertise a private/internal IP (e.g. k8s pod
    // `10.x` addresses) — unroutable, so dialing them only wastes timeouts.
    if has_unroutable_ip4(ma) {
        return false;
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        is_ws(ma) || (ma.contains("/tcp/") && !is_ws(ma))
    }
    #[cfg(target_arch = "wasm32")]
    {
        is_ws(ma)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_000_000;

    fn peer() -> Peer {
        Peer::default()
    }

    #[test]
    fn never_dialed_is_unknown_rank_not_unreachable() {
        let p = peer();
        assert_eq!(p.dial_rank(NOW), 1, "never-dialed peer ranks as unknown");
        assert!(!p.is_recently_unreachable(NOW));
    }

    #[test]
    fn recent_success_is_best_rank() {
        let mut p = peer();
        p.last_dial_success_unix = Some(NOW - 10);
        assert_eq!(p.dial_rank(NOW), 0);
        assert!(!p.is_recently_unreachable(NOW));
    }

    #[test]
    fn success_after_failure_clears_unreachable() {
        let mut p = peer();
        p.last_dial_failure_unix = Some(NOW - 100);
        p.last_dial_success_unix = Some(NOW - 10);
        // A later success outranks the earlier failure.
        assert_eq!(p.dial_rank(NOW), 0);
        assert!(!p.is_recently_unreachable(NOW));
    }

    #[test]
    fn single_failure_recovers_in_two_minutes_not_five() {
        let mut p = peer();
        p.last_dial_failure_unix = Some(NOW);
        p.consecutive_failures = 1;
        // Backoff for 1 failure is 60 * 2^1 = 120s.
        assert!(p.is_recently_unreachable(NOW + 60));
        assert!(!p.is_recently_unreachable(NOW + 121));
        assert_eq!(p.dial_rank(NOW + 60), 2, "soft failure inside window");
        assert_eq!(
            p.dial_rank(NOW + 121),
            1,
            "stale failure outside window is retryable"
        );
    }

    #[test]
    fn many_failures_rank_hard_and_park_longer() {
        let mut p = peer();
        p.last_dial_failure_unix = Some(NOW);
        p.consecutive_failures = 5;
        // Backoff caps at RECENT_FAILURE_SECS (300s) regardless of shift.
        assert!(p.is_recently_unreachable(NOW + 250));
        assert!(!p.is_recently_unreachable(NOW + RECENT_FAILURE_SECS + 1));
        assert_eq!(p.dial_rank(NOW + 10), 3, "hard failure ranks last");
    }

    #[test]
    fn backoff_grows_with_failures() {
        let mut p = peer();
        p.last_dial_failure_unix = Some(NOW);
        p.consecutive_failures = 1;
        assert_eq!(p.failure_backoff_secs(), 120);
        p.consecutive_failures = 2;
        assert_eq!(p.failure_backoff_secs(), 240);
        p.consecutive_failures = 3;
        assert_eq!(p.failure_backoff_secs(), RECENT_FAILURE_SECS); // 480 capped to 300
        p.consecutive_failures = 9;
        assert_eq!(p.failure_backoff_secs(), RECENT_FAILURE_SECS);
    }
}
