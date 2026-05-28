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

    /// `true` if this peer's last dial attempt failed within the recent
    /// failure window AND we haven't had a successful dial since.
    pub fn is_recently_unreachable(&self, now_unix: u64) -> bool {
        let Some(fail) = self.last_dial_failure_unix else {
            return false;
        };
        if let Some(succ) = self.last_dial_success_unix {
            if succ >= fail {
                return false;
            }
        }
        now_unix.saturating_sub(fail) < RECENT_FAILURE_SECS
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
    #[cfg(not(target_arch = "wasm32"))]
    {
        is_ws(ma) || (ma.contains("/tcp/") && !is_ws(ma))
    }
    #[cfg(target_arch = "wasm32")]
    {
        is_ws(ma)
    }
}
