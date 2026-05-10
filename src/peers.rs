//! In-memory + (native) JSON-backed peer store.
//!
//! WS-only filtering: only peers with at least one ws/wss multiaddr are accepted.

use libp2p::Multiaddr;
use nectar_primitives::address::SwarmAddress;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
}

impl Peer {
    pub fn first_underlay(&self) -> Option<Multiaddr> {
        self.underlays.iter().find_map(|s| s.parse().ok())
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

    /// Insert or merge a peer. Underlays are filtered to ws/wss only and de-duplicated.
    pub fn upsert(&mut self, mut peer: Peer) {
        peer.underlays.retain(|u| is_ws(u));
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
            }
            None => {
                self.peers.insert(key, peer);
            }
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
