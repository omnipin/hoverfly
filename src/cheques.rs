//! Per-peer cumulative-payout state for issued SWAP cheques.
//!
//! Bee's `pkg/settlement/swap/chequestore.go::ReceiveCheque` rejects any
//! cheque whose `CumulativePayout` is **not strictly greater** than the
//! last accepted one from the same chequebook (lines 90-110 of
//! chequestore.go, roughly: `if cheque.CumulativePayout <= last accepted
//! then ErrChequeNotIncreasing`). That means we MUST persist the per-peer
//! cumulative across CLI runs — otherwise a second invocation issues
//! `CumulativePayout = base + amount` starting from 0, which is less
//! than what we already sent in run 1, and every peer rejects us.
//!
//! Persistence shape (`cheques.json`):
//!   {
//!     "version": 1,
//!     "chequebook": "0x...",      // sanity check: we don't reuse this
//!                                  // file with a different chequebook
//!     "peers": { "<overlay_hex>": "<u128 decimal>" }
//!   }
//!
//! Stored as a decimal string because JSON has no `u128` and PLUR/BZZ
//! payouts comfortably overflow `u64` (the BZZ supply is 100 M with
//! 16 decimals → 10^24, ~2^80).
//!
//! Native-only (`cfg(not(target_arch = "wasm32"))`). The wasm build
//! doesn't have a filesystem; if we ever do SWAP from a browser we'll
//! use IndexedDB or LocalStorage with the same logical shape.

#![cfg(not(target_arch = "wasm32"))]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChequeStoreError {
    #[error("io: {0}")]
    Io(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("chequebook mismatch: file has {file}, runtime has {runtime}")]
    ChequebookMismatch { file: String, runtime: String },
    #[error("amount overflows u128")]
    Overflow,
    #[error("decimal parse: {0}")]
    Parse(String),
}

/// On-disk schema. Version-tagged so we can migrate later.
#[derive(Debug, Serialize, Deserialize)]
struct OnDisk {
    version: u32,
    chequebook: String,
    #[serde(default)]
    peers: BTreeMap<String, String>,
}

/// In-memory cheque-issuance state.
///
/// Cloneable + thread-safe via `Arc<Mutex<…>>` at the call site
/// (see transport.rs). The store itself is not internally locked
/// because we only mutate it from the per-session settle path, which
/// already serializes against `SessionState::settle_lock`.
#[derive(Debug, Clone)]
pub struct ChequeStore {
    chequebook: [u8; 20],
    /// `peer_overlay_hex_lowercase -> cumulative_payout_bzz_wei`.
    /// Keyed by overlay rather than Ethereum address because the
    /// only stable identity we have for a remote peer across runs
    /// is its swarm overlay. Bee re-derives our beneficiary (their
    /// Ethereum address) from the BzzAddress signature each time, so
    /// it stays stable too as long as their bee keystore doesn't
    /// rotate.
    payouts: BTreeMap<String, u128>,
    path: Option<PathBuf>,
}

impl ChequeStore {
    pub fn new(chequebook: [u8; 20]) -> Self {
        Self {
            chequebook,
            payouts: BTreeMap::new(),
            path: None,
        }
    }

    pub fn chequebook(&self) -> &[u8; 20] {
        &self.chequebook
    }

    /// Load from disk, or create a fresh empty store if the file is
    /// missing. Returns an error only if the file exists but is for a
    /// different chequebook (programmer / operator error — refuse to
    /// continue rather than overwrite live state).
    pub fn load_or_create<P: AsRef<Path>>(
        path: P,
        chequebook: [u8; 20],
    ) -> Result<Self, ChequeStoreError> {
        let path = path.as_ref().to_path_buf();
        let mut store = Self::new(chequebook);
        store.path = Some(path.clone());

        let text = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(store),
            Err(e) => return Err(ChequeStoreError::Io(e.to_string())),
        };
        let on_disk: OnDisk = serde_json::from_str(&text)?;
        let file_hex = on_disk.chequebook.trim_start_matches("0x").to_lowercase();
        let runtime_hex = hex::encode(chequebook);
        if file_hex != runtime_hex {
            return Err(ChequeStoreError::ChequebookMismatch {
                file: format!("0x{}", file_hex),
                runtime: format!("0x{}", runtime_hex),
            });
        }
        for (k, v) in on_disk.peers {
            let n: u128 = v
                .parse()
                .map_err(|e: std::num::ParseIntError| ChequeStoreError::Parse(e.to_string()))?;
            store.payouts.insert(k.to_lowercase(), n);
        }
        Ok(store)
    }

    /// Return the current cumulative payout we've sent this peer.
    pub fn cumulative(&self, peer_overlay_hex: &str) -> u128 {
        self.payouts
            .get(&peer_overlay_hex.to_lowercase())
            .copied()
            .unwrap_or(0)
    }

    /// Bump the cumulative for this peer by `delta` and return the
    /// new cumulative — this is the `CumulativePayout` to put in the
    /// cheque we're about to send. Caller is responsible for actually
    /// issuing the cheque after; if they fail to do so, the state will
    /// be inconsistent (we'll claim to have paid more than we did).
    /// That's OK — the cheque is only valuable if the peer presents
    /// it, and bee discards unwritten cheques on overlay key rotation
    /// anyway. The opposite mistake (under-reporting) would cause
    /// future cheques to bounce as `ErrChequeNotIncreasing`.
    pub fn bump_and_get(
        &mut self,
        peer_overlay_hex: &str,
        delta: u128,
    ) -> Result<u128, ChequeStoreError> {
        let key = peer_overlay_hex.to_lowercase();
        let cur = self.payouts.get(&key).copied().unwrap_or(0);
        let next = cur.checked_add(delta).ok_or(ChequeStoreError::Overflow)?;
        self.payouts.insert(key, next);
        Ok(next)
    }

    /// Atomically persist via write-rename. Cheap; the file is tiny
    /// (~50 bytes per peer we've ever paid). Called from the same
    /// `apply_log`-style flush path peers.json uses.
    pub fn save(&self) -> Result<(), ChequeStoreError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let on_disk = OnDisk {
            version: 1,
            chequebook: format!("0x{}", hex::encode(self.chequebook)),
            peers: self
                .payouts
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
        };
        let s = serde_json::to_string_pretty(&on_disk)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, s).map_err(|e| ChequeStoreError::Io(e.to_string()))?;
        std::fs::rename(&tmp, path).map_err(|e| ChequeStoreError::Io(e.to_string()))?;
        Ok(())
    }
}
