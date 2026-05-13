//! Daemon-wide chunk cache.
//!
//! Holds wire-form `(data, stamp)` pairs for every chunk we've stamped
//! during an upload (and optionally chunks we've fetched on behalf of a
//! caller). The inbound retrieval responder reads from this map so the
//! daemon can serve its own uploads back to the swarm immediately —
//! without waiting for pushsync to propagate the chunk into the
//! neighborhood. Mirrors what a bee node naturally does by storing every
//! uploaded chunk in its local uploadstore.
//!
//! In-memory only, unbounded for now. Each entry is ~4 KiB of `data` +
//! ~113 bytes of `stamp`; 50 MiB of uploads ≈ 50 MB of cache. Disk
//! backing + LRU eviction are deliberately out of scope for v1.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

/// A single cached chunk in the exact shape the retrieval responder
/// will hand back over the wire.
#[derive(Clone, Debug)]
pub struct CachedChunk {
    /// `span_LE_8 || payload` — i.e. the bee `Delivery.Data` field as
    /// expected by `cac.Valid(swarm.NewChunk(addr, data))` on the
    /// remote side.
    pub data: Bytes,
    /// Postage stamp bytes, written as `Delivery.Stamp`.
    pub stamp: Bytes,
}

/// Shared chunk cache. `Clone` is cheap (`Arc`).
#[derive(Clone, Default, Debug)]
pub struct ChunkCache {
    inner: Arc<RwLock<HashMap<[u8; 32], CachedChunk>>>,
}

impl ChunkCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, addr: &[u8; 32]) -> Option<CachedChunk> {
        self.inner.read().ok()?.get(addr).cloned()
    }

    pub fn contains(&self, addr: &[u8; 32]) -> bool {
        self.inner
            .read()
            .map(|g| g.contains_key(addr))
            .unwrap_or(false)
    }

    pub fn put(&self, addr: [u8; 32], data: Bytes, stamp: Bytes) {
        if let Ok(mut g) = self.inner.write() {
            g.insert(addr, CachedChunk { data, stamp });
        }
    }

    /// Bulk insert. Existing entries for the same address are
    /// overwritten — semantically harmless because chunk address is a
    /// content hash, so the wire bytes are identical; the stamp may
    /// have rotated, and the newer stamp is the one we want to serve.
    pub fn put_many<I>(&self, items: I)
    where
        I: IntoIterator<Item = ([u8; 32], Bytes, Bytes)>,
    {
        if let Ok(mut g) = self.inner.write() {
            for (addr, data, stamp) in items {
                g.insert(addr, CachedChunk { data, stamp });
            }
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().map(|g| g.is_empty()).unwrap_or(true)
    }
}
