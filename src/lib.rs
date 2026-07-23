//! hoverfly — minimal WASM-portable Swarm micro-client.
//!
//! Three operations: `discover`, `fetch`, `upload`. Networking is libp2p
//! over plain TCP **and** WebSocket on native builds, WebSocket-only on
//! `wasm32` (browsers can't open raw TCP sockets). DNS resolution is
//! DoH-only — no system resolver dependency, works the same in all
//! deployment shapes (CLI, daemon, WASM bundle).

#[cfg(not(target_arch = "wasm32"))]
pub mod batch;
#[cfg(all(not(target_arch = "wasm32"), feature = "bridge"))]
pub mod bridge;
pub mod cache;
#[cfg(not(target_arch = "wasm32"))]
pub mod cheques;
pub mod cid;
pub mod dnsaddr;
pub mod doh;
pub mod mime;
pub mod peers;
pub mod signer;
pub mod stamp;

pub mod proto;

pub mod client;
pub mod erasure;
pub mod feed;
pub mod manifest;
pub mod protocols;
pub mod ratelimit;
pub mod transport;

#[cfg(target_arch = "wasm32")]
pub mod wasm;
// Vendored copy of libp2p-websocket-websys, patched so `WebSocket.send()` is
// handed a non-shared buffer (the wasm memory is a SharedArrayBuffer because of
// the atomics/wasm-bindgen-rayon build, and Chrome rejects sending shared views).
#[cfg(target_arch = "wasm32")]
mod wsws;
// Persistent IndexedDB-backed L2 chunk cache (browser only). Immutable,
// content-addressed chunks survive reloads/sessions on top of the per-fetch
// in-memory cache in `client::NetworkedStore`.
#[cfg(target_arch = "wasm32")]
pub mod idb_chunk_store;

// Re-export nectar primitives so consumers don't have to depend on nectar separately.
pub use nectar_primitives::{
    address::SwarmAddress,
    bmt::Hasher as BmtHasher,
    chunk::{ChunkAddress, ContentChunk},
};

pub use client::{
    ClientError, ManifestEntry, SessionPool, StampedChunk, UploadFile, bmt_root, collection_root,
    discover, fetch_bytes, fetch_manifest_path, list_manifest, prepare_upload_bytes,
    push_chunks_with_pool, upload_bytes, upload_collection,
};
pub use peers::{Peer, PeerStore};
pub use signer::SwarmSigner;
pub use transport::{Transport, TransportConfig, TransportError};

#[cfg(unix)]
pub mod daemon;

#[cfg(not(target_arch = "wasm32"))]
pub mod inbound;

pub use cache::{CachedChunk, ChunkCache};
pub use doh::Doh;

/// Default Swarm mainnet bootnode (resolved via DoH).
pub const MAINNET_BOOTNODE: &str = "/dnsaddr/mainnet.ethswarm.org";

/// Default DoH resolver URL.
pub const DEFAULT_DOH_URL: &str = "https://cloudflare-dns.com/dns-query";

/// Crate version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
