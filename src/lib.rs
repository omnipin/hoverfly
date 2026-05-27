//! isheika — minimal WASM-portable Swarm micro-client.
//!
//! Three operations: `discover`, `fetch`, `upload`. Networking is libp2p
//! over plain TCP **and** WebSocket on native builds, WebSocket-only on
//! `wasm32` (browsers can't open raw TCP sockets). DNS resolution is
//! DoH-only — no system resolver dependency, works the same in all
//! deployment shapes (CLI, daemon, WASM bundle).

pub mod cache;
#[cfg(not(target_arch = "wasm32"))]
pub mod cheques;
pub mod cid;
pub mod doh;
pub mod dnsaddr;
pub mod mime;
pub mod peers;
pub mod signer;
pub mod stamp;

pub mod proto {
    pub mod handshake {
        include!(concat!(env!("OUT_DIR"), "/handshake.rs"));
    }
    pub mod headers {
        include!(concat!(env!("OUT_DIR"), "/headers.rs"));
    }
    pub mod hive {
        include!(concat!(env!("OUT_DIR"), "/hive.rs"));
    }
    pub mod pricing {
        include!(concat!(env!("OUT_DIR"), "/pricing.rs"));
    }
    pub mod retrieval {
        include!(concat!(env!("OUT_DIR"), "/retrieval.rs"));
    }
    pub mod pushsync {
        include!(concat!(env!("OUT_DIR"), "/pushsync.rs"));
    }
    pub mod pseudosettle {
        include!(concat!(env!("OUT_DIR"), "/pseudosettle.rs"));
    }
    pub mod swap {
        include!(concat!(env!("OUT_DIR"), "/swap.rs"));
    }
    pub mod status {
        include!(concat!(env!("OUT_DIR"), "/status.rs"));
    }
}

pub mod protocols;
pub mod transport;
pub mod client;
pub mod manifest;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-export nectar primitives so consumers don't have to depend on nectar separately.
pub use nectar_primitives::{
    address::SwarmAddress,
    bmt::Hasher as BmtHasher,
    chunk::{ChunkAddress, ContentChunk},
};

pub use signer::SwarmSigner;
pub use peers::{Peer, PeerStore};
pub use transport::{Transport, TransportConfig, TransportError};
pub use client::{
    discover, fetch_bytes, fetch_manifest_path, list_manifest, prepare_upload_bytes,
    push_chunks_with_pool, upload_bytes, upload_collection, ClientError, ManifestEntry,
    SessionPool, StampedChunk, UploadFile,
};

#[cfg(unix)]
pub mod daemon;

#[cfg(not(target_arch = "wasm32"))]
pub mod inbound;

pub use cache::{ChunkCache, CachedChunk};
pub use doh::Doh;

/// Default Swarm mainnet bootnode (resolved via DoH).
pub const MAINNET_BOOTNODE: &str = "/dnsaddr/mainnet.ethswarm.org";

/// Default DoH resolver URL.
pub const DEFAULT_DOH_URL: &str = "https://cloudflare-dns.com/dns-query";

/// Crate version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
