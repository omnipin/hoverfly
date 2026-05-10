//! isheika — minimal WS-only WASM-portable Swarm micro-client.
//!
//! Three operations: `discover`, `fetch`, `upload`. Networking is libp2p WebSocket
//! only (websys on wasm32). DNS resolution is DoH-only.

pub mod doh;
pub mod dnsaddr;
pub mod peers;
pub mod signer;

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
    discover, fetch_bytes, fetch_manifest_path, list_manifest, upload_bytes, ClientError,
    ManifestEntry,
};
pub use doh::Doh;

/// Default Swarm mainnet bootnode (resolved via DoH).
pub const MAINNET_BOOTNODE: &str = "/dnsaddr/mainnet.ethswarm.org";

/// Default DoH resolver URL.
pub const DEFAULT_DOH_URL: &str = "https://cloudflare-dns.com/dns-query";

/// Crate version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
