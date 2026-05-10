//! High-level orchestration: `discover`, `fetch`, `upload`.
//!
//! Layered on top of `Transport` (libp2p WS) and nectar primitives. The
//! retrieval path implements [`nectar_primitives::store::ChunkGet`] over
//! peerlist-routed requests so that nectar's Joiner can drive multi-chunk
//! reassembly without knowing about libp2p.

use core::time::Duration;
use libp2p::Multiaddr;
use nectar_postage::{Batch, BatchId};
use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunk, ChunkAddress, ContentChunk};
use nectar_primitives::file::{join, sync_split};
use nectar_primitives::store::{ChunkGet, ChunkStoreError, SyncChunkGet, SyncChunkPut};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::dnsaddr::{is_ws_multiaddr, resolve, DnsAddrError};
use crate::doh::Doh;
use crate::peers::{Peer, PeerStore};
use crate::signer::SwarmSigner;
use crate::transport::{Transport, TransportError};

const BUCKET_DEPTH: u8 = 16;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    #[error("dnsaddr: {0}")]
    DnsAddr(#[from] DnsAddrError),
    #[error("primitives: {0}")]
    Primitives(String),
    #[error("file: {0}")]
    File(String),
    #[error("store: {0}")]
    Store(#[from] ChunkStoreError),
    #[error("hex decode: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid hash length: {0}")]
    BadHashLen(usize),
    #[error("no peers: {0}")]
    NoPeers(String),
    #[error("invalid batch id length: {0}")]
    BadBatchLen(usize),
    #[error("stamp: {0}")]
    Stamp(String),
    #[error("manifest: {0}")]
    Manifest(String),
}

impl From<nectar_primitives::error::PrimitivesError> for ClientError {
    fn from(e: nectar_primitives::error::PrimitivesError) -> Self {
        Self::Primitives(e.to_string())
    }
}

impl From<nectar_primitives::file::FileError> for ClientError {
    fn from(e: nectar_primitives::file::FileError) -> Self {
        Self::File(e.to_string())
    }
}

/// A `ChunkGet` adapter that routes requests through libp2p retrieval to the
/// closest peer in a peerlist, with retry-on-other-peer fallback.
pub struct NetworkedStore<'a> {
    transport: &'a Transport,
    peers: &'a PeerStore,
    max_retries: usize,
}

impl<'a> NetworkedStore<'a> {
    pub const fn new(transport: &'a Transport, peers: &'a PeerStore, max_retries: usize) -> Self {
        Self { transport, peers, max_retries }
    }
}

impl<'a> ChunkGet<DEFAULT_BODY_SIZE> for NetworkedStore<'a> {
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
        let mut bytes32 = [0u8; 32];
        bytes32.copy_from_slice(address.as_bytes());

        let candidates = self.peers.closest(address, self.max_retries.max(1));
        if candidates.is_empty() {
            return Err(ChunkStoreError::Other("no peers in peerlist".into()));
        }

        let mut last_err = String::from("no peers tried");
        for peer in candidates {
            let underlay = match peer.first_underlay() {
                Some(ma) if is_ws_multiaddr(&ma) => ma,
                _ => continue,
            };
            match self.transport.fetch_chunk(&underlay, &bytes32).await {
                Ok(delivery) => {
                    // bee sends span(8) || payload; parse via TryFrom which splits them.
                    let chunk = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(delivery.data.as_slice())
                        .map_err(|e| ChunkStoreError::Other(format!("decode chunk: {e}")))?;
                    use nectar_primitives::Chunk as _;
                    if chunk.address() != address {
                        warn!(target: "isheika::fetch", "peer {} returned chunk with wrong address", peer.overlay);
                        last_err = "address mismatch".to_string();
                        continue;
                    }
                    return Ok(AnyChunk::from(chunk));
                }
                Err(e) => {
                    warn!(target: "isheika::fetch", "peer {} failed: {}", peer.overlay, e);
                    last_err = e.to_string();
                }
            }
        }
        Err(ChunkStoreError::Other(format!("all peers failed: {last_err}")))
    }
}

/// A `SyncChunkGet` adapter that wraps an async network fetch by blocking
/// the current thread (via `tokio::task::block_in_place`). Used by mantaray
/// manifest decoding which expects a synchronous chunk store.
pub struct BlockingNetworkedStore<'a> {
    transport: &'a Transport,
    peers: &'a PeerStore,
    max_retries: usize,
    cache: Mutex<HashMap<ChunkAddress, AnyChunk<DEFAULT_BODY_SIZE>>>,
}

impl<'a> BlockingNetworkedStore<'a> {
    pub fn new(transport: &'a Transport, peers: &'a PeerStore, max_retries: usize) -> Self {
        Self {
            transport,
            peers,
            max_retries,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

impl<'a> SyncChunkGet<DEFAULT_BODY_SIZE> for BlockingNetworkedStore<'a> {
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
        if let Some(c) = self.cache.lock().unwrap().get(address).cloned() {
            return Ok(c);
        }
        info!(target: "isheika::manifest", "blocking fetch for {}", address);
        let handle = tokio::runtime::Handle::current();
        let store = NetworkedStore::new(self.transport, self.peers, self.max_retries);
        let address_copy = *address;
        let chunk = handle.block_on(async move {
            ChunkGet::<DEFAULT_BODY_SIZE>::get(&store, &address_copy).await
        })?;
        info!(target: "isheika::manifest", "got chunk: data.len()={}", chunk.data().len());
        self.cache.lock().unwrap().insert(*address, chunk.clone());
        Ok(chunk)
    }
}

/// Discover peers by dialing a bootstrap multiaddr (or `/dnsaddr/...`) and
/// listening on the hive stream.
pub async fn discover(
    transport: &Transport,
    doh: &Doh,
    bootstrap: &Multiaddr,
    wait: Duration,
) -> Result<Vec<Peer>, ClientError> {
    let resolved = resolve(bootstrap, doh).await?;
    if resolved.is_empty() {
        return Err(ClientError::NoPeers(format!(
            "no ws/wss multiaddrs from {bootstrap}"
        )));
    }

    let mut all: Vec<Peer> = Vec::new();
    for ma in resolved {
        info!(target: "isheika::discover", "dialing {}", ma);
        match transport.discover_peers(&ma, wait).await {
            Ok(mut batch) => {
                debug!(target: "isheika::discover", "{} returned {} peers", ma, batch.len());
                all.append(&mut batch);
            }
            Err(e) => warn!(target: "isheika::discover", "discover from {} failed: {}", ma, e),
        }
    }
    Ok(all)
}

/// Fetch arbitrary-size content addressed by `root` (32-byte content address).
/// Walks the BMT tree via [`nectar_primitives::file::join`].
pub async fn fetch_bytes(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    max_retries_per_chunk: usize,
) -> Result<Vec<u8>, ClientError> {
    let root = parse_root(root_hex)?;
    let store = NetworkedStore::new(transport, peers, max_retries_per_chunk);
    let bytes = join::<ChunkAddress, _, DEFAULT_BODY_SIZE>(store, root).await?;
    Ok(bytes)
}

/// Resolve `path` through the mantaray manifest at `root_hex` and fetch the
/// resulting entry's content. Returns `(content_bytes, content_type)` where
/// `content_type` is `None` if the manifest entry has no `Content-Type`
/// metadata.
pub async fn fetch_manifest_path(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    path: &str,
    max_retries_per_chunk: usize,
) -> Result<(Vec<u8>, Option<String>), ClientError> {
    let root = parse_root(root_hex)?;
    let (target, content_type) = lookup_manifest_path(transport, peers, root, path, max_retries_per_chunk).await?;

    let async_store = NetworkedStore::new(transport, peers, max_retries_per_chunk);
    let bytes = join::<ChunkAddress, _, DEFAULT_BODY_SIZE>(async_store, target).await?;
    Ok((bytes, content_type))
}

/// List entries in the mantaray manifest at `root_hex`.
pub async fn list_manifest(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    max_retries_per_chunk: usize,
) -> Result<Vec<ManifestEntry>, ClientError> {
    let root = parse_root(root_hex)?;
    let mut out = Vec::new();
    walk_manifest(transport, peers, root, Vec::new(), max_retries_per_chunk, &mut out).await?;
    Ok(out)
}

async fn lookup_manifest_path(
    transport: &Transport,
    peers: &PeerStore,
    root: ChunkAddress,
    path: &str,
    max_retries: usize,
) -> Result<(ChunkAddress, Option<String>), ClientError> {
    use crate::manifest::decode_node;
    let store = NetworkedStore::new(transport, peers, max_retries);
    let mut current = root;
    let mut remaining: &[u8] = path.as_bytes();
    let mut last_content_type: Option<String> = None;

    loop {
        let chunk = ChunkGet::<DEFAULT_BODY_SIZE>::get(&store, &current)
            .await
            .map_err(|e| ClientError::Manifest(format!("fetch node {current}: {e}")))?;
        let node = decode_node(chunk.data())
            .map_err(|e| ClientError::Manifest(e.to_string()))?;

        if remaining.is_empty() {
            return node
                .entry
                .map(|addr| (addr, last_content_type.clone()))
                .ok_or_else(|| ClientError::Manifest(format!("path {path} has no entry")));
        }

        let first = remaining[0];
        let fork = node
            .forks
            .get(&first)
            .ok_or_else(|| ClientError::Manifest(format!("no fork for {path}")))?;
        if !remaining.starts_with(&fork.prefix) {
            return Err(ClientError::Manifest(format!(
                "path {path} doesn't match fork prefix"
            )));
        }
        if let Some(ct) = fork.metadata.get("Content-Type") {
            last_content_type = Some(ct.clone());
        }
        remaining = &remaining[fork.prefix.len()..];
        current = fork.reference;
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_manifest<'a>(
    transport: &'a Transport,
    peers: &'a PeerStore,
    addr: ChunkAddress,
    path_so_far: Vec<u8>,
    max_retries: usize,
    out: &'a mut Vec<ManifestEntry>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send + 'a>> {
    Box::pin(async move {
        use crate::manifest::decode_node;
        let store = NetworkedStore::new(transport, peers, max_retries);
        let chunk = ChunkGet::<DEFAULT_BODY_SIZE>::get(&store, &addr)
            .await
            .map_err(|e| ClientError::Manifest(format!("fetch node {addr}: {e}")))?;
        let node =
            decode_node(chunk.data()).map_err(|e| ClientError::Manifest(e.to_string()))?;

        if let Some(entry_addr) = node.entry {
            let path = String::from_utf8_lossy(&path_so_far).into_owned();
            out.push(ManifestEntry {
                path,
                reference: hex::encode(entry_addr.as_bytes()),
                content_type: None,
            });
        }

        for fork in node.forks.values() {
            let mut next_path = path_so_far.clone();
            next_path.extend_from_slice(&fork.prefix);
            walk_manifest(transport, peers, fork.reference, next_path, max_retries, out).await?;
        }
        Ok(())
    })
}

#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub path: String,
    pub reference: String,
    pub content_type: Option<String>,
}

/// Upload arbitrary-size content. Splits via nectar, stamps each chunk with
/// the supplied batch + signer, and pushes every chunk via pushsync to the
/// closest peer in the peerlist. Returns the root content address.
pub async fn upload_bytes(
    transport: &Transport,
    peers: &PeerStore,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    max_retries_per_chunk: usize,
) -> Result<ChunkAddress, ClientError> {
    let batch_id = parse_batch_id(batch_id_hex)?;

    // 1. Split into a local store.
    let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data)?;
    info!(target: "isheika::upload", "split {} bytes into {} chunks (root {})",
        data.len(), store.len(), root);

    // 2. Build a stamper. We construct an issuer keyed by depth/bucket-depth.
    let batch = Batch::new(
        batch_id,
        0u128,
        0u64,
        alloy_primitives::Address::from(*signer.eth_address()),
        depth,
        BUCKET_DEPTH,
        false,
    );
    let issuer = MemoryIssuer::from_batch(&batch);
    let mut stamper = BatchStamper::new(issuer, signer.alloy_signer().clone());

    // 3. Pushsync every chunk in the store. We iterate over a snapshot so we don't
    // hold the store lock across awaits.
    let snapshot = store.into_chunks();
    for (addr, chunk) in &snapshot {
        let stamp = stamper
            .stamp(addr)
            .map_err(|e| ClientError::Stamp(e.to_string()))?;
        let stamp_bytes = stamp.to_bytes();
        let chunk_data = chunk.data().to_vec();
        let mut addr32 = [0u8; 32];
        addr32.copy_from_slice(addr.as_bytes());

        push_one_chunk(
            transport,
            peers,
            &addr32,
            &chunk_data,
            &stamp_bytes[..],
            max_retries_per_chunk,
        )
        .await?;
    }

    Ok(root)
}

async fn push_one_chunk(
    transport: &Transport,
    peers: &PeerStore,
    chunk_addr: &[u8; 32],
    chunk_data: &[u8],
    stamp_bytes: &[u8],
    max_retries: usize,
) -> Result<(), ClientError> {
    let target = ChunkAddress::new(*chunk_addr);
    let candidates = peers.closest(&target, max_retries.max(1));
    if candidates.is_empty() {
        return Err(ClientError::NoPeers("peerlist empty".into()));
    }
    let mut last_err = String::from("no peers tried");
    for peer in candidates {
        let underlay = match peer.first_underlay() {
            Some(ma) if is_ws_multiaddr(&ma) => ma,
            _ => continue,
        };
        match transport
            .pushsync_chunk(&underlay, chunk_addr, chunk_data, stamp_bytes)
            .await
        {
            Ok(_receipt) => return Ok(()),
            Err(e) => {
                warn!(target: "isheika::upload", "push to {} failed: {}", peer.overlay, e);
                last_err = e.to_string();
            }
        }
    }
    Err(ClientError::NoPeers(format!("push failed: {last_err}")))
}

fn parse_root(hex_str: &str) -> Result<ChunkAddress, ClientError> {
    let bytes = hex::decode(hex_str.trim_start_matches("0x"))?;
    if bytes.len() != 32 {
        return Err(ClientError::BadHashLen(bytes.len()));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(ChunkAddress::new(arr))
}

fn parse_batch_id(hex_str: &str) -> Result<BatchId, ClientError> {
    let bytes = hex::decode(hex_str.trim_start_matches("0x"))?;
    if bytes.len() != 32 {
        return Err(ClientError::BadBatchLen(bytes.len()));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(BatchId::from(arr))
}

// Unused trait imports kept here to ensure the bridge between sync/async
// store traits is available (nectar wires them via blanket impls).
#[allow(dead_code)]
fn _store_traits_in_scope<S: SyncChunkGet<DEFAULT_BODY_SIZE> + SyncChunkPut<DEFAULT_BODY_SIZE>>(_: S) {}
