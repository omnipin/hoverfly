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
use crate::transport::{
    is_connection_dead, peer_price, PeerSession, PushOutcome, Transport, TransportError,
};
use nectar_primitives::address::SwarmAddress;

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
/// listening on the hive stream. Equivalent to a single-hop discover.
pub async fn discover(
    transport: &Transport,
    doh: &Doh,
    bootstrap: &Multiaddr,
    wait: Duration,
) -> Result<Vec<Peer>, ClientError> {
    discover_recursive(transport, doh, bootstrap, wait, 1).await
}

/// Recursively discover peers up to `max_rounds` hops out from
/// `bootstrap`. Each round, every newly-found peer is itself dialed and
/// asked for its hive — building up a much larger peerset that spans
/// more of the swarm address space. Pushing chunks to a sparse peerlist
/// fails when bee can't find a forwarding path (`could not push chunk`),
/// so for upload-heavy workloads call this with `max_rounds=2..4`.
pub async fn discover_recursive(
    transport: &Transport,
    doh: &Doh,
    bootstrap: &Multiaddr,
    wait_per_peer: Duration,
    max_rounds: usize,
) -> Result<Vec<Peer>, ClientError> {
    use std::collections::HashSet;

    let resolved = resolve(bootstrap, doh).await?;
    if resolved.is_empty() {
        return Err(ClientError::NoPeers(format!(
            "no ws/wss multiaddrs from {bootstrap}"
        )));
    }

    let mut all: Vec<Peer> = Vec::new();
    let mut seen_overlays: HashSet<String> = HashSet::new();
    let mut frontier: Vec<Multiaddr> = resolved;

    for round in 0..max_rounds {
        if frontier.is_empty() {
            break;
        }
        info!(target: "isheika::discover",
            "round {} of {}: dialing {} peer(s)",
            round + 1, max_rounds, frontier.len());

        let mut next_frontier: Vec<Multiaddr> = Vec::new();
        for ma in frontier.drain(..) {
            debug!(target: "isheika::discover", "dialing {}", ma);
            match transport.discover_peers(&ma, wait_per_peer).await {
                Ok(batch) => {
                    debug!(target: "isheika::discover",
                        "{} returned {} peers", ma, batch.len());
                    for p in batch {
                        let key = p.overlay.to_lowercase();
                        if seen_overlays.insert(key) {
                            // Queue this peer as a discovery target for the
                            // next round if it has any ws/wss underlay.
                            // (Bee hive announcements typically include both
                            // a TCP and a ws address per peer; we filter
                            // explicitly to ws — the only transport we can
                            // dial from a WASM-portable client.)
                            if let Some(u) = p.first_ws_underlay() {
                                next_frontier.push(u);
                            }
                            all.push(p);
                        }
                    }
                }
                Err(e) => {
                    debug!(target: "isheika::discover",
                        "discover from {} failed: {}", ma, e);
                }
            }
        }

        info!(target: "isheika::discover",
            "round {} done: total unique peers = {}", round + 1, all.len());
        frontier = next_frontier;
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

/// Default number of peer sessions opened in parallel for upload.
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 8;

/// Upload a file's content plus a single-entry mantaray manifest pointing
/// `path` at the file root, with optional `Content-Type` metadata. Returns
/// the *manifest* root — fetchable via `fetch <manifest_root> --path <path>`.
#[allow(clippy::too_many_arguments)]
pub async fn upload_file_with_manifest(
    transport: &Transport,
    peers: &PeerStore,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    path: &str,
    content_type: Option<&str>,
    max_retries_per_chunk: usize,
) -> Result<ChunkAddress, ClientError> {
    upload_file_with_manifest_ex(
        transport,
        peers,
        signer,
        batch_id_hex,
        depth,
        data,
        path,
        content_type,
        max_retries_per_chunk,
        DEFAULT_UPLOAD_CONCURRENCY,
    )
    .await
}

/// Input to `upload_collection_ex`: one file's bytes and its in-manifest
/// path (matches bee's tar/multipart `dirUploadHandler`).
pub struct UploadFile {
    pub path: String,
    pub content_type: Option<String>,
    pub data: Vec<u8>,
}

/// Upload a collection of files as a multi-entry mantaray manifest, the way
/// bee handles `POST /bzz` with `Content-Type: application/x-tar` or
/// `multipart/form-data`. Each file is split with BMT independently, and a
/// single manifest is built with one entry per file. Optional
/// `index_document` / `error_document` are written as website metadata at
/// the root path so that gateways serve `index.html` for `/<root>/` etc.
///
/// Returns the manifest root.
#[allow(clippy::too_many_arguments)]
pub async fn upload_collection(
    transport: &Transport,
    peers: &PeerStore,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    files: Vec<UploadFile>,
    index_document: Option<&str>,
    error_document: Option<&str>,
    max_retries_per_chunk: usize,
    concurrency: usize,
) -> Result<ChunkAddress, ClientError> {
    use crate::manifest::CollectionEntry;

    if files.is_empty() {
        return Err(ClientError::Manifest("collection is empty".into()));
    }

    let batch_id = parse_batch_id(batch_id_hex)?;
    let mut stamper = build_stamper(signer, batch_id, depth);

    // Bee enforces `index < 2^(depth - bucketDepth)` per (batch, bucket).
    // Stamping the same chunk address twice burns two indices in the same
    // bucket and can overflow it, which bee rejects with `invalid stamp:
    // invalid index`. Across a tar full of small files there's huge
    // duplication (common headers, all-zero padding, identical assets),
    // so we deduplicate by chunk address before stamping.
    let mut seen: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
    let mut work: Vec<StampedChunk> = Vec::new();
    let mut entries: Vec<CollectionEntry> = Vec::with_capacity(files.len());
    let mut total_bytes: usize = 0;
    let mut raw_chunks = 0usize;
    for f in &files {
        let (file_root, file_store) = sync_split::<DEFAULT_BODY_SIZE>(&f.data)?;
        debug!(
            target: "isheika::upload",
            "collection: {} ({} bytes) -> {} chunks (root {})",
            f.path, f.data.len(), file_store.len(), file_root
        );
        total_bytes += f.data.len();
        for (addr, chunk) in file_store.into_chunks() {
            raw_chunks += 1;
            let mut addr_bytes = [0u8; 32];
            addr_bytes.copy_from_slice(addr.as_bytes());
            if !seen.insert(addr_bytes) {
                continue; // already stamped — bee dedupes on address anyway
            }
            work.push(stamp_chunk(&mut stamper, &addr, wire_form(&chunk))?);
        }
        entries.push(CollectionEntry {
            path: f.path.clone(),
            reference: file_root,
            content_type: f.content_type.clone(),
        });
    }

    // 2. Build the multi-entry manifest.
    let (manifest_root, manifest_chunks) =
        crate::manifest::build_collection_manifest(&entries, index_document, error_document)
            .map_err(|e| ClientError::Manifest(e.to_string()))?;
    info!(
        target: "isheika::upload",
        "collection: {} files ({} bytes) -> {} unique file chunks ({} duplicates skipped) + {} manifest chunks (root {})",
        files.len(), total_bytes, work.len(),
        raw_chunks.saturating_sub(work.len()),
        manifest_chunks.len(), manifest_root,
    );

    // 3. Stamp manifest chunks (also dedup; share the seen set).
    for (addr, wire) in manifest_chunks {
        let mut addr_bytes = [0u8; 32];
        addr_bytes.copy_from_slice(addr.as_bytes());
        if !seen.insert(addr_bytes) {
            continue;
        }
        work.push(stamp_chunk(&mut stamper, &addr, wire.to_vec())?);
    }

    // 4. Push everything concurrently.
    push_chunks_concurrent(transport, peers, work, max_retries_per_chunk, concurrency).await?;
    Ok(manifest_root)
}

#[allow(clippy::too_many_arguments)]
pub async fn upload_file_with_manifest_ex(
    transport: &Transport,
    peers: &PeerStore,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    path: &str,
    content_type: Option<&str>,
    max_retries_per_chunk: usize,
    concurrency: usize,
) -> Result<ChunkAddress, ClientError> {
    let batch_id = parse_batch_id(batch_id_hex)?;

    // 1. Split file into BMT tree.
    let (file_root, file_store) = sync_split::<DEFAULT_BODY_SIZE>(data)?;
    info!(target: "isheika::upload", "file: {} bytes -> {} chunks (root {})",
        data.len(), file_store.len(), file_root);

    // 2. Build a single-entry manifest pointing at file_root.
    let (manifest_root, manifest_chunks) =
        crate::manifest::build_single_entry_manifest(path, file_root, content_type)
            .map_err(|e| ClientError::Manifest(e.to_string()))?;
    info!(target: "isheika::upload", "manifest: {} chunks (root {})", manifest_chunks.len(), manifest_root);

    // 3. Build stamper.
    let mut stamper = build_stamper(signer, batch_id, depth);

    // 4. Pre-stamp everything (stamper is single-threaded — produce the wire
    // forms and stamps up-front so the push phase is purely network-bound).
    let mut work: Vec<StampedChunk> =
        Vec::with_capacity(file_store.len() + manifest_chunks.len());
    for (addr, chunk) in file_store.into_chunks() {
        work.push(stamp_chunk(&mut stamper, &addr, wire_form(&chunk))?);
    }
    for (addr, wire) in manifest_chunks {
        work.push(stamp_chunk(&mut stamper, &addr, wire.to_vec())?);
    }

    // 5. Open a pool of long-lived sessions and push concurrently.
    push_chunks_concurrent(transport, peers, work, max_retries_per_chunk, concurrency).await?;

    Ok(manifest_root)
}

/// Convert a nectar AnyChunk into the wire form `span_LE_8 || payload`.
fn wire_form(chunk: &AnyChunk<DEFAULT_BODY_SIZE>) -> Vec<u8> {
    let mut wire = Vec::with_capacity(8 + chunk.data().len());
    wire.extend_from_slice(&chunk.span().to_le_bytes());
    wire.extend_from_slice(chunk.data());
    wire
}

/// A chunk pre-stamped and ready for the wire.
struct StampedChunk {
    addr: [u8; 32],
    wire: Vec<u8>,
    stamp: Vec<u8>,
}

fn build_stamper(
    signer: &SwarmSigner,
    batch_id: BatchId,
    depth: u8,
) -> BatchStamper<MemoryIssuer, alloy_signer_local::PrivateKeySigner> {
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
    BatchStamper::new(issuer, signer.alloy_signer().clone())
}

fn stamp_chunk(
    stamper: &mut BatchStamper<MemoryIssuer, alloy_signer_local::PrivateKeySigner>,
    addr: &ChunkAddress,
    wire: Vec<u8>,
) -> Result<StampedChunk, ClientError> {
    let stamp = stamper
        .stamp(addr)
        .map_err(|e| ClientError::Stamp(e.to_string()))?;
    let stamp_bytes = stamp.to_bytes().to_vec();
    let mut addr32 = [0u8; 32];
    addr32.copy_from_slice(addr.as_bytes());
    Ok(StampedChunk { addr: addr32, wire, stamp: stamp_bytes })
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
    upload_bytes_ex(
        transport,
        peers,
        signer,
        batch_id_hex,
        depth,
        data,
        max_retries_per_chunk,
        DEFAULT_UPLOAD_CONCURRENCY,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn upload_bytes_ex(
    transport: &Transport,
    peers: &PeerStore,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    max_retries_per_chunk: usize,
    concurrency: usize,
) -> Result<ChunkAddress, ClientError> {
    let batch_id = parse_batch_id(batch_id_hex)?;

    let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data)?;
    info!(target: "isheika::upload", "split {} bytes into {} chunks (root {})",
        data.len(), store.len(), root);

    let mut stamper = build_stamper(signer, batch_id, depth);

    let snapshot = store.into_chunks();
    let mut work: Vec<StampedChunk> = Vec::with_capacity(snapshot.len());
    for (addr, chunk) in &snapshot {
        work.push(stamp_chunk(&mut stamper, addr, wire_form(chunk))?);
    }

    push_chunks_concurrent(transport, peers, work, max_retries_per_chunk, concurrency).await?;
    Ok(root)
}

/// A session and the peer overlay it talks to, kept together so we can
/// route each chunk to the session whose peer is closest to it. The
/// `PeerSession` inside is replaced on the fly when the driver retires
/// itself after `MAX_PUSHES_PER_SESSION` pushes (see transport.rs); a
/// brand-new libp2p connection is dialed to reset bee's `ghostBalance`.
struct SessionEntry {
    overlay: SwarmAddress,
    overlay_hex: String,
    underlay: libp2p::Multiaddr,
    session: std::sync::Mutex<PeerSession>,
}

impl SessionEntry {
    fn snapshot(&self) -> PeerSession {
        self.session.lock().expect("session mutex poisoned").clone()
    }

    /// Replace the stored session with `new`, returning whether the
    /// caller's snapshot is now stale (always true).
    fn replace(&self, new: PeerSession) {
        let mut guard = self.session.lock().expect("session mutex poisoned");
        *guard = new;
    }
}

/// Push a batch of pre-stamped chunks using a pool of long-lived peer
/// sessions. Each session handshakes once and is then reused for many
/// pushsync streams (each chunk gets its own yamux substream).
///
/// Routing: bee's pushsync handler will only store a chunk locally if it
/// lies within the peer's storage radius; otherwise it must forward to a
/// closer peer, which can fail with `handler: push to closest chunk X:
/// could not push chunk`. To minimise forwarding hops we sort sessions by
/// proximity to the chunk address and try the closest ones first.
async fn push_chunks_concurrent(
    transport: &Transport,
    peers: &PeerStore,
    work: Vec<StampedChunk>,
    max_retries: usize,
    concurrency: usize,
) -> Result<(), ClientError> {
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    if work.is_empty() {
        return Ok(());
    }

    let sessions = open_session_pool(transport, peers, concurrency.max(1)).await?;
    if sessions.is_empty() {
        return Err(ClientError::NoPeers("no reachable ws peers".into()));
    }
    info!(
        target: "isheika::upload",
        "opened {} peer session(s), pushing {} chunks",
        sessions.len(),
        work.len()
    );

    let pool = Arc::new(sessions);
    let total = work.len();
    let pushed = Arc::new(AtomicUsize::new(0));

    // For each chunk we walk sessions in descending-proximity order. With
    // many sessions and yamux multiplexing, keeping pool*4 requests in
    // flight saturates round-trip latency without overcommitting any one
    // session.
    let buffer = pool.len().saturating_mul(4).max(pool.len());

    let dispatch = |chunk: StampedChunk| {
        let pool = pool.clone();
        let pushed = pushed.clone();
        let transport = transport;
        async move {
            // Rank sessions by proximity to this chunk's address; closest
            // first. bee at that peer is then either inside its area of
            // responsibility (stores directly) or only a short hop away.
            let chunk_addr = SwarmAddress::new(chunk.addr);
            let mut order: Vec<usize> = (0..pool.len()).collect();
            order.sort_by(|&a, &b| {
                let pa = chunk_addr.proximity(&pool[a].overlay);
                let pb = chunk_addr.proximity(&pool[b].overlay);
                pb.cmp(&pa) // descending PO == closer first
            });

            // Two outer rounds: if every peer reports Overdraft on the first
            // pass we sleep briefly to let pseudosettle refresh free credit,
            // then retry. After that, treat as a hard failure.
            let mut last_err: Option<TransportError> = None;
            for outer in 0..3u32 {
                let mut all_overdraft = true;
                for (i, &idx) in order.iter().take(max_retries.max(1)).enumerate() {
                    let entry = &pool[idx];
                    let mut peer_overlay = [0u8; 32];
                    peer_overlay.copy_from_slice(entry.overlay.as_bytes());
                    let price = peer_price(&peer_overlay, &chunk.addr);
                    let outcome = try_push_with_rotation(entry, &chunk, price, transport).await;
                    match outcome {
                        Ok(PushOutcome::Receipt(_)) => {
                            let done = pushed.fetch_add(1, Ordering::Relaxed) + 1;
                            // Log every 50th success at info, plus the last
                            // few. Per-chunk would drown the WARN noise we
                            // already emit on failures.
                            if done % 50 == 0 || done == total {
                                info!(target: "isheika::upload",
                                    "pushed {}/{} chunks (latest via {} po={})",
                                    done, total, entry.overlay_hex,
                                    chunk_addr.proximity(&entry.overlay));
                            } else {
                                debug!(target: "isheika::upload",
                                    "push ok ({}/{}) via {} (po={}, price={})",
                                    done, total, entry.overlay_hex,
                                    chunk_addr.proximity(&entry.overlay), price);
                            }
                            return Ok::<_, ClientError>(());
                        }
                        Ok(PushOutcome::Overdraft) => {
                            debug!(target: "isheika::upload",
                                "overdraft on {} (po={}); trying next peer",
                                entry.overlay_hex,
                                chunk_addr.proximity(&entry.overlay));
                            continue;
                        }
                        Err(e) => {
                            all_overdraft = false;
                            warn!(
                                target: "isheika::upload",
                                "push attempt {} via {} (po={}) failed: {}",
                                i + 1,
                                entry.overlay_hex,
                                chunk_addr.proximity(&entry.overlay),
                                e
                            );
                            last_err = Some(e);
                        }
                    }
                }
                if !all_overdraft {
                    break;
                }
                debug!(target: "isheika::upload",
                    "all peers overdrafted (round {}); waiting for refresh", outer);
                tokio::time::sleep(Duration::from_millis(1100)).await;
            }
            Err(ClientError::NoPeers(format!(
                "all {} attempts failed: {}",
                max_retries.max(1),
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "all overdrafted".into())
            )))
        }
    };

    let mut inflight = FuturesUnordered::new();
    let mut iter = work.into_iter();

    for _ in 0..buffer {
        if let Some(c) = iter.next() {
            inflight.push(dispatch(c));
        } else {
            break;
        }
    }

    let mut first_err: Option<ClientError> = None;
    while let Some(res) = inflight.next().await {
        match res {
            Ok(()) => {
                if let Some(c) = iter.next() {
                    inflight.push(dispatch(c));
                }
            }
            Err(e) => {
                first_err = Some(e);
                break;
            }
        }
    }

    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

/// Send one push, transparently rotating the underlying libp2p
/// connection when the driver retires. The driver retires after
/// `MAX_PUSHES_PER_SESSION` pushes to keep bee's per-overlay
/// `ghostBalance` from reaching its disconnect threshold (see
/// transport.rs::MAX_PUSHES_PER_SESSION).
async fn try_push_with_rotation(
    entry: &SessionEntry,
    chunk: &StampedChunk,
    price: u64,
    transport: &Transport,
) -> Result<PushOutcome, TransportError> {
    let session = entry.snapshot();
    match session
        .pushsync_chunk_priced(&chunk.addr, &chunk.wire, &chunk.stamp, price)
        .await
    {
        Ok(out) => Ok(out),
        // Bee returned a Receipt with err set ("could not push chunk",
        // "invalid stamp", etc.). The connection is fine; the chunk is
        // just unhappy on this peer. Bubble up so the dispatcher picks
        // the next peer.
        Err(e) if !is_connection_dead(&e) => Err(e),
        // Connection is gone. Dial a fresh one (resets bee's
        // ghostBalance via Connect()) and retry once on the new session.
        Err(_) => {
            let fresh = transport.open_session(&entry.underlay).await?;
            debug!(target: "isheika::upload",
                "rotated session to {} (new libp2p connection)",
                entry.overlay_hex);
            entry.replace(fresh.clone());
            fresh
                .pushsync_chunk_priced(&chunk.addr, &chunk.wire, &chunk.stamp, price)
                .await
        }
    }
}

/// Open sessions to every reachable ws peer in the store, capped at
/// `max_sessions`. We want broad address-space coverage because per-chunk
/// dispatch uses proximity routing — the more peers we can reach, the
/// closer (on average) the picked session is to any given chunk address,
/// and the less bee has to forward.
async fn open_session_pool(
    transport: &Transport,
    peers: &PeerStore,
    max_sessions: usize,
) -> Result<Vec<SessionEntry>, ClientError> {
    use futures::stream::{FuturesUnordered, StreamExt};

    // Take every ws peer we know about (capped). `closest` to the zero
    // address is just a stable ordering — content here is the full set.
    let zero = ChunkAddress::new([0u8; 32]);
    let candidates = peers.closest(&zero, peers.len().max(max_sessions));
    if candidates.is_empty() {
        return Err(ClientError::NoPeers("peerlist empty".into()));
    }

    let mut dialing = FuturesUnordered::new();
    for peer in candidates {
        let underlay = match peer.first_underlay() {
            Some(ma) if is_ws_multiaddr(&ma) => ma,
            _ => continue,
        };
        let overlay_hex = peer.overlay.clone();
        let overlay = match peer.overlay_address() {
            Some(o) => o,
            None => continue,
        };
        dialing.push(async move {
            let result = transport.open_session(&underlay).await;
            (overlay, overlay_hex, underlay, result)
        });
        if dialing.len() >= max_sessions {
            break;
        }
    }

    let mut sessions = Vec::with_capacity(max_sessions);
    while let Some((overlay, overlay_hex, underlay, res)) = dialing.next().await {
        match res {
            Ok(session) => {
                debug!(target: "isheika::upload",
                    "session opened to {} ({})", overlay_hex, underlay);
                sessions.push(SessionEntry {
                    overlay,
                    overlay_hex,
                    underlay,
                    session: std::sync::Mutex::new(session),
                });
            }
            Err(e) => {
                warn!(target: "isheika::upload",
                    "session to {} failed: {}", overlay_hex, e);
            }
        }
    }
    Ok(sessions)
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
