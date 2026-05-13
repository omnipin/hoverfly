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
use nectar_primitives::file::{sync_split, GenericJoiner};
use nectar_primitives::store::{ChunkGet, ChunkStoreError, SyncChunkGet, SyncChunkPut};
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::dnsaddr::{resolve, DnsAddrError};
use crate::doh::Doh;
use crate::peers::{DialResult, Peer, PeerStore};
use crate::signer::SwarmSigner;
use crate::transport::{
    is_connection_dead, peer_price, PeerSession, PushOutcome, Transport, TransportError,
    GHOST_BALANCE_LIMIT_PLUR, GHOST_BALANCE_PREWARM_DENOMINATOR,
    GHOST_BALANCE_PREWARM_NUMERATOR,
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

/// Default number of peers raced in parallel per chunk fetch. Each peer
/// is given the full per-request timeout, but slow/dead peers no longer
/// block faster ones. Set to 1 to restore the legacy sequential behavior.
pub const DEFAULT_FETCH_CONCURRENCY: usize = 5;

/// Default number of peers dialed in parallel per discover round. Each
/// peer is held open for the hive `wait_per_peer` duration; parallelising
/// avoids 70-peer-round-2 dial chains taking `70 × wait` seconds.
pub const DEFAULT_DISCOVER_CONCURRENCY: usize = 16;

/// A `ChunkGet` adapter that routes requests through libp2p retrieval to the
/// closest peers in a peerlist. Up to `concurrency` requests are raced in
/// parallel; whichever peer responds first with a valid chunk wins, and
/// the rest are dropped. If a peer fails, the next-closest candidate is
/// launched until either a success is observed or `max_retries` peers have
/// been exhausted.
#[derive(Clone)]
pub struct NetworkedStore<'a> {
    transport: &'a Transport,
    peers: &'a PeerStore,
    max_retries: usize,
    concurrency: usize,
    /// Process-local cache of chunks already fetched. Used by mantaray
    /// manifest decoding (which re-visits forks) and any composite call
    /// chain that touches a chunk more than once. Cheap (a HashMap +
    /// Mutex); chunks are at most 4 KiB so even tens of thousands of
    /// entries cost only single-digit MB.
    ///
    /// `Clone` shares the cache: pass a clone of the store to nectar's
    /// `join` and our manifest walkers and they'll reuse fetched chunks.
    cache: std::sync::Arc<std::sync::Mutex<HashMap<ChunkAddress, AnyChunk<DEFAULT_BODY_SIZE>>>>,
}

impl<'a> NetworkedStore<'a> {
    /// Construct a store with sequential fetch (concurrency = 1).
    pub fn new(transport: &'a Transport, peers: &'a PeerStore, max_retries: usize) -> Self {
        Self {
            transport,
            peers,
            max_retries,
            concurrency: 1,
            cache: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Construct a store that races up to `concurrency` peers in parallel
    /// per chunk. `concurrency` is clamped to at least 1.
    pub fn with_concurrency(
        transport: &'a Transport,
        peers: &'a PeerStore,
        max_retries: usize,
        concurrency: usize,
    ) -> Self {
        Self {
            transport,
            peers,
            max_retries,
            concurrency,
            cache: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl<'a> NetworkedStore<'a> {
    /// Body of [`ChunkGet::get`]. Pulled into a private helper so the
    /// `ChunkGet` impl can be split per-target: native uses `async fn`,
    /// wasm wraps in `SendWrapper` to satisfy the trait's `+ Send` bound.
    async fn fetch_chunk_inner(
        &self,
        address: ChunkAddress,
    ) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, ChunkStoreError> {
        use futures::stream::{FuturesUnordered, StreamExt};

        // Cache hit: skip the entire network round-trip. Manifest decode
        // re-fetches the root multiple times during `walk_manifest` and
        // `lookup_manifest_path`; BMT joins re-visit intermediate nodes.
        if let Some(c) = self.cache.lock().unwrap().get(&address).cloned() {
            return Ok(c);
        }

        let mut bytes32 = [0u8; 32];
        bytes32.copy_from_slice(address.as_bytes());

        // Consider ALL peers in the peerstore, ordered by proximity to the
        // chunk address. Bee's retrieval protocol forwards requests through
        // the receiving peer's kademlia table to the chunk's neighborhood,
        // so even far peers can yield a result — but closest-first still
        // wins on average because nearby peers are more likely to have the
        // chunk locally and skip the forwarding cost.
        //
        // Peers that recently failed to dial (per `peers.json`'s reachability
        // cache) are pushed to the back of the candidate list so we don't
        // waste timeouts on known-dead peers up front.
        //
        // `max_retries == 0` means "no cap"; otherwise it bounds the number
        // of peer attempts before giving up.
        let now = crate::peers::now_unix();
        let (fresh, stale): (Vec<_>, Vec<_>) = self
            .peers
            .closest(&address, usize::MAX)
            .into_iter()
            .partition(|p| !p.is_recently_unreachable(now));
        let candidates: Vec<&Peer> = fresh.into_iter().chain(stale.into_iter()).collect();
        if candidates.is_empty() {
            return Err(ChunkStoreError::Other("no peers in peerlist".into()));
        }
        let attempt_cap = if self.max_retries == 0 {
            candidates.len()
        } else {
            self.max_retries.min(candidates.len())
        };

        let concurrency = self.concurrency.max(1);
        let mut candidates_iter = candidates.into_iter().take(attempt_cap);

        // Build a future that performs a single peer fetch and returns a
        // structured result. Captures peer metadata for logging and feeds
        // dial-result observations into the transport's reachability log.
        let log = self.transport.reachability_log().clone();
        let try_peer = |peer: &'a Peer| {
            let overlay = peer.overlay.clone();
            let underlay = peer.first_dialable_underlay();
            let log = log.clone();
            async move {
                let Some(underlay) = underlay else {
                    return (overlay, Err("no dialable underlay".to_string()));
                };
                let started = web_time::Instant::now();
                let res = self.transport.fetch_chunk(&underlay, &bytes32).await;
                let rtt_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
                match res {
                    Ok(delivery) => {
                        log.lock().unwrap().insert(
                            overlay.to_lowercase(),
                            crate::peers::DialResult::Success { rtt_ms },
                        );
                        match ContentChunk::<DEFAULT_BODY_SIZE>::try_from(delivery.data.as_slice()) {
                            Ok(chunk) => {
                                use nectar_primitives::Chunk as _;
                                if chunk.address() != &address {
                                    (overlay, Err("address mismatch".to_string()))
                                } else {
                                    (overlay, Ok(AnyChunk::from(chunk)))
                                }
                            }
                            Err(e) => (overlay, Err(format!("decode chunk: {e}"))),
                        }
                    }
                    Err(e) => {
                        log.lock().unwrap().insert(
                            overlay.to_lowercase(),
                            crate::peers::DialResult::Failure,
                        );
                        (overlay, Err(e.to_string()))
                    }
                }
            }
        };

        let mut inflight = FuturesUnordered::new();
        // Seed the initial window.
        for peer in candidates_iter.by_ref().take(concurrency) {
            inflight.push(try_peer(peer));
        }

        let mut last_err = String::from("no peers tried");
        while let Some((overlay, outcome)) = inflight.next().await {
            match outcome {
                Ok(chunk) => {
                    self.cache.lock().unwrap().insert(address, chunk.clone());
                    return Ok(chunk);
                }
                Err(e) => {
                    warn!(target: "isheika::fetch", "peer {} failed: {}", overlay, e);
                    last_err = e;
                    if let Some(next) = candidates_iter.next() {
                        inflight.push(try_peer(next));
                    }
                }
            }
        }
        Err(ChunkStoreError::Other(format!("all peers failed: {last_err}")))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> ChunkGet<DEFAULT_BODY_SIZE> for NetworkedStore<'a> {
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
        self.fetch_chunk_inner(*address).await
    }
}

// On wasm32 the inner future isn't `Send` (libp2p swarm + tokio_with_wasm
// timers aren't Send), but the nectar trait requires `+ Send`. Wrap in
// `SendWrapper`, which is safe because wasm32 is single-threaded — the
// future will always be polled on the same thread it was created on.
#[cfg(target_arch = "wasm32")]
impl<'a> ChunkGet<DEFAULT_BODY_SIZE> for NetworkedStore<'a> {
    type Error = ChunkStoreError;

    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl core::future::Future<Output = Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error>> + Send
    {
        let address = *address;
        send_wrapper::SendWrapper::new(self.fetch_chunk_inner(address))
    }
}

/// A `SyncChunkGet` adapter that wraps an async network fetch by blocking
/// the current thread (via `tokio::task::block_in_place`). Used by mantaray
/// manifest decoding which expects a synchronous chunk store.
///
/// Native-only: wasm32 has no multi-thread runtime to block on.
#[cfg(not(target_arch = "wasm32"))]
pub struct BlockingNetworkedStore<'a> {
    transport: &'a Transport,
    peers: &'a PeerStore,
    max_retries: usize,
    concurrency: usize,
    cache: Mutex<HashMap<ChunkAddress, AnyChunk<DEFAULT_BODY_SIZE>>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> BlockingNetworkedStore<'a> {
    pub fn new(transport: &'a Transport, peers: &'a PeerStore, max_retries: usize) -> Self {
        Self::with_concurrency(transport, peers, max_retries, 1)
    }

    pub fn with_concurrency(
        transport: &'a Transport,
        peers: &'a PeerStore,
        max_retries: usize,
        concurrency: usize,
    ) -> Self {
        Self {
            transport,
            peers,
            max_retries,
            concurrency,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> SyncChunkGet<DEFAULT_BODY_SIZE> for BlockingNetworkedStore<'a> {
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
        if let Some(c) = self.cache.lock().unwrap().get(address).cloned() {
            return Ok(c);
        }
        info!(target: "isheika::manifest", "blocking fetch for {}", address);
        let handle = tokio::runtime::Handle::current();
        let store = NetworkedStore::with_concurrency(self.transport, self.peers, self.max_retries, self.concurrency);
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
/// more of the swarm address space.
///
/// Uses [`DEFAULT_DISCOVER_CONCURRENCY`] parallel dials per round; for a
/// custom value use [`discover_recursive_with_concurrency`].
pub async fn discover_recursive(
    transport: &Transport,
    doh: &Doh,
    bootstrap: &Multiaddr,
    wait_per_peer: Duration,
    max_rounds: usize,
) -> Result<Vec<Peer>, ClientError> {
    discover_recursive_with_concurrency(
        transport,
        doh,
        bootstrap,
        wait_per_peer,
        max_rounds,
        DEFAULT_DISCOVER_CONCURRENCY,
    )
    .await
}

/// Like [`discover_recursive`], but with an explicit per-round concurrency
/// cap. `concurrency` controls how many peers are dialed in parallel; each
/// dial holds the hive stream open for `wait_per_peer`. With 70 peers in a
/// round and `concurrency=16`, the round completes in roughly
/// `ceil(70/16) × wait_per_peer` seconds rather than `70 × wait_per_peer`.
pub async fn discover_recursive_with_concurrency(
    transport: &Transport,
    doh: &Doh,
    bootstrap: &Multiaddr,
    wait_per_peer: Duration,
    max_rounds: usize,
    concurrency: usize,
) -> Result<Vec<Peer>, ClientError> {
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::collections::HashSet;

    let resolved = resolve(bootstrap, doh).await?;
    if resolved.is_empty() {
        return Err(ClientError::NoPeers(format!(
            "no ws/wss multiaddrs from {bootstrap}"
        )));
    }

    let concurrency = concurrency.max(1);
    let mut all: Vec<Peer> = Vec::new();
    let mut seen_overlays: HashSet<String> = HashSet::new();
    let mut frontier: Vec<Multiaddr> = resolved;

    for round in 0..max_rounds {
        if frontier.is_empty() {
            break;
        }
        info!(target: "isheika::discover",
            "round {} of {}: dialing {} peer(s) ({} in parallel)",
            round + 1, max_rounds, frontier.len(), concurrency);

        let mut next_frontier: Vec<Multiaddr> = Vec::new();
        let mut iter = std::mem::take(&mut frontier).into_iter();
        let mut inflight = FuturesUnordered::new();

        // Closure-as-fn (rather than an outer fn) keeps the borrow of
        // `transport` clean and produces a single async-block type so
        // FuturesUnordered can hold them all.
        let dial = |ma: Multiaddr| async move {
            debug!(target: "isheika::discover", "dialing {}", ma);
            let res = transport.discover_peers(&ma, wait_per_peer).await;
            (ma, res)
        };

        // Seed initial window.
        for _ in 0..concurrency {
            let Some(ma) = iter.next() else { break };
            inflight.push(dial(ma));
        }

        while let Some((ma, res)) = inflight.next().await {
            match res {
                Ok(batch) => {
                    debug!(target: "isheika::discover",
                        "{} returned {} peers", ma, batch.len());
                    for p in batch {
                        let key = p.overlay.to_lowercase();
                        if seen_overlays.insert(key) {
                            // Queue this peer as a discovery target for the
                            // next round if our transport can dial it. (Bee
                            // hive announcements often include both a TCP and
                            // a ws address per peer; native builds can use
                            // either, WASM builds only ws.)
                            if let Some(u) = p.first_dialable_underlay() {
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
            // Refill the window.
            if let Some(ma) = iter.next() {
                inflight.push(dial(ma));
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
    fetch_bytes_ex(transport, peers, root_hex, max_retries_per_chunk, 1).await
}

/// Like [`fetch_bytes`], but races up to `concurrency` peers in parallel
/// per chunk request.
pub async fn fetch_bytes_ex(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    max_retries_per_chunk: usize,
    concurrency: usize,
) -> Result<Vec<u8>, ClientError> {
    let root = parse_root(root_hex)?;
    let store = NetworkedStore::with_concurrency(transport, peers, max_retries_per_chunk, concurrency);
    // Drive nectar's BMT joiner with the same per-chunk concurrency as
    // our network store so deep trees don't bottleneck on its default
    // (8). Each chunk fetch already races peers internally; this is the
    // outer "chunks in flight" knob.
    let bytes = GenericJoiner::<_, nectar_primitives::file::mode::PlainMode, DEFAULT_BODY_SIZE>::new(store, root)
        .await?
        .with_concurrency(concurrency.max(1).max(8))
        .read_all()
        .await?;
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
    fetch_manifest_path_ex(transport, peers, root_hex, path, max_retries_per_chunk, 1).await
}

/// Like [`fetch_manifest_path`], but races up to `concurrency` peers in
/// parallel per chunk request.
pub async fn fetch_manifest_path_ex(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    path: &str,
    max_retries_per_chunk: usize,
    concurrency: usize,
) -> Result<(Vec<u8>, Option<String>), ClientError> {
    let root = parse_root(root_hex)?;
    // Single store shared between path lookup and content fetch; the
    // root chunk is hit by both phases so the cache saves a round-trip.
    let store =
        NetworkedStore::with_concurrency(transport, peers, max_retries_per_chunk, concurrency);
    let (target, content_type) = lookup_manifest_path(&store, root, path).await?;
    let bytes = GenericJoiner::<_, nectar_primitives::file::mode::PlainMode, DEFAULT_BODY_SIZE>::new(store, target)
        .await?
        .with_concurrency(concurrency.max(1).max(8))
        .read_all()
        .await?;
    Ok((bytes, content_type))
}

/// List entries in the mantaray manifest at `root_hex`.
pub async fn list_manifest(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    max_retries_per_chunk: usize,
) -> Result<Vec<ManifestEntry>, ClientError> {
    list_manifest_ex(transport, peers, root_hex, max_retries_per_chunk, 1).await
}

/// Like [`list_manifest`], but races up to `concurrency` peers in
/// parallel per chunk request.
pub async fn list_manifest_ex(
    transport: &Transport,
    peers: &PeerStore,
    root_hex: &str,
    max_retries_per_chunk: usize,
    concurrency: usize,
) -> Result<Vec<ManifestEntry>, ClientError> {
    let root = parse_root(root_hex)?;
    let store = NetworkedStore::with_concurrency(transport, peers, max_retries_per_chunk, concurrency);
    walk_manifest(&store, root, Vec::new()).await
}

async fn lookup_manifest_path(
    store: &NetworkedStore<'_>,
    root: ChunkAddress,
    path: &str,
) -> Result<(ChunkAddress, Option<String>), ClientError> {
    use crate::manifest::decode_node;
    let mut current = root;
    let mut remaining: &[u8] = path.as_bytes();
    let mut last_content_type: Option<String> = None;

    loop {
        let chunk = ChunkGet::<DEFAULT_BODY_SIZE>::get(store, &current)
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

/// Walk the manifest rooted at `addr`, fanning out fork descents in
/// parallel. Each level's forks are independent chunk fetches; serial
/// descent was the dominant cost on deep manifests (every level adds an
/// RTT). The store's internal cache makes repeat visits free.
fn walk_manifest<'a>(
    store: &'a NetworkedStore<'a>,
    addr: ChunkAddress,
    path_so_far: Vec<u8>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ManifestEntry>, ClientError>> + Send + 'a>>
{
    Box::pin(async move {
        use crate::manifest::decode_node;
        use futures::stream::{FuturesUnordered, StreamExt};

        let chunk = ChunkGet::<DEFAULT_BODY_SIZE>::get(store, &addr)
            .await
            .map_err(|e| ClientError::Manifest(format!("fetch node {addr}: {e}")))?;
        let node =
            decode_node(chunk.data()).map_err(|e| ClientError::Manifest(e.to_string()))?;

        let mut out = Vec::new();
        if let Some(entry_addr) = node.entry {
            let path = String::from_utf8_lossy(&path_so_far).into_owned();
            out.push(ManifestEntry {
                path,
                reference: hex::encode(entry_addr.as_bytes()),
                content_type: None,
            });
        }

        // Descend into each fork in parallel; each subtree's entries are
        // appended in arrival order.
        let mut children: FuturesUnordered<_> = node
            .forks
            .values()
            .map(|fork| {
                let mut next_path = path_so_far.clone();
                next_path.extend_from_slice(&fork.prefix);
                let r = fork.reference;
                walk_manifest(store, r, next_path)
            })
            .collect();
        while let Some(res) = children.next().await {
            out.extend(res?);
        }
        Ok(out)
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
    let mut stamp_in: Vec<(ChunkAddress, Vec<u8>)> = Vec::new();
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
            stamp_in.push((addr, wire_form(&chunk)));
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
    let unique_data_chunks = stamp_in.len();
    // 3. Add manifest chunks (also dedup; share the seen set).
    for (addr, wire) in manifest_chunks.iter() {
        let mut addr_bytes = [0u8; 32];
        addr_bytes.copy_from_slice(addr.as_bytes());
        if !seen.insert(addr_bytes) {
            continue;
        }
        stamp_in.push((*addr, wire.to_vec()));
    }
    info!(
        target: "isheika::upload",
        "collection: {} files ({} bytes) -> {} unique file chunks ({} duplicates skipped) + {} manifest chunks (root {})",
        files.len(), total_bytes, unique_data_chunks,
        raw_chunks.saturating_sub(unique_data_chunks),
        manifest_chunks.len(), manifest_root,
    );

    // 4. Stamp in parallel, then push everything concurrently.
    let work = stamp_chunks_parallel(&mut stamper, stamp_in)?;
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
    let (manifest_root, work) = prepare_upload_file_with_manifest(
        signer, batch_id_hex, depth, data, path, content_type,
    )?;
    push_chunks_concurrent(transport, peers, work, max_retries_per_chunk, concurrency).await?;
    Ok(manifest_root)
}

/// Daemon-mode single-file-with-manifest upload through a pre-built pool.
#[allow(clippy::too_many_arguments)]
pub async fn upload_file_with_manifest_with_pool(
    transport: &Transport,
    pool: &SessionPool,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    path: &str,
    content_type: Option<&str>,
    max_retries_per_chunk: usize,
    cache: Option<&crate::cache::ChunkCache>,
) -> Result<ChunkAddress, ClientError> {
    let (manifest_root, work) = prepare_upload_file_with_manifest(
        signer, batch_id_hex, depth, data, path, content_type,
    )?;
    if let Some(c) = cache {
        populate_cache(c, &work);
    }
    push_chunks_with_pool(transport, pool, work, max_retries_per_chunk).await?;
    Ok(manifest_root)
}

fn prepare_upload_file_with_manifest(
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    path: &str,
    content_type: Option<&str>,
) -> Result<(ChunkAddress, Vec<StampedChunk>), ClientError> {
    let batch_id = parse_batch_id(batch_id_hex)?;

    let (file_root, file_store) = sync_split::<DEFAULT_BODY_SIZE>(data)?;
    info!(target: "isheika::upload", "file: {} bytes -> {} chunks (root {})",
        data.len(), file_store.len(), file_root);

    let (manifest_root, manifest_chunks) =
        crate::manifest::build_single_entry_manifest(path, file_root, content_type)
            .map_err(|e| ClientError::Manifest(e.to_string()))?;
    info!(target: "isheika::upload", "manifest: {} chunks (root {})", manifest_chunks.len(), manifest_root);

    let mut stamper = build_stamper(signer, batch_id, depth);
    let mut stamp_in: Vec<(ChunkAddress, Vec<u8>)> =
        Vec::with_capacity(file_store.len() + manifest_chunks.len());
    for (addr, chunk) in file_store.into_chunks() {
        stamp_in.push((addr, wire_form(&chunk)));
    }
    for (addr, wire) in manifest_chunks {
        stamp_in.push((addr, wire.to_vec()));
    }
    let work = stamp_chunks_parallel(&mut stamper, stamp_in)?;
    Ok((manifest_root, work))
}

/// Convert a nectar AnyChunk into the wire form `span_LE_8 || payload`.
fn wire_form(chunk: &AnyChunk<DEFAULT_BODY_SIZE>) -> Vec<u8> {
    let mut wire = Vec::with_capacity(8 + chunk.data().len());
    wire.extend_from_slice(&chunk.span().to_le_bytes());
    wire.extend_from_slice(chunk.data());
    wire
}

/// A chunk pre-stamped and ready for the wire.
#[derive(Clone)]
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

/// Stamp a batch of (address, wire) pairs, signing in parallel via rayon.
///
/// secp256k1 signing is ~ms per chunk and serial on `stamp_chunk` — for
/// big uploads (10 MB ≈ 2500 chunks) this can dominate a few seconds of
/// CPU. Split the operation: the issuer-side `prepare_stamp` (index
/// allocation, no crypto) stays serial because the issuer requires
/// `&mut`, then the digest signing fans out across cores.
///
/// Native-only: wasm32 is single-threaded so rayon has no thread pool
/// to spread work over; the serial path is just as fast there.
#[cfg(not(target_arch = "wasm32"))]
fn stamp_chunks_parallel(
    stamper: &mut BatchStamper<MemoryIssuer, alloy_signer_local::PrivateKeySigner>,
    work: Vec<(ChunkAddress, Vec<u8>)>,
) -> Result<Vec<StampedChunk>, ClientError> {
    use nectar_postage::current_timestamp;
    use nectar_postage_issuer::StampIssuer;
    use rayon::prelude::*;

    // Phase 1 (serial): allocate batch indices & build digests.
    let timestamp = current_timestamp();
    let mut prepared: Vec<(ChunkAddress, Vec<u8>, nectar_postage::StampDigest)> =
        Vec::with_capacity(work.len());
    for (addr, wire) in work {
        let digest = stamper
            .issuer_mut()
            .prepare_stamp(&addr, timestamp)
            .map_err(|e| ClientError::Stamp(e.to_string()))?;
        prepared.push((addr, wire, digest));
    }

    // Phase 2 (parallel): sign each digest. `PrivateKeySigner: Sync` so
    // the same instance can be shared across rayon worker threads.
    let signer: &alloy_signer_local::PrivateKeySigner = stamper.signer();
    let stamped: Result<Vec<StampedChunk>, ClientError> = prepared
        .into_par_iter()
        .map(|(addr, wire, digest)| {
            use alloy_signer::SignerSync;
            let prehash = digest.to_prehash();
            let sig = signer
                .sign_message_sync(prehash.as_slice())
                .map_err(|e| ClientError::Stamp(e.to_string()))?;
            let stamp = BatchStamper::<MemoryIssuer, alloy_signer_local::PrivateKeySigner>::stamp_from_signature(&digest, sig);
            let stamp_bytes = stamp.to_bytes().to_vec();
            let mut addr32 = [0u8; 32];
            addr32.copy_from_slice(addr.as_bytes());
            Ok(StampedChunk { addr: addr32, wire, stamp: stamp_bytes })
        })
        .collect();
    stamped
}

#[cfg(target_arch = "wasm32")]
fn stamp_chunks_parallel(
    stamper: &mut BatchStamper<MemoryIssuer, alloy_signer_local::PrivateKeySigner>,
    work: Vec<(ChunkAddress, Vec<u8>)>,
) -> Result<Vec<StampedChunk>, ClientError> {
    work.into_iter()
        .map(|(addr, wire)| stamp_chunk(stamper, &addr, wire))
        .collect()
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
    let (root, work) = prepare_upload_bytes(signer, batch_id_hex, depth, data)?;
    push_chunks_concurrent(transport, peers, work, max_retries_per_chunk, concurrency).await?;
    Ok(root)
}

/// Daemon-mode raw upload: split + stamp + push through a pre-built
/// session pool. Skips the per-request pool-fill dial parade.
#[allow(clippy::too_many_arguments)]
pub async fn upload_bytes_with_pool(
    transport: &Transport,
    pool: &SessionPool,
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
    max_retries_per_chunk: usize,
    cache: Option<&crate::cache::ChunkCache>,
) -> Result<ChunkAddress, ClientError> {
    let (root, work) = prepare_upload_bytes(signer, batch_id_hex, depth, data)?;
    if let Some(c) = cache {
        populate_cache(c, &work);
    }
    push_chunks_with_pool(transport, pool, work, max_retries_per_chunk).await?;
    Ok(root)
}

/// Populate the daemon's [`ChunkCache`] from a batch of stamped chunks
/// produced by `prepare_upload_*`. Called by the `_with_pool` variants
/// before they hand `work` over to `push_chunks_with_pool`, so the
/// cache is hot the moment our peers (or bzz.limo) start asking us
/// for these chunks via retrieval — they're served directly from RAM
/// without waiting for pushsync propagation.
fn populate_cache(cache: &crate::cache::ChunkCache, work: &[StampedChunk]) {
    use bytes::Bytes;
    cache.put_many(
        work.iter()
            .map(|c| (c.addr, Bytes::copy_from_slice(&c.wire), Bytes::copy_from_slice(&c.stamp))),
    );
}

/// Split + stamp data, returning the root and the stamped chunks ready
/// for pushsync. Pure CPU; no network. Pulled out so both the one-shot
/// (`upload_bytes_ex`) and daemon (`upload_bytes_with_pool`) paths
/// share the BMT/stamp work.
fn prepare_upload_bytes(
    signer: &SwarmSigner,
    batch_id_hex: &str,
    depth: u8,
    data: &[u8],
) -> Result<(ChunkAddress, Vec<StampedChunk>), ClientError> {
    let batch_id = parse_batch_id(batch_id_hex)?;

    let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data)?;
    info!(target: "isheika::upload", "split {} bytes into {} chunks (root {})",
        data.len(), store.len(), root);

    let mut stamper = build_stamper(signer, batch_id, depth);

    let snapshot = store.into_chunks();
    let stamp_in: Vec<(ChunkAddress, Vec<u8>)> = snapshot
        .iter()
        .map(|(addr, chunk)| (*addr, wire_form(chunk)))
        .collect();
    let work = stamp_chunks_parallel(&mut stamper, stamp_in)?;
    Ok((root, work))
}

/// A session and the peer overlay it talks to, kept together so we can
/// route each chunk to the session whose peer is closest to it. The
/// `PeerSession` inside is replaced on the fly when the driver retires
/// itself after accumulating too much client-side mirrored ghost balance;
/// a
/// brand-new libp2p connection is dialed to reset bee's `ghostBalance`.
struct SessionEntry {
    overlay: SwarmAddress,
    overlay_hex: String,
    underlay: libp2p::Multiaddr,
    session: std::sync::Mutex<PeerSession>,
    /// Pre-warmed replacement session. Populated by the upload loop
    /// once the active session crosses the ghost-balance pre-warm
    /// threshold; if present, `try_push_with_rotation` swaps it in
    /// instead of dialing synchronously. `bool` flags whether a pre-warm
    /// is already in flight (so we don't queue two for the same entry).
    pending: std::sync::Mutex<Option<PeerSession>>,
    prewarm_inflight: std::sync::atomic::AtomicBool,
}

impl SessionEntry {
    fn snapshot(&self) -> PeerSession {
        self.session.lock().expect("session mutex poisoned").clone()
    }

    /// Replace the stored session with `new`. The previous session's
    /// `cmd_tx` is dropped, which signals its driver to shut down once
    /// any in-flight pushes finish.
    fn replace(&self, new: PeerSession) {
        let mut guard = self.session.lock().expect("session mutex poisoned");
        *guard = new;
    }

    /// Take a pre-warmed session if one is ready. Returns `None` if no
    /// pre-warm has completed yet — caller falls back to dialing sync.
    fn take_pending(&self) -> Option<PeerSession> {
        self.pending.lock().expect("pending mutex poisoned").take()
    }

    /// Store a freshly-dialed session as the pre-warmed replacement.
    fn store_pending(&self, session: PeerSession) {
        let mut guard = self.pending.lock().expect("pending mutex poisoned");
        *guard = Some(session);
    }
}

const PREWARM_GHOST_BALANCE_PLUR: u64 =
    GHOST_BALANCE_LIMIT_PLUR * GHOST_BALANCE_PREWARM_NUMERATOR / GHOST_BALANCE_PREWARM_DENOMINATOR;

/// A long-lived pool of peer sessions usable across multiple uploads.
/// Construct with [`SessionPool::open`]. Pre-warm rotation, mid-upload
/// session retirement, and accounting state are all handled internally —
/// once opened, a pool can be re-used (e.g. by the daemon) for many
/// upload requests without paying the dial-fill cost each time.
pub struct SessionPool {
    sessions: std::sync::Arc<Vec<SessionEntry>>,
}

impl SessionPool {
    /// Open up to `target_size` sessions to peers selected by proximity
    /// to the zero address (a stable ordering). Skips recently-failed
    /// peers and dials wider than `target_size` in parallel to absorb
    /// the high failure rate of stale mainnet hive announcements.
    pub async fn open(
        transport: &Transport,
        peers: &PeerStore,
        target_size: usize,
    ) -> Result<Self, ClientError> {
        let sessions = open_session_pool(transport, peers, target_size).await?;
        if sessions.is_empty() {
            return Err(ClientError::NoPeers("no reachable ws peers".into()));
        }
        Ok(Self { sessions: std::sync::Arc::new(sessions) })
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

/// Build a one-shot pool sized for `work.len()` and push everything
/// through it. The pool is dropped on return; for daemon-style reuse,
/// build a [`SessionPool`] separately and call
/// [`push_chunks_with_pool`].
async fn push_chunks_concurrent(
    transport: &Transport,
    peers: &PeerStore,
    work: Vec<StampedChunk>,
    max_retries: usize,
    concurrency: usize,
) -> Result<(), ClientError> {
    if work.is_empty() {
        return Ok(());
    }
    // Adaptive sizing: never open more sessions than we have chunks to
    // push. A 1888-byte file is 2 chunks; opening 32 sessions for that
    // wastes ~30 s on dial timeouts when the user picked a high
    // --concurrency for the upload-machine defaults. Floor at 4 so very
    // small uploads still get the multi-peer race for resilience.
    let target_sessions = concurrency.max(1).min(work.len().max(4));
    let pool = SessionPool::open(transport, peers, target_sessions).await?;
    info!(
        target: "isheika::upload",
        "opened {} peer session(s), pushing {} chunks",
        pool.len(),
        work.len()
    );
    push_chunks_with_pool(transport, &pool, work, max_retries).await
}

/// Push `work` through an existing pool. Used by the daemon to amortise
/// pool-fill cost across many upload requests; the CLI builds a fresh
/// pool per invocation via [`push_chunks_concurrent`].
pub(crate) async fn push_chunks_with_pool(
    transport: &Transport,
    session_pool: &SessionPool,
    work: Vec<StampedChunk>,
    max_retries: usize,
) -> Result<(), ClientError> {
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    if work.is_empty() {
        return Ok(());
    }
    let pool = session_pool.sessions.clone();
    if pool.is_empty() {
        return Err(ClientError::NoPeers("no reachable ws peers".into()));
    }
    let total = work.len();
    let pushed = Arc::new(AtomicUsize::new(0));

    // For each chunk we walk sessions in descending-proximity order. With
    // many sessions and yamux multiplexing, keeping pool*4 requests in
    // flight saturates round-trip latency without overcommitting any one
    // session.
    let buffer = pool.len().saturating_mul(4).max(pool.len());

    // Per-chunk parallelism (matches bee's `pushsync.maxMultiplexForwards`
    // + `preemptiveInterval`): start with the closest peer, then every
    // `PREEMPT_INTERVAL` fire another push to the next-closest peer in
    // parallel, returning on the first valid receipt.
    const CHUNK_PEER_PARALLELISM: usize = 3;
    // Shorter than bee's 5s — bee tunes for inter-node forwarding RTTs;
    // our pushes are a single hop from the client and a stuck peer at
    // 2.5s usually means a dead path, not a slow forwarder.
    const PREEMPT_INTERVAL: Duration = Duration::from_millis(2500);

    let dispatch = |chunk: StampedChunk| {
        let pool = pool.clone();
        let pushed = pushed.clone();
        let transport = transport;
        // Wrap the chunk in Arc once per dispatch so preemptive retries
        // share its (potentially 4 KiB) wire bytes instead of cloning
        // per attempt.
        let chunk = Arc::new(chunk);
        async move {
            use futures::stream::{FuturesUnordered, StreamExt};

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

            let cap = max_retries.max(1).min(order.len());
            let mut order_iter = order.iter().take(cap).copied();

            let attempt = |idx: usize, attempt_no: usize| {
                let pool = pool.clone();
                let chunk = chunk.clone();
                async move {
                    let entry = &pool[idx];
                    let mut peer_overlay = [0u8; 32];
                    peer_overlay.copy_from_slice(entry.overlay.as_bytes());
                    let price = peer_price(&peer_overlay, &chunk.addr);
                    let outcome = try_push_with_rotation(entry, &chunk, price, transport).await;
                    (idx, attempt_no, price, outcome)
                }
            };

            let mut inflight = FuturesUnordered::new();
            let mut attempt_no = 0usize;

            // Seed with the first peer.
            if let Some(idx) = order_iter.next() {
                attempt_no += 1;
                inflight.push(attempt(idx, attempt_no));
            }

            // Two outer rounds: if every peer reports Overdraft on the first
            // pass we sleep briefly to let pseudosettle refresh free credit,
            // then retry. After that, treat as a hard failure.
            let mut last_err: Option<TransportError> = None;
            let mut overdrafts = 0usize;
            let mut errors = 0usize;
            // Box-pinned sleep that we recreate on each fire / push-refill;
            // PREEMPT_INTERVAL then counts from the most recent push event.
            // (Native tokio has Sleep::reset, but tokio_with_wasm doesn't,
            // so a re-pin is the portable common subset.)
            let mut sleep: std::pin::Pin<Box<tokio::time::Sleep>> =
                Box::pin(tokio::time::sleep(PREEMPT_INTERVAL));

            loop {
                tokio::select! {
                    biased;

                    Some((idx, n, price, outcome)) = inflight.next(), if !inflight.is_empty() => {
                        let entry = &pool[idx];
                        match outcome {
                            Ok(PushOutcome::Receipt(_)) => {
                                let done = pushed.fetch_add(1, Ordering::Relaxed) + 1;
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
                                overdrafts += 1;
                                debug!(target: "isheika::upload",
                                    "overdraft on {} (po={}); trying next peer",
                                    entry.overlay_hex,
                                    chunk_addr.proximity(&entry.overlay));
                            }
                            Err(e) => {
                                errors += 1;
                                warn!(target: "isheika::upload",
                                    "push attempt {} via {} (po={}) failed: {}",
                                    n, entry.overlay_hex,
                                    chunk_addr.proximity(&entry.overlay), e);
                                last_err = Some(e);
                            }
                        }
                        // Top up the in-flight window with the next-closest peer.
                        if let Some(idx) = order_iter.next() {
                            attempt_no += 1;
                            inflight.push(attempt(idx, attempt_no));
                        }
                        // Reset preempt timer: we just observed activity, so
                        // start the next PREEMPT_INTERVAL countdown fresh.
                        sleep = Box::pin(tokio::time::sleep(PREEMPT_INTERVAL));
                    }

                    _ = sleep.as_mut(), if inflight.len() < CHUNK_PEER_PARALLELISM => {
                        // Preemptive fanout: closest peer hasn't returned within
                        // `PREEMPT_INTERVAL`, so race another peer in parallel.
                        if let Some(idx) = order_iter.next() {
                            attempt_no += 1;
                            inflight.push(attempt(idx, attempt_no));
                        }
                        // Restart preempt countdown.
                        sleep = Box::pin(tokio::time::sleep(PREEMPT_INTERVAL));
                    }

                    else => break,
                }
            }

            // All candidates within `cap` exhausted. If everyone
            // overdrafted (no real errors), prefer trying more peers
            // beyond `cap` over sleeping — the pool has many peers, and
            // a fresh peer's credit ceiling is uncorrelated with our
            // already-attempted ones'. Only fall back to a 1.1 s
            // refresh-wait + closest-N retry if there genuinely are no
            // more peers in the pool.
            if errors == 0 && overdrafts > 0 {
                let already_attempted = attempt_no;
                let extra: Vec<usize> = order.iter().skip(already_attempted).copied().collect();
                if !extra.is_empty() {
                    debug!(target: "isheika::upload",
                        "all {} attempted peers overdrafted; trying {} more",
                        already_attempted, extra.len());
                    for idx in extra {
                        let entry = &pool[idx];
                        let mut peer_overlay = [0u8; 32];
                        peer_overlay.copy_from_slice(entry.overlay.as_bytes());
                        let price = peer_price(&peer_overlay, &chunk.addr);
                        match try_push_with_rotation(entry, &chunk, price, transport).await {
                            Ok(PushOutcome::Receipt(_)) => {
                                let done = pushed.fetch_add(1, Ordering::Relaxed) + 1;
                                if done % 50 == 0 || done == total {
                                    info!(target: "isheika::upload",
                                        "pushed {}/{} chunks (latest via {} po={})",
                                        done, total, entry.overlay_hex,
                                        chunk_addr.proximity(&entry.overlay));
                                }
                                return Ok::<_, ClientError>(());
                            }
                            Ok(PushOutcome::Overdraft) => continue,
                            Err(e) => {
                                last_err = Some(e);
                                break;
                            }
                        }
                    }
                } else {
                    debug!(target: "isheika::upload",
                        "all peers overdrafted and no more candidates; waiting for refresh");
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    for idx in order.iter().take(cap).copied() {
                        let entry = &pool[idx];
                        let mut peer_overlay = [0u8; 32];
                        peer_overlay.copy_from_slice(entry.overlay.as_bytes());
                        let price = peer_price(&peer_overlay, &chunk.addr);
                        match try_push_with_rotation(entry, &chunk, price, transport).await {
                            Ok(PushOutcome::Receipt(_)) => {
                                let done = pushed.fetch_add(1, Ordering::Relaxed) + 1;
                                if done % 50 == 0 || done == total {
                                    info!(target: "isheika::upload",
                                        "pushed {}/{} chunks (latest via {} po={})",
                                        done, total, entry.overlay_hex,
                                        chunk_addr.proximity(&entry.overlay));
                                }
                                return Ok::<_, ClientError>(());
                            }
                            Ok(PushOutcome::Overdraft) => continue,
                            Err(e) => {
                                last_err = Some(e);
                                break;
                            }
                        }
                    }
                }
            }

            Err(ClientError::NoPeers(format!(
                "all {} attempts failed: {}",
                cap,
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

    // Separate side-queue of background dials used to pre-warm session
    // replacements. Each dial runs concurrently with chunk pushes, so
    // when an active session retires on ghost-balance the
    // replacement is already open instead of forcing the chunk that
    // triggered the rotation to pay the dial cost synchronously.
    //
    // The future borrows `transport` for `'_`, so we use BoxFuture<'_>
    // from the futures crate (which carries an explicit lifetime),
    // not the more common +'static dyn pinning.
    #[cfg(not(target_arch = "wasm32"))]
    let mut prewarm_dials: FuturesUnordered<
        futures::future::BoxFuture<'_, (usize, Result<PeerSession, TransportError>)>,
    > = FuturesUnordered::new();
    #[cfg(target_arch = "wasm32")]
    let mut prewarm_dials: FuturesUnordered<
        futures::future::LocalBoxFuture<'_, (usize, Result<PeerSession, TransportError>)>,
    > = FuturesUnordered::new();

    let maybe_prewarm = |idx: usize, prewarm_dials: &mut FuturesUnordered<_>| {
        let entry = &pool[idx];
        let ghost = entry.snapshot().ghost_balance_plur();
        if ghost >= PREWARM_GHOST_BALANCE_PLUR
            && entry
                .prewarm_inflight
                .compare_exchange(
                    false,
                    true,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            let underlay = entry.underlay.clone();
            #[cfg(not(target_arch = "wasm32"))]
            prewarm_dials.push(Box::pin(async move {
                let res = transport.open_session(&underlay).await;
                (idx, res)
            }) as futures::future::BoxFuture<'_, _>);
            #[cfg(target_arch = "wasm32")]
            prewarm_dials.push(Box::pin(async move {
                let res = transport.open_session(&underlay).await;
                (idx, res)
            }) as futures::future::LocalBoxFuture<'_, _>);
        }
    };

    let mut first_err: Option<ClientError> = None;
    loop {
        tokio::select! {
            biased;

            Some(res) = inflight.next(), if !inflight.is_empty() => {
                match res {
                    Ok(()) => {
                        // Opportunistically pre-warm any session that's
                        // approaching its rotation limit. compare_exchange
                        // ensures only one dial per entry at a time.
                        for i in 0..pool.len() {
                            maybe_prewarm(i, &mut prewarm_dials);
                        }
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

            Some((idx, res)) = prewarm_dials.next(), if !prewarm_dials.is_empty() => {
                let entry = &pool[idx];
                entry.prewarm_inflight.store(false, Ordering::Release);
                match res {
                    Ok(session) => {
                        debug!(target: "isheika::upload",
                            "pre-warm dial for {} ready", entry.overlay_hex);
                        entry.store_pending(session);
                    }
                    Err(e) => {
                        debug!(target: "isheika::upload",
                            "pre-warm dial for {} failed: {}", entry.overlay_hex, e);
                    }
                }
            }

            else => break,
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
        // Connection is gone. Prefer a pre-warmed replacement (zero
        // wait) if the upload loop has dialed one for us; otherwise
        // dial sync. Either way, resets bee's ghostBalance via the
        // fresh `Connect()` and retries the push.
        Err(_) => {
            let fresh = match entry.take_pending() {
                Some(s) => {
                    debug!(target: "isheika::upload",
                        "rotated to pre-warmed session for {}", entry.overlay_hex);
                    s
                }
                None => {
                    let s = transport.open_session(&entry.underlay).await?;
                    debug!(target: "isheika::upload",
                        "rotated session to {} (sync dial)", entry.overlay_hex);
                    s
                }
            };
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
/// How many session dials we keep in flight at once while filling the
/// session pool. Mainnet peerlists are heavy with unreachable peers
/// (NAT'd, gone offline since being announced, etc.) so we need a wide
/// in-flight window to find `max_sessions` reachable ones quickly. Bee's
/// per-incoming-connection cost is cheap, and these dials only run once
/// per upload.
const SESSION_DIAL_PARALLELISM: usize = 32;

async fn open_session_pool(
    transport: &Transport,
    peers: &PeerStore,
    max_sessions: usize,
) -> Result<Vec<SessionEntry>, ClientError> {
    let log = transport.reachability_log();
    use futures::stream::{FuturesUnordered, StreamExt};

    // Walk every peer in the peerstore in a stable (closest-to-zero)
    // order. We keep `dial_parallelism` dials in flight at once and take
    // the first `max_sessions` successful ones — most candidate addresses
    // on mainnet are stale, so a wide dial window finds reachable peers
    // ~order-of-magnitude faster than a `max_sessions`-wide window.
    //
    // Peers we've recently failed to dial (within RECENT_FAILURE_SECS)
    // are moved to the end of the candidate list rather than dropped:
    // they're still tried if no fresher peer answers, but won't burn
    // 10 s timeouts at the front of the dial parade.
    let now = crate::peers::now_unix();
    let zero = ChunkAddress::new([0u8; 32]);
    let (fresh, stale): (Vec<_>, Vec<_>) = peers
        .closest(&zero, usize::MAX)
        .into_iter()
        .filter_map(|p| {
            let underlay = p.first_dialable_underlay()?;
            let overlay = p.overlay_address()?;
            Some((overlay, p.overlay.clone(), underlay, p.is_recently_unreachable(now)))
        })
        .partition(|(_, _, _, stale)| !stale);
    let candidates: Vec<(SwarmAddress, String, Multiaddr)> = fresh
        .into_iter()
        .chain(stale.into_iter())
        .map(|(o, hex, u, _)| (o, hex, u))
        .collect();
    if candidates.is_empty() {
        return Err(ClientError::NoPeers("peerlist empty".into()));
    }

    let dial_parallelism = SESSION_DIAL_PARALLELISM.max(max_sessions);
    let mut iter = candidates.into_iter();
    let mut dialing = FuturesUnordered::new();
    let dial = |overlay: SwarmAddress, overlay_hex: String, underlay: Multiaddr| async move {
        let started = web_time::Instant::now();
        let result = transport.open_session(&underlay).await;
        let rtt_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        (overlay, overlay_hex, underlay, result, rtt_ms)
    };

    // Seed initial in-flight window — wider than max_sessions to absorb
    // the high failure rate of mainnet peer dials.
    for (overlay, overlay_hex, underlay) in iter.by_ref().take(dial_parallelism) {
        dialing.push(dial(overlay, overlay_hex, underlay));
    }

    let mut sessions = Vec::with_capacity(max_sessions);
    while let Some((overlay, overlay_hex, underlay, res, rtt_ms)) = dialing.next().await {
        match res {
            Ok(session) => {
                debug!(target: "isheika::upload",
                    "session opened to {} ({}) in {} ms",
                    overlay_hex, underlay, rtt_ms);
                log.lock().unwrap().insert(
                    overlay_hex.to_lowercase(),
                    DialResult::Success { rtt_ms },
                );
                sessions.push(SessionEntry {
                    overlay,
                    overlay_hex,
                    underlay,
                    session: std::sync::Mutex::new(session),
                    pending: std::sync::Mutex::new(None),
                    prewarm_inflight: std::sync::atomic::AtomicBool::new(false),
                });
                if sessions.len() >= max_sessions {
                    break;
                }
            }
            Err(e) => {
                warn!(target: "isheika::upload",
                    "session to {} failed: {}", overlay_hex, e);
                log.lock().unwrap()
                    .insert(overlay_hex.to_lowercase(), DialResult::Failure);
            }
        }
        // Keep the in-flight window full so we don't sit waiting on a few
        // remaining timeouts when many candidates remain.
        if let Some((overlay, overlay_hex, underlay)) = iter.next() {
            dialing.push(dial(overlay, overlay_hex, underlay));
        }
    }
    Ok(sessions)
}

/// Quick reachability probe: dial each peer in parallel, record success/
/// failure (with rtt) into the reachability log without keeping the
/// resulting sessions open. Called optionally by `discover` after a hive
/// round to pre-prune dead peers from `peers.json`.
pub async fn healthcheck_peers(
    transport: &Transport,
    peers: &PeerStore,
    concurrency: usize,
) {
    let log = transport.reachability_log();
    use futures::stream::{FuturesUnordered, StreamExt};

    let zero = ChunkAddress::new([0u8; 32]);
    let candidates: Vec<_> = peers
        .closest(&zero, usize::MAX)
        .into_iter()
        .filter_map(|p| {
            let underlay = p.first_dialable_underlay()?;
            Some((p.overlay.clone(), underlay))
        })
        .collect();
    let total = candidates.len();

    let concurrency = concurrency.max(1);
    let mut iter = candidates.into_iter();
    let mut inflight = FuturesUnordered::new();
    let probe = |overlay_hex: String, underlay: Multiaddr| async move {
        let started = web_time::Instant::now();
        let res = transport.open_session(&underlay).await;
        let rtt_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        (overlay_hex, res.is_ok(), rtt_ms)
    };
    for (overlay_hex, underlay) in iter.by_ref().take(concurrency) {
        inflight.push(probe(overlay_hex, underlay));
    }
    let mut reached = 0usize;
    while let Some((overlay_hex, ok, rtt_ms)) = inflight.next().await {
        if ok {
            reached += 1;
        }
        log.lock().unwrap().insert(
            overlay_hex.to_lowercase(),
            if ok { DialResult::Success { rtt_ms } } else { DialResult::Failure },
        );
        if let Some((overlay_hex, underlay)) = iter.next() {
            inflight.push(probe(overlay_hex, underlay));
        }
    }
    info!(target: "isheika::discover",
        "healthcheck: {}/{} peers reachable", reached, total);
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
