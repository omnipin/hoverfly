//! Erasure-coding-aware BMT file joiner (whole-file read).
//!
//! Ported from bee `pkg/file/joiner` + `pkg/file/redundancy/getter`, restricted
//! to the full-file read hoverfly needs (`read_all`). Given a root reference to
//! an erasure-coded object, it walks the BMT tree and, for every intermediate
//! node whose data children can't all be retrieved, reconstructs the missing
//! data chunks from that node's parity siblings using Reed–Solomon
//! ([`super::reedsolomon`]).
//!
//! The traversal mirrors bee exactly:
//! - each intermediate node's payload is `shard_cnt` data references followed by
//!   `parity_cnt` parity references;
//! - the per-node `(shard_cnt, parity_cnt)` split is derived from the node's own
//!   subtree span and redundancy level via [`super::reference_count`];
//! - a child's own redundancy level/parity is re-read from *its* span, so mixed
//!   levels down the tree are handled;
//! - leaf data chunks are copied out in tree order to reconstruct the file.
//!
//! Data-shard reconstruction uses the RS invariant that the `shard_cnt` data
//! chunks plus `parity_cnt` parity chunks of one node are an RS codeword: any
//! `shard_cnt` of those `shard_cnt + parity_cnt` chunks suffice to recover all
//! data chunks. Parity chunks are *padded to full chunk size* on encode, so a
//! recovered short last data chunk is truncated back to its span length.

use futures::stream::{FuturesUnordered, StreamExt};
use nectar_primitives::bmt::{DEFAULT_BODY_SIZE, SPAN_SIZE};
use nectar_primitives::chunk::{AnyChunk, ChunkAddress};
use nectar_primitives::store::ChunkGet;
use tracing::{debug, info};

use super::reedsolomon::{ReedSolomon, RsError};
use super::{
    CHUNK_SIZE, Level, chunk_addresses, chunk_payload_size, decode_span, is_level_encoded,
    reference_count,
};

/// Errors from the erasure joiner.
#[derive(Debug, thiserror::Error)]
pub enum ErasureError {
    #[error("erasure: chunk fetch failed: {0}")]
    Fetch(String),
    #[error("erasure: malformed tree: {0}")]
    Malformed(&'static str),
    #[error("erasure: reed-solomon: {0}")]
    Rs(#[from] RsError),
    #[error("erasure: could not recover an intermediate node ({have}/{need} shards available)")]
    Unrecoverable { have: usize, need: usize },
}

/// A chunk's full wire form split into its decoded parts.
struct DecodedChunk {
    /// Raw span (little-endian, level byte intact) as a u64.
    raw_span: u64,
    /// Payload (chunk data without the 8-byte span).
    data: Vec<u8>,
}

impl DecodedChunk {
    fn from_any(chunk: &AnyChunk<DEFAULT_BODY_SIZE>) -> Self {
        DecodedChunk {
            raw_span: chunk.span(),
            data: chunk.data().to_vec(),
        }
    }

    /// The redundancy level and true length encoded in the span.
    fn level_and_len(&self) -> (Level, u64) {
        decode_span(&self.raw_span.to_le_bytes())
    }
}

/// Fetch and reconstruct a complete erasure-coded object by its root reference.
///
/// `root` must be the 32-byte content reference. The root chunk is fetched via
/// `store`; if its span carries no redundancy level this returns an error so the
/// caller can fall back to the plain joiner (callers should check
/// [`super::is_level_encoded`] on the root span first — see
/// [`root_is_erasure_coded`]).
pub async fn fetch_erasure_bytes<G>(store: &G, root: ChunkAddress) -> Result<Vec<u8>, ErasureError>
where
    G: ChunkGet<DEFAULT_BODY_SIZE>,
{
    fetch_erasure_bytes_progress(store, root, None).await
}

/// A byte-progress reporter for the erasure join, called with the running
/// count of leaf bytes appended so far. `Send + Sync` so the boxed recursion
/// future stays `Send` on native.
type EcProgress<'a> = &'a (dyn Fn(usize) + Send + Sync);

/// Like [`fetch_erasure_bytes`], but drives an optional byte-progress callback
/// as leaf data lands (the total is the file's decoded span, known up front).
pub async fn fetch_erasure_bytes_progress<G>(
    store: &G,
    root: ChunkAddress,
    progress: Option<&crate::client::ProgressFn>,
) -> Result<Vec<u8>, ErasureError>
where
    G: ChunkGet<DEFAULT_BODY_SIZE>,
{
    let root_chunk = store
        .get(&root)
        .await
        .map_err(|e| ErasureError::Fetch(e.to_string()))?;
    let root_dc = DecodedChunk::from_any(&root_chunk);
    let (level, span) = root_dc.level_and_len();

    let mut out = Vec::with_capacity(span as usize);
    let root_parity = if level == Level::None {
        0
    } else {
        reference_count(span, level).1
    };
    info!(
        target: "hoverfly::erasure",
        "joining erasure-coded object: {span} bytes, level {level:?}, root node has {root_parity} parities"
    );

    // Wrap the caller's ProgressFn into a reporter closure that tracks total
    // bytes appended and reports (done, span). `None` → a no-op reporter.
    let total = span as usize;
    let reporter: Box<dyn Fn(usize) + Send + Sync> = match progress {
        Some(cb) => {
            let cb = cb.clone();
            Box::new(move |done: usize| cb(done.min(total), total))
        }
        None => Box::new(|_done: usize| {}),
    };
    reporter(0);

    read_node(
        store,
        &root_dc.data,
        span,
        root_parity,
        &mut out,
        &*reporter,
    )
    .await?;
    // The recursion copies exactly `span` bytes of leaf data in order.
    out.truncate(span as usize);
    reporter(out.len());
    Ok(out)
}

/// Boxed recursion future. `Send` on native so a fetch can run inside a
/// multi-thread `tokio::spawn` (the daemon does this); `!Send` on wasm where the
/// store is `Rc`-backed. The actual Send-ness follows from the store `G`
/// (`ChunkGet::get` is `Send` on native via `MaybeSend`), so this only relaxes
/// the trait-object bound per target — identical to `MaybeSendWalk` in
/// `client.rs`.
#[cfg(not(target_arch = "wasm32"))]
type ReadNodeFut<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ErasureError>> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type ReadNodeFut<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ErasureError>> + 'a>>;

/// Recursively read a subtree rooted at `data` (a chunk payload, span already
/// stripped) that spans `subtrie_size` bytes, appending leaf bytes to `out`.
///
/// `parity` is the number of parity references *this* node carries (0 for a
/// data leaf or a non-redundant node). Boxed future because the recursion is
/// not tail-recursive and Rust `async fn` can't self-recur otherwise.
fn read_node<'a, G>(
    store: &'a G,
    data: &'a [u8],
    subtrie_size: u64,
    parity: usize,
    out: &'a mut Vec<u8>,
    progress: EcProgress<'a>,
) -> ReadNodeFut<'a>
where
    G: ChunkGet<DEFAULT_BODY_SIZE>,
{
    Box::pin(async move {
        // Leaf data chunk: its payload IS the file bytes for this range.
        if subtrie_size <= data.len() as u64 {
            out.extend_from_slice(data);
            progress(out.len());
            return Ok(());
        }

        // Intermediate node. Trim trailing zero-reference padding, then split
        // into data + parity references.
        let payload_size =
            chunk_payload_size(data).ok_or(ErasureError::Malformed("node has no children"))?;
        let (addrs, shard_cnt) = chunk_addresses(&data[..payload_size], parity);
        if shard_cnt == 0 || shard_cnt > addrs.len() {
            return Err(ErasureError::Malformed("bad shard count"));
        }

        // Fetch (and if necessary reconstruct) every DATA child of this node.
        let children = fetch_node_children(store, &addrs, shard_cnt).await?;

        // Recurse into each data child in order. Each child re-derives its own
        // subtree span, level and parity from its own span.
        //
        // Determine each child's subtree span. All but the last data child
        // cover an equal, maximal span; the last covers the remainder. We read
        // that directly off the child chunk's decoded span, which is
        // authoritative and matches bee (`chunkToSpan(ch.Data())`).
        for child in children.iter().take(shard_cnt) {
            let (child_level, child_span) = child.level_and_len();
            let child_parity = if child_level == Level::None {
                0
            } else {
                reference_count(child_span, child_level).1
            };
            read_node(store, &child.data, child_span, child_parity, out, progress).await?;
        }

        Ok(())
    })
}

/// Fetch the data children of an intermediate node, reconstructing any that
/// can't be retrieved from the node's parity siblings via Reed–Solomon.
///
/// `addrs` is the full ordered list of `shard_cnt` data refs followed by
/// `addrs.len() - shard_cnt` parity refs. Returns the `shard_cnt` decoded data
/// children (in order).
///
/// Strategy mirrors bee's getter (`pkg/file/redundancy/getter`):
/// - **Non-redundant node** (`parity_cnt == 0`): fetch all data children
///   concurrently; a single failure is fatal (same as the plain joiner).
/// - **Redundant node**: race *all* siblings — data **and** parity — at once
///   (bee's RACE strategy). As soon as any `shard_cnt` of the `shard_cnt +
///   parity_cnt` chunks land, cancel the rest and RS-reconstruct the missing
///   data shards. Racing everything (rather than data-first-then-parity
///   sequentially) is what makes recovery work on a flaky neighbourhood where
///   an arbitrary subset of a node's children are unretrievable — the exact
///   bee#5541 condition.
async fn fetch_node_children<G>(
    store: &G,
    addrs: &[[u8; 32]],
    shard_cnt: usize,
) -> Result<Vec<DecodedChunk>, ErasureError>
where
    G: ChunkGet<DEFAULT_BODY_SIZE>,
{
    let total = addrs.len();
    let parity_cnt = total - shard_cnt;

    // Fast path: no redundancy for this node. Fetch all data children
    // concurrently; a single failure is fatal.
    if parity_cnt == 0 {
        let mut futs = FuturesUnordered::new();
        for (i, a) in addrs.iter().take(shard_cnt).enumerate() {
            let addr = ChunkAddress::from(*a);
            futs.push(async move { (i, store.get(&addr).await) });
        }
        let mut slots: Vec<Option<DecodedChunk>> = (0..shard_cnt).map(|_| None).collect();
        while let Some((i, res)) = futs.next().await {
            let ch = res.map_err(|e| ErasureError::Fetch(e.to_string()))?;
            slots[i] = Some(DecodedChunk::from_any(&ch));
        }
        return Ok(slots.into_iter().map(|d| d.unwrap()).collect());
    }

    // Redundant node: RACE every sibling (data + parity) concurrently. Keep the
    // slot index so we can place each landed chunk in the RS buffer, and stop
    // once we hold `shard_cnt` chunks total.
    let shard_wire_size = SPAN_SIZE + CHUNK_SIZE; // 4104, bee ChunkWithSpanSize
    let mut rs_shards: Vec<Option<Vec<u8>>> = vec![None; total];
    // Keep decoded forms of the data shards we fetched directly, so we don't
    // re-parse them from the padded wire after reconstruction.
    let mut decoded_data: Vec<Option<DecodedChunk>> = (0..shard_cnt).map(|_| None).collect();
    let mut present = 0usize;
    let mut failed = 0usize;

    let mut futs = FuturesUnordered::new();
    for (i, a) in addrs.iter().enumerate() {
        let addr = ChunkAddress::from(*a);
        futs.push(async move { (i, store.get(&addr).await) });
    }

    while let Some((i, res)) = futs.next().await {
        match res {
            Ok(ch) => {
                let dc = DecodedChunk::from_any(&ch);
                rs_shards[i] = Some(to_wire_padded(&dc, shard_wire_size));
                if i < shard_cnt {
                    decoded_data[i] = Some(dc);
                }
                present += 1;
                // Enough to reconstruct: cancel the remaining in-flight fetches.
                if present >= shard_cnt {
                    break;
                }
            }
            Err(_) => {
                failed += 1;
                // If too many have failed to possibly reach shard_cnt, give up
                // early rather than waiting out every remaining timeout.
                if total - failed < shard_cnt {
                    break;
                }
            }
        }
    }
    drop(futs); // cancel any still-in-flight sibling fetches

    if present < shard_cnt {
        return Err(ErasureError::Unrecoverable {
            have: present,
            need: shard_cnt,
        });
    }

    // If every data shard arrived directly, no reconstruction is needed.
    let data_present = decoded_data.iter().filter(|d| d.is_some()).count();
    if data_present == shard_cnt {
        debug!(
            target: "hoverfly::erasure",
            "node: all {shard_cnt} data shards fetched directly (no reconstruction)"
        );
        return Ok(decoded_data.into_iter().map(|d| d.unwrap()).collect());
    }

    // Reconstruct the missing DATA shards from the (data + parity) shards we
    // gathered. RS needs any `shard_cnt` of the `total` shards present, which
    // the race guaranteed.
    let recovered = shard_cnt - data_present;
    let parity_used = present - data_present;
    info!(
        target: "hoverfly::erasure",
        "node: reconstructing {recovered} missing data shard(s) — \
         {data_present}/{shard_cnt} data fetched directly, {parity_used}/{parity_cnt} parity used, \
         {failed} sibling fetch failures"
    );
    let rs = ReedSolomon::new(shard_cnt, parity_cnt)?;
    rs.reconstruct_data(&mut rs_shards)?;

    // Materialise the data children in order: keep the directly-fetched decoded
    // forms; parse reconstructed shards from their padded wire.
    let mut out = Vec::with_capacity(shard_cnt);
    for (i, slot) in decoded_data.into_iter().enumerate() {
        match slot {
            Some(dc) => out.push(dc),
            None => {
                let wire = rs_shards[i]
                    .as_ref()
                    .ok_or(ErasureError::Malformed("reconstruction left a hole"))?;
                out.push(decode_wire(wire)?);
            }
        }
    }
    Ok(out)
}

/// Convert a decoded chunk back to its full RS wire form (`span[8] || payload`)
/// padded with zeros to `size` bytes (bee `ChunkWithSpanSize`).
fn to_wire_padded(dc: &DecodedChunk, size: usize) -> Vec<u8> {
    let mut wire = Vec::with_capacity(size);
    wire.extend_from_slice(&dc.raw_span.to_le_bytes());
    wire.extend_from_slice(&dc.data);
    if wire.len() < size {
        wire.resize(size, 0);
    } else {
        wire.truncate(size);
    }
    wire
}

/// Parse a reconstructed RS shard (full padded wire form) back into a
/// [`DecodedChunk`], trimming the payload to the length its span declares.
fn decode_wire(wire: &[u8]) -> Result<DecodedChunk, ErasureError> {
    if wire.len() < SPAN_SIZE {
        return Err(ErasureError::Malformed("reconstructed shard too short"));
    }
    let mut span_bytes = [0u8; SPAN_SIZE];
    span_bytes.copy_from_slice(&wire[..SPAN_SIZE]);
    let raw_span = u64::from_le_bytes(span_bytes);
    let (_, real_len) = decode_span(&span_bytes);

    // Payload length: for a leaf data chunk it's `real_len` (capped at
    // CHUNK_SIZE); for an intermediate node the span is a subtree size far
    // larger than the chunk, so the payload is the full chunk body. Cap at the
    // available payload bytes either way.
    let body = &wire[SPAN_SIZE..];
    let payload_len = if real_len as usize <= CHUNK_SIZE {
        (real_len as usize).min(body.len())
    } else {
        body.len()
    };
    Ok(DecodedChunk {
        raw_span,
        data: body[..payload_len].to_vec(),
    })
}

/// Whether a fetched root chunk's span carries an erasure-coding level, meaning
/// the object must be joined with [`fetch_erasure_bytes`] rather than the plain
/// nectar joiner. `root_span` is the raw 8-byte little-endian span.
pub fn root_is_erasure_coded(root_span: &[u8]) -> bool {
    is_level_encoded(root_span)
}

/// Convenience: given an already-fetched root chunk, report `(is_erasure,
/// level, span_len)`.
#[allow(dead_code)]
pub fn inspect_root(root: &AnyChunk<DEFAULT_BODY_SIZE>) -> (bool, Level, u64) {
    let raw = root.span().to_le_bytes();
    let (level, len) = decode_span(&raw);
    (is_level_encoded(&raw), level, len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::Chunk as _;
    use nectar_primitives::chunk::ContentChunk;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory store that can be told to "lose" specific chunk addresses to
    /// simulate unretrievable data shards on a fresh upload.
    struct MemStore {
        map: HashMap<[u8; 32], Vec<u8>>, // addr -> wire (span||payload)
        lost: Mutex<std::collections::HashSet<[u8; 32]>>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("not found")]
    struct NotFound;

    impl ChunkGet<DEFAULT_BODY_SIZE> for MemStore {
        type Error = NotFound;
        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
            let key: [u8; 32] = (*address).into();
            if self.lost.lock().unwrap().contains(&key) {
                return Err(NotFound);
            }
            let wire = self.map.get(&key).ok_or(NotFound)?;
            let cc = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(wire.as_slice())
                .map_err(|_| NotFound)?;
            Ok(AnyChunk::from(cc))
        }
    }

    /// Build a content chunk from raw (level-encoded) span + payload, returning
    /// (address, wire bytes).
    fn make_chunk(raw_span: u64, payload: &[u8]) -> ([u8; 32], Vec<u8>) {
        let mut wire = Vec::with_capacity(SPAN_SIZE + payload.len());
        wire.extend_from_slice(&raw_span.to_le_bytes());
        wire.extend_from_slice(payload);
        let cc = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(wire.as_slice()).unwrap();
        let addr: [u8; 32] = (*cc.address()).into();
        (addr, wire)
    }

    // Build a small two-level erasure-coded tree by hand:
    //   - N data leaves, each one full chunk (4096 bytes) except the last.
    //   - RS(N, P) parity leaves computed over the padded wire forms.
    //   - one root intermediate node holding N data refs + P parity refs,
    //     with a MEDIUM-encoded span = total file length.
    // Then drop some data leaves and confirm reconstruction yields the file.
    #[test]
    fn erasure_roundtrip_medium_with_losses() {
        futures::executor::block_on(async {
            let level = Level::Medium;
            let n_data = 4usize; // small file: 4 data chunks
            let file_len = 3 * CHUNK_SIZE + 1000; // last chunk partial
            let parity_cnt = level.parities(n_data);

            // Build data payloads.
            let mut file = vec![0u8; file_len];
            for (i, b) in file.iter_mut().enumerate() {
                *b = ((i * 13 + 7) & 0xff) as u8;
            }

            let mut map: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
            let mut data_addrs: Vec<[u8; 32]> = Vec::new();
            // RS shards over full padded wire forms.
            let shard_wire = SPAN_SIZE + CHUNK_SIZE;
            let mut rs_in: Vec<Vec<u8>> = Vec::new();

            for i in 0..n_data {
                let start = i * CHUNK_SIZE;
                let end = ((i + 1) * CHUNK_SIZE).min(file_len);
                let payload = &file[start..end];
                // leaf span = its real length (no level bit for leaves in this
                // hand-built tree; bee sets level bits on intermediate spans).
                let raw_span = payload.len() as u64;
                let (addr, wire) = make_chunk(raw_span, payload);
                map.insert(addr, wire.clone());
                data_addrs.push(addr);
                // padded wire form for RS
                let mut w = wire.clone();
                w.resize(shard_wire, 0);
                rs_in.push(w);
            }

            // Compute parity shards via our own RS (byte-exact to bee).
            let rs = ReedSolomon::new(n_data, parity_cnt).unwrap();
            let mut shards: Vec<Vec<u8>> = rs_in.clone();
            for _ in 0..parity_cnt {
                shards.push(vec![0u8; shard_wire]);
            }
            rs.encode(&mut shards).unwrap();

            let mut parity_addrs: Vec<[u8; 32]> = Vec::new();
            for p in 0..parity_cnt {
                let wire = &shards[n_data + p];
                // parity chunk is stored as a content chunk of the full padded
                // wire (span is whatever the encode produced in the first 8
                // bytes; store as-is).
                let cc = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(wire.as_slice()).unwrap();
                let addr: [u8; 32] = (*cc.address()).into();
                map.insert(addr, wire.clone());
                parity_addrs.push(addr);
            }

            // Root intermediate node: data refs then parity refs.
            let mut root_payload = Vec::new();
            for a in &data_addrs {
                root_payload.extend_from_slice(a);
            }
            for a in &parity_addrs {
                root_payload.extend_from_slice(a);
            }
            // Root span = file length with MEDIUM level bit set.
            let mut root_span_bytes = (file_len as u64).to_le_bytes();
            root_span_bytes[SPAN_SIZE - 1] = (Level::Medium as u8) | 0x80;
            let root_raw_span = u64::from_le_bytes(root_span_bytes);
            let (root_addr, root_wire) = make_chunk(root_raw_span, &root_payload);
            map.insert(root_addr, root_wire);

            let store = MemStore {
                map,
                lost: Mutex::new(std::collections::HashSet::new()),
            };

            // Sanity: with everything present, we get the file back.
            let got = fetch_erasure_bytes(&store, ChunkAddress::from(root_addr))
                .await
                .unwrap();
            assert_eq!(got, file);

            // Now "lose" as many data shards as we have parity for and confirm
            // reconstruction still yields the exact file.
            {
                let mut lost = store.lost.lock().unwrap();
                for a in data_addrs.iter().take(parity_cnt) {
                    lost.insert(*a);
                }
            }
            let recovered = fetch_erasure_bytes(&store, ChunkAddress::from(root_addr))
                .await
                .unwrap();
            assert_eq!(recovered, file, "reconstructed file must be byte-exact");
        });
    }

    #[test]
    fn erasure_unrecoverable_when_too_many_lost() {
        futures::executor::block_on(async {
            let level = Level::Medium;
            let n_data = 4usize;
            let file_len = 4 * CHUNK_SIZE;
            let parity_cnt = level.parities(n_data);

            let mut file = vec![0u8; file_len];
            for (i, b) in file.iter_mut().enumerate() {
                *b = ((i * 5 + 1) & 0xff) as u8;
            }

            let mut map: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
            let mut data_addrs: Vec<[u8; 32]> = Vec::new();
            let shard_wire = SPAN_SIZE + CHUNK_SIZE;
            let mut rs_in: Vec<Vec<u8>> = Vec::new();
            for i in 0..n_data {
                let start = i * CHUNK_SIZE;
                let end = ((i + 1) * CHUNK_SIZE).min(file_len);
                let payload = &file[start..end];
                let raw_span = payload.len() as u64;
                let (addr, wire) = make_chunk(raw_span, payload);
                map.insert(addr, wire.clone());
                data_addrs.push(addr);
                let mut w = wire.clone();
                w.resize(shard_wire, 0);
                rs_in.push(w);
            }
            let rs = ReedSolomon::new(n_data, parity_cnt).unwrap();
            let mut shards: Vec<Vec<u8>> = rs_in.clone();
            for _ in 0..parity_cnt {
                shards.push(vec![0u8; shard_wire]);
            }
            rs.encode(&mut shards).unwrap();
            let mut parity_addrs: Vec<[u8; 32]> = Vec::new();
            for p in 0..parity_cnt {
                let wire = &shards[n_data + p];
                let cc = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(wire.as_slice()).unwrap();
                let addr: [u8; 32] = (*cc.address()).into();
                map.insert(addr, wire.clone());
                parity_addrs.push(addr);
            }
            let mut root_payload = Vec::new();
            for a in &data_addrs {
                root_payload.extend_from_slice(a);
            }
            for a in &parity_addrs {
                root_payload.extend_from_slice(a);
            }
            let mut root_span_bytes = (file_len as u64).to_le_bytes();
            root_span_bytes[SPAN_SIZE - 1] = (Level::Medium as u8) | 0x80;
            let (root_addr, root_wire) =
                make_chunk(u64::from_le_bytes(root_span_bytes), &root_payload);
            map.insert(root_addr, root_wire);

            let store = MemStore {
                map,
                lost: Mutex::new(std::collections::HashSet::new()),
            };
            // Lose parity_cnt + 1 data shards → unrecoverable.
            {
                let mut lost = store.lost.lock().unwrap();
                for a in data_addrs.iter().take(parity_cnt + 1) {
                    lost.insert(*a);
                }
            }
            let res = fetch_erasure_bytes(&store, ChunkAddress::from(root_addr)).await;
            assert!(matches!(res, Err(ErasureError::Unrecoverable { .. })));
        });
    }
}
