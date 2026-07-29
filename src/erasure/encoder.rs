//! Erasure-coding-aware file splitter (the **upload** side).
//!
//! This is the write-side mirror of [`super::joiner`]: given a byte slice and a
//! redundancy [`Level`], it produces the exact chunk set bee's upload pipeline
//! produces — data chunks, Reed–Solomon parity chunks, and intermediate
//! ("hashtrie") nodes whose spans carry the level byte and whose payloads lay
//! out `shard_cnt` data references followed by `parity_cnt` parity references.
//!
//! ## Why not reuse nectar's splitter
//!
//! `nectar_primitives::file::split` builds a plain BMT tree: every reference in
//! an intermediate node is a data reference and the branching factor is a
//! constant 128. Erasure coding changes both — the effective branching is
//! `Level::max_shards()` (119 at MEDIUM) and each node carries a run of parity
//! references after its data references. So we port bee's writer
//! (`pkg/file/pipeline/hashtrie` + `pkg/file/redundancy`) instead.
//!
//! ## The algorithm (bee `hashTrieWriter`)
//!
//! Data is fed in 4096-byte chunks. Each chunk is:
//! 1. emitted as a content chunk (`span_LE_8 || payload`), and its reference
//!    appended to **trie level 1**;
//! 2. cached, zero-padded to `SPAN_SIZE + CHUNK_SIZE` (4104, bee's
//!    `ChunkWithSpanSize`), in **redundancy buffer 0**.
//!
//! When a redundancy buffer reaches `max_shards` entries it is RS-encoded; the
//! resulting parity shards are emitted as content chunks too, and their
//! references appended to the trie level above the buffer (`buffer i` feeds
//! `trie level i + 1`). A trie level fills at `max_shards + parities(max_shards)`
//! = 128 references, at which point it is *wrapped*: the node's span is the sum
//! of its **data** children's spans (parity spans are gibberish and are not
//! summed), level-encoded via [`super::Level`] when the node has any parity
//! children, and the node itself becomes a chunk whose reference goes one level
//! up — and which is *itself* cached for RS encoding at that level.
//!
//! `finish` flushes: for each level bottom-up, a level holding a single
//! reference is *carried over* to the level above without wrapping (bee's
//! dangling-chunk optimisation, and the reason a one-chunk file has the same
//! address at every redundancy level), and a level holding several references is
//! RS-encoded and wrapped.
//!
//! ## Consequences worth knowing
//!
//! - The root reference **changes** with the level: a level-encoded span is part
//!   of the BMT preimage. This is exactly how bee behaves — `POST /bytes` with
//!   `Swarm-Redundancy-Level: 0` and `: 1` give different references for the
//!   same bytes.
//! - A file that fits in one chunk gets no parity at all (there is nothing to
//!   wrap), so its reference is identical at every level.
//! - Small multi-chunk files pay a large *relative* overhead (2 data chunks at
//!   MEDIUM get 3 parity chunks) because the erasure table's low rows are tuned
//!   for a fixed error probability, not a fixed rate. Large files converge on
//!   the table's top row: 9 parities per 119 data chunks ≈ +7.6%.

use nectar_primitives::bmt::{HASH_SIZE, SPAN_SIZE};
use nectar_primitives::chunk::ChunkAddress;
use tracing::debug;

use super::reedsolomon::{ReedSolomon, RsError};
use super::{CHUNK_SIZE, Level, wire_address};

/// bee `swarm.ChunkWithSpanSize` — the RS shard size (span + full payload).
const CHUNK_WITH_SPAN: usize = SPAN_SIZE + CHUNK_SIZE;

/// bee `hashtrie.maxLevel` — the trie tops out at level 8, i.e. 128^7 * 4096
/// bytes of addressable content.
const MAX_LEVEL: usize = 8;

/// Errors from the erasure encoder.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("erasure encode: reed-solomon: {0}")]
    Rs(#[from] RsError),
    #[error("erasure encode: chunk body exceeds {CHUNK_SIZE} bytes ({0})")]
    OversizedChunk(usize),
    #[error("erasure encode: trie full (content exceeds the addressable 8-level tree)")]
    TrieFull,
    #[error("erasure encode: inconsistent references at the root level")]
    InconsistentRefs,
    #[error("erasure encode: dispersed replica: {0}")]
    Replica(String),
}

/// One reference held by a trie level: the child's raw span (level byte intact,
/// gibberish for a parity child) and its 32-byte address.
#[derive(Clone, Copy)]
struct Entry {
    span: u64,
    reference: [u8; HASH_SIZE],
}

/// A trie level's pending children. Data references always precede parity
/// references: parities are only appended when a codeword closes, which is also
/// when the level fills and is wrapped, so a level never holds parity
/// references across a wrap.
#[derive(Default)]
struct LevelBuf {
    entries: Vec<Entry>,
    /// How many of `entries` are *data* references (bee's
    /// `effectiveChunkCounters`). The rest are parity.
    effective: usize,
}

/// The chunk set produced by a split, deduplicated by address.
///
/// Deduplication matters: stamping the same address twice burns two indices in
/// the same postage bucket and bee rejects the overflow (`invalid stamp: invalid
/// index`). nectar's splitter dedupes implicitly by returning a `MemoryStore`
/// keyed on address; we do it explicitly so repeated content (all-zero regions,
/// and the all-zero parity shards they produce) behaves the same.
pub struct SplitOutput {
    /// Root reference of the object.
    pub root: ChunkAddress,
    /// Every unique chunk in wire form: `(address, span_LE_8 || payload)`.
    pub chunks: Vec<(ChunkAddress, Vec<u8>)>,
    /// Number of parity chunks among `chunks` (diagnostics only).
    pub parity_chunks: usize,
    /// Number of dispersed root replicas among `chunks` (diagnostics only).
    pub replica_chunks: usize,
}

/// Split `data` into an erasure-coded chunk tree at redundancy `level`.
///
/// Byte-exact with bee's upload pipeline: the returned root is the reference a
/// bee gateway would return for `POST /bytes` with the matching
/// `Swarm-Redundancy-Level` header, and the chunk set is the same set bee would
/// store. `Level::None` is accepted and produces a plain tree (identical to
/// nectar's splitter), so callers can funnel every upload through one function.
pub fn split_with_redundancy(data: &[u8], level: Level) -> Result<SplitOutput, EncodeError> {
    let mut enc = Encoder::new(level);
    for chunk in data.chunks(CHUNK_SIZE) {
        enc.write(chunk)?;
    }
    if data.is_empty() {
        // bee's feeder emits a single zero-span chunk for empty content so the
        // reference is the well-known empty-file hash rather than nothing.
        enc.write(&[])?;
    }
    enc.finish()
}

/// bee `hashTrieWriter` + `redundancy.Params`, fused into one struct because
/// they are mutually recursive (a wrap feeds the redundancy buffer, and a
/// redundancy encode feeds the level above).
struct Encoder {
    level: Level,
    /// Effective data references per full node (bee `Level.GetMaxShards`).
    max_shards: usize,
    /// References per full node, data + parity (bee `maxChildrenChunks`). 128
    /// for any level; `max_shards` when there is no redundancy.
    max_children: usize,
    /// Trie levels 1..=8 (index 0 unused, mirroring bee's 1-based levels).
    levels: Vec<LevelBuf>,
    /// Redundancy buffers: `rs_buf[i]` caches the padded chunk bodies whose
    /// references land on trie level `i + 1`.
    rs_buf: Vec<Vec<Vec<u8>>>,
    full: bool,
    /// Emitted chunks, deduplicated by address.
    out: Vec<(ChunkAddress, Vec<u8>)>,
    seen: std::collections::HashSet<[u8; HASH_SIZE]>,
    parity_chunks: usize,
}

impl Encoder {
    fn new(level: Level) -> Self {
        // `Level::None` has an empty erasure table, so `max_shards` is the full
        // 128 branches and `max_children` collapses to it — the plain tree.
        let max_shards = level.max_shards();
        let max_children = max_shards + level.parities(max_shards);
        Encoder {
            level,
            max_shards,
            max_children,
            levels: (0..=MAX_LEVEL + 1).map(|_| LevelBuf::default()).collect(),
            // bee allocates 8 redundancy levels; buffer `i` is consumed by trie
            // level `i + 1`, and the carrier-chunk elevation can reach index 7.
            rs_buf: (0..MAX_LEVEL).map(|_| Vec::new()).collect(),
            full: false,
            out: Vec::new(),
            seen: std::collections::HashSet::new(),
            parity_chunks: 0,
        }
    }

    /// Feed one data chunk (≤ [`CHUNK_SIZE`] bytes). bee's `chunkFeeder` +
    /// `writeToDataLevel`.
    fn write(&mut self, payload: &[u8]) -> Result<(), EncodeError> {
        if self.full {
            return Err(EncodeError::TrieFull);
        }
        let span = payload.len() as u64;
        let mut wire = Vec::with_capacity(SPAN_SIZE + payload.len());
        wire.extend_from_slice(&span.to_le_bytes());
        wire.extend_from_slice(payload);
        let reference = self.emit(&wire, false)?;

        self.write_to_level(1, false, span, reference)?;
        // Data chunks are the shards of redundancy buffer 0.
        self.chunk_write(0, &wire)
    }

    /// Emit a chunk (dedup by address) and return its reference.
    fn emit(&mut self, wire: &[u8], parity: bool) -> Result<[u8; HASH_SIZE], EncodeError> {
        let address =
            wire_address(wire).ok_or(EncodeError::OversizedChunk(wire.len() - SPAN_SIZE))?;
        let reference: [u8; HASH_SIZE] = address.into();
        if self.seen.insert(reference) {
            self.out.push((address, wire.to_vec()));
            if parity {
                self.parity_chunks += 1;
            }
        }
        Ok(reference)
    }

    /// bee `writeToIntermediateLevel`: append a child reference to `level`,
    /// wrapping the level when it fills.
    fn write_to_level(
        &mut self,
        level: usize,
        parity: bool,
        span: u64,
        reference: [u8; HASH_SIZE],
    ) -> Result<(), EncodeError> {
        let buf = &mut self.levels[level];
        buf.entries.push(Entry { span, reference });
        if !parity {
            buf.effective += 1;
        }
        let filled = buf.entries.len() == self.max_children;
        if filled {
            self.wrap_level(level)?;
        }
        Ok(())
    }

    /// bee `wrapFullLevel`: turn a level's pending references into one
    /// intermediate chunk, push that chunk's reference one level up, and cache
    /// it for RS encoding at this level.
    fn wrap_level(&mut self, level: usize) -> Result<(), EncodeError> {
        let entries = std::mem::take(&mut self.levels[level].entries);
        let effective = std::mem::take(&mut self.levels[level].effective);
        debug_assert!(effective <= entries.len());

        // Only data children contribute to the node's span; a parity chunk's
        // first 8 bytes are RS output, not a length.
        //
        // The sum **wraps** (bee sums the raw little-endian spans in Go, where
        // `+` on uint64 wraps). It has to: a data child that is itself a
        // level-encoded intermediate node carries `(level | 0x80) << 56` in its
        // span, so summing several of them overflows byte 7 — harmlessly,
        // because `EncodeLevel` below overwrites that byte, and a node with
        // level-encoded children always has parity children too (every wrap at
        // a non-`None` level closes a codeword). The low 56 bits, which hold
        // the real lengths, add up correctly either way.
        let span = entries[..effective]
            .iter()
            .fold(0u64, |acc, e| acc.wrapping_add(e.span));
        let parities = entries.len() - effective;

        let mut span_bytes = span.to_le_bytes();
        if parities > 0 {
            // bee `redundancy.EncodeLevel`: the level rides in the span's top
            // byte with bit 7 set. The BMT address covers this byte, so it must
            // be set before hashing.
            span_bytes[SPAN_SIZE - 1] = (self.level as u8) | 0x80;
        }

        let mut wire = Vec::with_capacity(SPAN_SIZE + entries.len() * HASH_SIZE);
        wire.extend_from_slice(&span_bytes);
        for e in &entries {
            wire.extend_from_slice(&e.reference);
        }
        let reference = self.emit(&wire, false)?;
        let raw_span = u64::from_le_bytes(span_bytes);

        self.write_to_level(level + 1, false, raw_span, reference)?;
        // The wrapped node is itself a shard of the codeword one level up.
        self.chunk_write(level, &wire)?;

        if level + 1 == MAX_LEVEL {
            self.full = true;
        }
        Ok(())
    }

    /// bee `redundancy.Params.ChunkWrite`: cache a chunk body (zero-padded to
    /// `ChunkWithSpanSize`) as a shard, RS-encoding the buffer when it fills.
    fn chunk_write(&mut self, rs_level: usize, wire: &[u8]) -> Result<(), EncodeError> {
        if self.level == Level::None || rs_level >= self.rs_buf.len() {
            return Ok(());
        }
        let mut shard = wire.to_vec();
        shard.resize(CHUNK_WITH_SPAN, 0);
        self.rs_buf[rs_level].push(shard);
        if self.rs_buf[rs_level].len() == self.max_shards {
            self.rs_encode(rs_level)?;
        }
        Ok(())
    }

    /// bee `redundancy.Params.encode`: RS-encode the buffered shards, emit each
    /// parity shard as a content chunk, and append its reference to the trie
    /// level above the buffer.
    fn rs_encode(&mut self, rs_level: usize) -> Result<(), EncodeError> {
        if self.level == Level::None || self.rs_buf[rs_level].is_empty() {
            return Ok(());
        }
        let shards = self.rs_buf[rs_level].len();
        let parities = self.level.parities(shards);
        if parities == 0 {
            self.rs_buf[rs_level].clear();
            return Ok(());
        }

        let mut buf = std::mem::take(&mut self.rs_buf[rs_level]);
        buf.resize(shards + parities, vec![0u8; CHUNK_WITH_SPAN]);
        ReedSolomon::new(shards, parities)?.encode(&mut buf)?;

        for parity in buf.into_iter().skip(shards) {
            // A parity chunk is a plain content chunk over the full padded
            // shard; its "span" is whatever RS produced in the first 8 bytes.
            let mut span_bytes = [0u8; SPAN_SIZE];
            span_bytes.copy_from_slice(&parity[..SPAN_SIZE]);
            let reference = self.emit(&parity, true)?;
            self.write_to_level(
                rs_level + 1,
                true,
                u64::from_le_bytes(span_bytes),
                reference,
            )?;
        }
        Ok(())
    }

    /// bee `redundancy.Params.ElevateCarrierChunk`: when a level ends up with a
    /// single reference it is carried over unwrapped, so the chunk it points at
    /// must join the codeword one level up instead.
    fn elevate_carrier_chunk(&mut self, rs_level: usize) -> Result<(), EncodeError> {
        if self.level == Level::None || rs_level >= self.rs_buf.len() {
            return Ok(());
        }
        // bee errors out if the level holds anything but the lone carrier; the
        // trie construction guarantees it, so treat a mismatch as a no-op
        // rather than failing an upload over an unreachable state.
        if self.rs_buf[rs_level].len() != 1 {
            debug!(
                target: "hoverfly::erasure",
                "elevate_carrier_chunk: rs level {rs_level} holds {} shards, expected 1",
                self.rs_buf[rs_level].len()
            );
            return Ok(());
        }
        let carrier = self.rs_buf[rs_level][0].clone();
        // bee leaves the source cursor alone — the level is never revisited.
        if rs_level + 1 < self.rs_buf.len() {
            self.rs_buf[rs_level + 1].push(carrier);
            if self.rs_buf[rs_level + 1].len() == self.max_shards {
                self.rs_encode(rs_level + 1)?;
            }
        }
        Ok(())
    }

    /// bee `hashTrieWriter.Sum`: flush every level bottom-up and return the
    /// root reference plus the emitted chunk set.
    fn finish(mut self) -> Result<SplitOutput, EncodeError> {
        for level in 1..MAX_LEVEL {
            match self.levels[level].entries.len() {
                0 => continue,
                // Reachable only via a carry-over that happened to fill the
                // level above; normal fills wrap inside `write_to_level`.
                n if n == self.max_children => self.wrap_level(level)?,
                1 => {
                    // Carry over: move the lone reference up without wrapping,
                    // and move its cached body into the codeword above.
                    let entry = self.levels[level].entries.remove(0);
                    self.levels[level].effective = 0;
                    self.levels[level + 1].entries.push(entry);
                    self.levels[level + 1].effective += 1;
                    self.elevate_carrier_chunk(level - 1)?;
                }
                _ => {
                    // Close the level's partial codeword, then wrap it.
                    self.rs_encode(level - 1)?;
                    self.wrap_level(level)?;
                }
            }
        }

        let top = &self.levels[MAX_LEVEL];
        if top.entries.len() != 1 {
            return Err(EncodeError::InconsistentRefs);
        }
        let root = ChunkAddress::from(top.entries[0].reference);

        // Dispersed replicas of the root (bee `hashtrie.Sum` -> `replicas`).
        // The root is the one chunk no parity covers, so it gets its own
        // redundancy: copies at deliberately scattered addresses, derivable
        // from the root reference alone. Emitted for every non-NONE level,
        // including objects too small to carry any parity.
        let root_wire = self
            .out
            .iter()
            .find(|(a, _)| *a == root)
            .map(|(_, w)| w.clone())
            .ok_or(EncodeError::InconsistentRefs)?;
        let mut replica_chunks = 0usize;
        for (addr, wire) in super::replicas::dispersed_replicas(&root_wire, self.level)? {
            let key: [u8; HASH_SIZE] = addr.into();
            if self.seen.insert(key) {
                self.out.push((addr, wire));
                replica_chunks += 1;
            }
        }

        Ok(SplitOutput {
            root,
            chunks: self.out,
            parity_chunks: self.parity_chunks,
            replica_chunks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure::joiner::fetch_erasure_bytes;
    use crate::erasure::{WireGet, decode_span, is_level_encoded};
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    #[derive(Debug, thiserror::Error)]
    #[error("not found")]
    struct NotFound;

    /// In-memory store over a split's output that can be told to "lose" chunks,
    /// simulating the sparse neighbourhood a fresh upload lands in.
    struct MemStore {
        map: HashMap<[u8; 32], Vec<u8>>,
        lost: Mutex<HashSet<[u8; 32]>>,
    }

    impl MemStore {
        fn new(out: &SplitOutput) -> Self {
            let mut map = HashMap::new();
            for (addr, wire) in &out.chunks {
                map.insert(<[u8; 32]>::from(*addr), wire.clone());
            }
            MemStore {
                map,
                lost: Mutex::new(HashSet::new()),
            }
        }
        fn lose(&self, addrs: &[[u8; 32]]) {
            let mut l = self.lost.lock().unwrap();
            for a in addrs {
                l.insert(*a);
            }
        }
    }

    impl WireGet for MemStore {
        type Error = NotFound;
        async fn get_wire(&self, address: &ChunkAddress) -> Result<Vec<u8>, Self::Error> {
            let key: [u8; 32] = (*address).into();
            if self.lost.lock().unwrap().contains(&key) {
                return Err(NotFound);
            }
            self.map.get(&key).cloned().ok_or(NotFound)
        }
    }

    /// Pseudo-random bytes (xorshift64*), so no two 4096-byte chunks are
    /// equal — a periodic generator would collapse under the address dedup and
    /// silently stop exercising the RS path.
    fn data_of(len: usize) -> Vec<u8> {
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    /// Every level-NONE split must be byte-identical to nectar's plain
    /// splitter — same root, same chunk set. This is the guard that the ported
    /// hashtrie is faithful before any redundancy is layered on.
    #[test]
    fn plain_matches_nectar_split() {
        use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
        use nectar_primitives::file::split;
        for len in [
            0usize,
            1,
            4095,
            4096,
            4097,
            CHUNK_SIZE * 3 + 17,
            CHUNK_SIZE * 128,
            CHUNK_SIZE * 129,
            CHUNK_SIZE * 500 + 3,
        ] {
            let data = data_of(len);
            let out = split_with_redundancy(&data, Level::None).unwrap();
            let (root, store) = split::<DEFAULT_BODY_SIZE>(&data).unwrap();
            assert_eq!(out.root, root, "root mismatch at len {len}");
            assert_eq!(out.chunks.len(), store.len(), "chunk count at len {len}");
            assert_eq!(out.parity_chunks, 0);
            let ours: HashSet<[u8; 32]> = out
                .chunks
                .iter()
                .map(|(a, _)| <[u8; 32]>::from(*a))
                .collect();
            for (addr, _) in store.into_chunks() {
                assert!(
                    ours.contains(&<[u8; 32]>::from(addr)),
                    "missing chunk at len {len}"
                );
            }
        }
    }

    /// A file that fits in one chunk has nothing to wrap, so redundancy is a
    /// no-op and the reference is level-independent (matches bee).
    #[test]
    fn single_chunk_file_is_level_independent() {
        let data = data_of(1000);
        let plain = split_with_redundancy(&data, Level::None).unwrap();
        assert_eq!(plain.chunks.len(), 1);
        for (level, replicas) in [
            (Level::Medium, 2),
            (Level::Strong, 4),
            (Level::Paranoid, 16),
        ] {
            let out = split_with_redundancy(&data, level).unwrap();
            assert_eq!(out.root, plain.root, "{level}");
            // No tree to hang parity off — but the root still gets replicas,
            // which is bee's behaviour too.
            assert_eq!(out.parity_chunks, 0, "{level}");
            assert_eq!(out.replica_chunks, replicas, "{level}");
            assert_eq!(out.chunks.len(), 1 + replicas, "{level}");
        }
    }

    /// The root of an erasure-coded object carries the level in its span, which
    /// is what routes the download path to the RS joiner.
    #[test]
    fn root_span_is_level_encoded() {
        let data = data_of(CHUNK_SIZE * 4);
        let out = split_with_redundancy(&data, Level::Medium).unwrap();
        let root_wire = out
            .chunks
            .iter()
            .find(|(a, _)| *a == out.root)
            .map(|(_, w)| w.clone())
            .unwrap();
        assert!(is_level_encoded(&root_wire[..SPAN_SIZE]));
        let (level, len) = decode_span(&root_wire[..SPAN_SIZE]);
        assert_eq!(level, Level::Medium);
        assert_eq!(len, data.len() as u64);
    }

    /// Parity counts follow bee's erasure table per codeword, and the tree
    /// round-trips through the erasure joiner byte-exactly.
    #[test]
    fn medium_roundtrip_through_joiner() {
        for len in [
            CHUNK_SIZE * 2,
            CHUNK_SIZE * 4 + 100,
            CHUNK_SIZE * 30,
            CHUNK_SIZE * 119,
            CHUNK_SIZE * 120 + 7,
            CHUNK_SIZE * 260,
        ] {
            let data = data_of(len);
            let out = split_with_redundancy(&data, Level::Medium).unwrap();
            assert!(out.parity_chunks > 0, "no parity produced at len {len}");
            let store = MemStore::new(&out);
            let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
            assert_eq!(got, data, "roundtrip mismatch at len {len}");
        }
    }

    /// The point of the exercise: an object whose data chunks are partly
    /// unretrievable still reads back byte-exactly from the parity siblings.
    #[test]
    fn reconstructs_after_losing_data_chunks() {
        let data = data_of(CHUNK_SIZE * 20);
        let out = split_with_redundancy(&data, Level::Medium).unwrap();
        // 20 data shards at MEDIUM → 5 parities, so up to 5 losses survive.
        let parities = Level::Medium.parities(20);
        assert_eq!(parities, 5);
        assert_eq!(out.parity_chunks, parities);

        // Lose `parities` leaf chunks (a leaf's raw span is exactly its own
        // length, and it is not the root).
        let leaves: Vec<[u8; 32]> = out
            .chunks
            .iter()
            .filter(|(a, w)| {
                *a != out.root
                    && u64::from_le_bytes(w[..SPAN_SIZE].try_into().unwrap()) == CHUNK_SIZE as u64
            })
            .map(|(a, _)| <[u8; 32]>::from(*a))
            .take(parities)
            .collect();
        assert_eq!(leaves.len(), parities);

        let store = MemStore::new(&out);
        store.lose(&leaves);
        let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
        assert_eq!(got, data);
    }

    /// Multi-level trees: 260 chunks span two levels of intermediate nodes, so
    /// level 2 gets its own codeword over the wrapped level-1 nodes. Pins the
    /// exact tree shape, since a mis-sized codeword still round-trips.
    #[test]
    fn multi_level_tree_codewords() {
        let data = data_of(CHUNK_SIZE * 260);
        let out = split_with_redundancy(&data, Level::Medium).unwrap();

        // Level 1: two full codewords (119 data + 9 parity) and a partial one
        // (22 data + 5 parity) → 3 intermediate nodes, 23 parity chunks.
        // Level 2: one codeword over those 3 nodes → 3 parity chunks + root.
        let level1_parity = 2 * Level::Medium.parities(119) + Level::Medium.parities(22);
        let level2_parity = Level::Medium.parities(3);
        assert_eq!((level1_parity, level2_parity), (23, 3));
        assert_eq!(out.parity_chunks, level1_parity + level2_parity);
        assert_eq!(out.replica_chunks, 2);
        assert_eq!(
            out.chunks.len(),
            260 + level1_parity + 3 + level2_parity + 1 + out.replica_chunks
        );

        let store = MemStore::new(&out);
        let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
        assert_eq!(got, data);
    }

    /// Bee's own `hashtrie.TestRedundancy` sample, reproduced: 98 full chunks
    /// at INSANE is "97 chunk references fit into one chunk + 1 carrier", for
    /// which bee asserts 37 parity chunks (31 for the full codeword + 6 for the
    /// 2-reference one) and exactly 2 intermediate chunks (the wrapped level-1
    /// node and the root). It exercises the carry-over / `ElevateCarrierChunk`
    /// path, which nothing else here reaches with a non-trivial codeword.
    #[test]
    fn matches_bee_hashtrie_redundancy_sample() {
        let level = Level::Insane;
        assert_eq!(level.max_shards(), 97);
        let data = data_of(CHUNK_SIZE * 98);
        let out = split_with_redundancy(&data, level).unwrap();

        // 31 parities for the full 97-shard codeword, 6 for the 2-shard one
        // (the wrapped node + the elevated carrier chunk).
        assert_eq!(level.parities(97), 31);
        assert_eq!(level.parities(2), 6);
        assert_eq!(out.parity_chunks, 37);
        // 98 data + 37 parity + 2 intermediate (level-1 node + root) + the 8
        // dispersed replicas INSANE stores of the root.
        assert_eq!(out.replica_chunks, 8);
        assert_eq!(out.chunks.len(), 98 + 37 + 2 + 8);

        // Root span: level INSANE, length 98 * 4096 — bee's `span check`.
        let root_wire = out
            .chunks
            .iter()
            .find(|(a, _)| *a == out.root)
            .map(|(_, w)| w.clone())
            .unwrap();
        let (root_level, root_len) = decode_span(&root_wire[..SPAN_SIZE]);
        assert_eq!(root_level, level);
        assert_eq!(root_len, (98 * CHUNK_SIZE) as u64);

        // And bee's `ReferenceCount` on that span must recover the root's own
        // parity count (37 total minus the 31 of the full codeword) — the
        // invariant the download path relies on to split refs from parities.
        let (_, root_parity) = crate::erasure::reference_count(root_len, root_level);
        assert_eq!(root_parity, 37 - level.parities(level.max_shards()));

        let store = MemStore::new(&out);
        let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
        assert_eq!(got, data);
    }

    /// Stronger levels produce strictly more parity for the same content.
    #[test]
    fn stronger_levels_add_more_parity() {
        let data = data_of(CHUNK_SIZE * 40);
        let medium = split_with_redundancy(&data, Level::Medium).unwrap();
        let strong = split_with_redundancy(&data, Level::Strong).unwrap();
        let insane = split_with_redundancy(&data, Level::Insane).unwrap();
        assert!(medium.parity_chunks < strong.parity_chunks);
        assert!(strong.parity_chunks < insane.parity_chunks);
        for out in [&medium, &strong, &insane] {
            let store = MemStore::new(out);
            let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
            assert_eq!(got, data);
        }
    }

    /// Parity chunks routinely have a "span" that nectar's `ContentChunk`
    /// refuses to represent, which is why both erasure paths hash and carry
    /// raw wire bytes ([`super::wire_address`], [`super::WireGet`]).
    ///
    /// Concretely: a parity shard's first eight bytes are the RS combination of
    /// the data shards' spans. With a partial final data chunk those bytes form
    /// an essentially arbitrary 16-bit number, which lands at or below the
    /// 4096-byte body size — and thus fails nectar's `span == data.len()` rule
    /// — for about 5% of parity chunks across file sizes. Here all four do.
    /// Routing them through nectar would have dropped them as "address
    /// mismatch" on retrieval, losing exactly the parity the joiner needs.
    #[test]
    fn parity_spans_that_nectar_rejects_still_round_trip() {
        use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
        use nectar_primitives::chunk::ContentChunk;

        // 8 full chunks + a 1234-byte remainder → 9 data shards, 4 parities.
        let data = data_of(CHUNK_SIZE * 8 + 1234);
        let out = split_with_redundancy(&data, Level::Medium).unwrap();
        assert_eq!(out.parity_chunks, Level::Medium.parities(9));

        let unrepresentable = out
            .chunks
            .iter()
            .filter(|(_, w)| ContentChunk::<DEFAULT_BODY_SIZE>::try_from(w.as_slice()).is_err())
            .count();
        assert_eq!(
            unrepresentable, out.parity_chunks,
            "expected every parity chunk here to be unrepresentable by nectar"
        );

        // Addresses are still the plain BMT hash of the wire form, so bee
        // accepts them (`cac.Valid` only re-hashes) — and so do we. Replicas
        // are appended last and are single-owner chunks, whose address is
        // `keccak256(id || owner)` rather than a BMT root, so they are excluded.
        let tree = &out.chunks[..out.chunks.len() - out.replica_chunks];
        for (addr, wire) in tree {
            assert_eq!(wire_address(wire).as_ref(), Some(addr));
        }

        let store = MemStore::new(&out);
        let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
        assert_eq!(got, data);
    }

    /// A node's siblings are fetched through a bounded in-flight window that
    /// refills as fetches land. The window must not change what the joiner
    /// *gets*: it still has to reach `shard_cnt` and reconstruct, even when the
    /// window is far narrower than a codeword and some data chunks are missing
    /// (so the refill has to walk into the parity tail to finish).
    #[test]
    fn narrow_in_flight_window_still_reconstructs() {
        use crate::erasure::joiner::fetch_erasure_bytes_progress;

        let data = data_of(CHUNK_SIZE * 20);
        let out = split_with_redundancy(&data, Level::Medium).unwrap();
        let parities = Level::Medium.parities(20);

        // Drop every parity's worth of data leaves, forcing the window to reach
        // past the data references into the parity tail.
        let leaves: Vec<[u8; 32]> = out
            .chunks
            .iter()
            .filter(|(a, w)| {
                *a != out.root
                    && u64::from_le_bytes(w[..SPAN_SIZE].try_into().unwrap()) == CHUNK_SIZE as u64
            })
            .map(|(a, _)| <[u8; 32]>::from(*a))
            .take(parities)
            .collect();

        for window in [1usize, 2, 7, 1000] {
            let store = MemStore::new(&out);
            store.lose(&leaves);
            let got = futures::executor::block_on(fetch_erasure_bytes_progress(
                &store, out.root, None, window,
            ))
            .unwrap_or_else(|e| panic!("window={window}: {e}"));
            assert_eq!(got, data, "window={window}");
        }
    }

    /// Chunk addresses are deduplicated: repeated content (and the identical
    /// parity it produces) must not be emitted twice, or stamping burns two
    /// postage indices in one bucket.
    #[test]
    fn output_addresses_are_unique() {
        let data = vec![0u8; CHUNK_SIZE * 40];
        let out = split_with_redundancy(&data, Level::Medium).unwrap();
        let unique: HashSet<[u8; 32]> = out
            .chunks
            .iter()
            .map(|(a, _)| <[u8; 32]>::from(*a))
            .collect();
        assert_eq!(unique.len(), out.chunks.len());
        let store = MemStore::new(&out);
        let got = futures::executor::block_on(fetch_erasure_bytes(&store, out.root)).unwrap();
        assert_eq!(got, data);
    }
}
