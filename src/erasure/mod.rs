//! Erasure-coding-aware download for Swarm content.
//!
//! ## Why this exists
//!
//! Since ~bee v2.8.1, gateway uploads (`POST /bytes`, `POST /bzz`) are
//! **Reed–Solomon erasure coded by default** (redundancy level MEDIUM). A file
//! is still a BMT tree of content chunks, but every intermediate ("hashtrie")
//! node now carries, in addition to its data-shard references, a run of
//! **parity-chunk** references. The parity chunks are computed by RS-encoding
//! the sibling data chunks at that level, so any missing data chunk in a node
//! can be reconstructed from the surviving data + parity chunks of that same
//! node.
//!
//! A freshly-uploaded object exists only in its storage neighbourhood; a
//! well-connected gateway reaches every chunk in one hop, but a
//! forwarding-dependent light client (hoverfly) sees a fraction of the data
//! chunks time out (`storage: not found`). Without erasure support the whole
//! fetch fails. *With* it, we fetch the parity siblings too and reconstruct the
//! unretrievable data chunks — exactly what bee's own joiner does
//! (`pkg/file/redundancy/getter`). This is the fix for
//! ethersphere/bee#5541 on the client side.
//!
//! ## Wire format (must match bee exactly)
//!
//! nectar's `GenericJoiner` is size-driven: it reconstructs the tree purely
//! from the root span and a fixed branching factor, assuming every reference is
//! a data chunk. That's wrong for erasure-coded content (the extra parity refs
//! are not data, and the effective branching per node varies with the parity
//! count). So we can't reuse it; we port bee's joiner here instead.
//!
//! Key facts, verified against `~/Coding/forks/bee/pkg`:
//! - **Span byte:** the redundancy level is encoded in the *most significant
//!   byte* of a chunk's 8-byte little-endian span: `span[7] = level | 0x80`
//!   (`pkg/file/redundancy/span.go`). If bit 7 is set the chunk is part of an
//!   erasure-coded tree; the low 7 bits are the [`Level`]. Masking the byte to
//!   zero recovers the true length. The BMT address is computed over the
//!   *level-encoded* span, so we must not strip it before hashing/verifying.
//! - **Per-node shard/parity split:** given a node's subtree span and the
//!   level, [`reference_count`] brute-forces how many *data* references the node
//!   holds; the level's erasure table then gives the parity count
//!   (`pkg/file/utils.go::ReferenceCount`). The node payload lays out
//!   `shardCnt` data refs followed by `parityCnt` parity refs.
//! - **Field/matrix:** GF(2^8), poly 0x11D, klauspost default Vandermonde→
//!   identity matrix (see [`reedsolomon`]).

mod reedsolomon;

pub mod joiner;

pub use joiner::{ErasureError, fetch_erasure_bytes, fetch_erasure_bytes_progress};

use nectar_primitives::bmt::{HASH_SIZE, SPAN_SIZE};

/// Bee `swarm.ChunkSize` — 4096 bytes of payload per content chunk.
pub const CHUNK_SIZE: usize = 4096;
/// Bee `swarm.Branches` — 128 references per full intermediate chunk (plain).
pub const BRANCHES: usize = 128;

/// Redundancy level, mirroring bee `pkg/file/redundancy/level.go`.
///
/// The numeric values are the on-wire encoding (stored in the span's top byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// No redundancy.
    None = 0,
    /// ~1% expected chunk retrieval error rate (bee's default upload level).
    Medium = 1,
    /// ~5%.
    Strong = 2,
    /// ~10%.
    Insane = 3,
    /// ~50%.
    Paranoid = 4,
}

impl Level {
    fn from_u8(v: u8) -> Option<Level> {
        Some(match v {
            0 => Level::None,
            1 => Level::Medium,
            2 => Level::Strong,
            3 => Level::Insane,
            4 => Level::Paranoid,
            _ => return None,
        })
    }

    /// Erasure table for non-encrypted chunks (bee appendix F table 5).
    ///
    /// Each `(shards, parities)` pair says: for up to `shards` data references,
    /// use `parities` parity references. Lookup scans descending and returns
    /// the first row whose `shards` bound is `<= max_shards`.
    fn erasure_table(self) -> &'static [(usize, usize)] {
        match self {
            Level::None => &[],
            Level::Medium => &MEDIUM_ET,
            Level::Strong => &STRONG_ET,
            Level::Insane => &INSANE_ET,
            Level::Paranoid => &PARANOID_ET,
        }
    }

    /// Number of parity references for `shards` data references at this level.
    /// Mirrors `Level.GetParities` (non-encrypted path).
    pub fn parities(self, shards: usize) -> usize {
        for &(s, p) in self.erasure_table() {
            if shards >= s {
                return p;
            }
        }
        0
    }

    /// Maximum number of effective data references in a full intermediate
    /// chunk at this level. Mirrors `Level.GetMaxShards`.
    pub fn max_shards(self) -> usize {
        let p = self.parities(BRANCHES);
        BRANCHES - p
    }
}

// Erasure tables copied verbatim from bee `pkg/file/redundancy/level.go`.
// (shards, parities), strictly descending in both columns.
static MEDIUM_ET: [(usize, usize); 8] = [
    (95, 9),
    (69, 8),
    (47, 7),
    (29, 6),
    (15, 5),
    (6, 4),
    (2, 3),
    (1, 2),
];

static STRONG_ET: [(usize, usize); 18] = [
    (105, 21),
    (96, 20),
    (87, 19),
    (78, 18),
    (70, 17),
    (62, 16),
    (54, 15),
    (47, 14),
    (40, 13),
    (33, 12),
    (27, 11),
    (21, 10),
    (16, 9),
    (11, 8),
    (7, 7),
    (4, 6),
    (2, 5),
    (1, 4),
];

static INSANE_ET: [(usize, usize); 27] = [
    (93, 31),
    (88, 30),
    (83, 29),
    (78, 28),
    (74, 27),
    (69, 26),
    (64, 25),
    (60, 24),
    (55, 23),
    (51, 22),
    (46, 21),
    (42, 20),
    (38, 19),
    (34, 18),
    (30, 17),
    (27, 16),
    (23, 15),
    (20, 14),
    (17, 13),
    (14, 12),
    (11, 11),
    (9, 10),
    (6, 9),
    (4, 8),
    (3, 7),
    (2, 6),
    (1, 5),
];

static PARANOID_ET: [(usize, usize); 37] = [
    (37, 89),
    (36, 87),
    (35, 86),
    (34, 84),
    (33, 83),
    (32, 81),
    (31, 80),
    (30, 78),
    (29, 76),
    (28, 75),
    (27, 73),
    (26, 71),
    (25, 70),
    (24, 68),
    (23, 66),
    (22, 65),
    (21, 63),
    (20, 61),
    (19, 59),
    (18, 58),
    (17, 56),
    (16, 54),
    (15, 52),
    (14, 50),
    (13, 48),
    (12, 47),
    (11, 45),
    (10, 43),
    (9, 40),
    (8, 38),
    (7, 36),
    (6, 34),
    (5, 31),
    (4, 29),
    (3, 26),
    (2, 23),
    (1, 19),
];

/// Decode the redundancy level from a chunk's raw 8-byte span and return the
/// true byte-length with the level byte cleared.
///
/// Mirrors bee `pkg/file/redundancy/span.go::DecodeSpan`. `span` is
/// little-endian; the level lives in `span[7]` when bit 7 is set.
pub fn decode_span(span: &[u8]) -> (Level, u64) {
    debug_assert!(span.len() >= SPAN_SIZE);
    let mut buf = [0u8; SPAN_SIZE];
    buf.copy_from_slice(&span[..SPAN_SIZE]);
    if buf[SPAN_SIZE - 1] <= 128 {
        // No level encoded (top byte not > 128); whole thing is the length.
        return (Level::None, u64::from_le_bytes(buf));
    }
    let level = Level::from_u8(buf[SPAN_SIZE - 1] & 0x7f).unwrap_or(Level::None);
    buf[SPAN_SIZE - 1] = 0;
    (level, u64::from_le_bytes(buf))
}

/// Whether a raw span has a redundancy level encoded in it.
pub fn is_level_encoded(span: &[u8]) -> bool {
    span.len() >= SPAN_SIZE && span[SPAN_SIZE - 1] > 128
}

/// Brute-force the number of *data* references and *parity* references an
/// intermediate node holds, from its subtree `span` and redundancy `level`.
///
/// Mirrors bee `pkg/file/utils.go::ReferenceCount` (non-encrypted path). The
/// tree is built so that every branch of an intermediate node except possibly
/// the last covers the same span; this walks up BMT levels until one branch can
/// hold `span`, computes how much one reference covers at that level, and
/// counts how many references are needed to cover `span`.
pub fn reference_count(span: u64, level: Level) -> (usize, usize) {
    let max_shards = level.max_shards().max(1);
    let branching = max_shards as u64;
    let mut branch_size = CHUNK_SIZE as u64;

    // Find the BMT level whose single-reference span is large enough to include
    // `span`, tracking the level number as we go.
    let mut branch_level = 1;
    while branch_size < span {
        branch_size *= branching;
        branch_level += 1;
    }

    // Span covered by one full reference at the level below `branch_level`.
    let mut reference_size = CHUNK_SIZE as u64;
    let mut i = 1;
    while i < branch_level - 1 {
        reference_size *= branching;
        i += 1;
    }

    // Count how many references it takes to cover `span`.
    let mut data_shards = 1usize;
    let mut span_offset = reference_size;
    while span_offset < span {
        span_offset += reference_size;
        data_shards += 1;
    }

    let parities = level.parities(data_shards);
    (data_shards, parities)
}

/// Split an intermediate chunk's payload into `(addresses, shard_count)`.
///
/// The payload (already trimmed of trailing zero references, see
/// [`chunk_payload_size`]) lays out `shard_count` 32-byte data references
/// followed by `parities` 32-byte parity references. Mirrors bee
/// `pkg/file/utils.go::ChunkAddresses` (non-encrypted path, ref len = 32).
pub fn chunk_addresses(payload: &[u8], parities: usize) -> (Vec<[u8; 32]>, usize) {
    let ref_len = HASH_SIZE;
    let shard_count = (payload.len().saturating_sub(parities * HASH_SIZE)) / ref_len;
    let mut addrs = Vec::with_capacity(payload.len() / ref_len);
    let mut offset = 0;
    while offset + HASH_SIZE <= payload.len() {
        let mut a = [0u8; 32];
        a.copy_from_slice(&payload[offset..offset + HASH_SIZE]);
        addrs.push(a);
        offset += ref_len;
    }
    (addrs, shard_count)
}

/// Effective payload length of an intermediate chunk: strip trailing all-zero
/// 32-byte references (padding). Mirrors bee `pkg/file/utils.go::ChunkPayloadSize`.
///
/// Returns `None` if the chunk has no non-zero reference at all.
pub fn chunk_payload_size(data: &[u8]) -> Option<usize> {
    let mut l = data.len();
    let zero = [0u8; HASH_SIZE];
    while l >= HASH_SIZE {
        if data[l - HASH_SIZE..l] != zero {
            return Some(l);
        }
        l -= HASH_SIZE;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_level_roundtrip() {
        // length 40_491_008, no level → (None, len)
        let span = 40_491_008u64.to_le_bytes();
        let (lvl, len) = decode_span(&span);
        assert_eq!(lvl, Level::None);
        assert_eq!(len, 40_491_008);
        assert!(!is_level_encoded(&span));

        // MEDIUM-encoded span
        let mut enc = 40_491_008u64.to_le_bytes();
        enc[SPAN_SIZE - 1] = (Level::Medium as u8) | 0x80;
        let (lvl, len) = decode_span(&enc);
        assert_eq!(lvl, Level::Medium);
        assert_eq!(len, 40_491_008);
        assert!(is_level_encoded(&enc));
    }

    #[test]
    fn medium_max_shards() {
        // MEDIUM at 128 refs → 9 parities → 119 max data shards.
        assert_eq!(Level::Medium.parities(128), 9);
        assert_eq!(Level::Medium.max_shards(), 119);
    }

    #[test]
    fn parities_scale_with_shards() {
        // MEDIUM table: 2 data shards → 3 parities; 1 → 2.
        assert_eq!(Level::Medium.parities(2), 3);
        assert_eq!(Level::Medium.parities(1), 2);
        assert_eq!(Level::Medium.parities(0), 0);
    }

    #[test]
    fn chunk_payload_size_strips_padding() {
        // 3 real refs + 2 zero refs → payload = 3*32.
        let mut data = vec![0u8; 5 * 32];
        for i in 0..3 {
            data[i * 32] = (i + 1) as u8;
        }
        assert_eq!(chunk_payload_size(&data), Some(3 * 32));
        // all-zero → None
        assert_eq!(chunk_payload_size(&vec![0u8; 64]), None);
    }

    #[test]
    fn chunk_addresses_splits_data_and_parity() {
        // 4 data + 2 parity refs.
        let mut payload = vec![0u8; 6 * 32];
        for i in 0..6 {
            payload[i * 32] = (i + 1) as u8;
        }
        let (addrs, shards) = chunk_addresses(&payload, 2);
        assert_eq!(addrs.len(), 6);
        assert_eq!(shards, 4);
    }

    #[test]
    fn reference_count_single_level_medium() {
        // A file just over one chunk of data but small enough to fit in one
        // intermediate node: span = 10 * 4096 → 10 data shards at MEDIUM.
        let (data, parity) = reference_count(10 * CHUNK_SIZE as u64, Level::Medium);
        assert_eq!(data, 10);
        assert_eq!(parity, Level::Medium.parities(10));
    }
}
