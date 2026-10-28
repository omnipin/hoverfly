//! Non-canonical BMT trees: chunk-level DoS regression tests.
//!
//! Every chunk built here is *individually valid* — a real content chunk whose
//! span matches its payload, addressed by the BMT hash of its own wire bytes.
//! What can be invalid is the *shape* of the tree they form: the children's
//! spans don't add up to the parent's declared span. A canonical splitter never
//! produces that, but anyone publishing chunks directly (which postage stamps
//! allow) can, so the download path must reject it rather than trust it.
//!
//! Before hoverfly walked by spans, the plain path used nectar's
//! `GenericJoiner`, which sizes each child from the *parent's* span and never
//! checks the child against it:
//!
//! - a leaf shorter than the implied size made `assemble_range` slice out of
//!   bounds — a panic in the CLI, a poisoned HTTP request in the daemon, and a
//!   taken-down `SharedWorker` in the wasm gateway;
//! - a root declaring more bytes than its children cover was assembled from
//!   whatever was there, silently handing the caller a wrong or truncated file.
//!
//! The [[`plain_short_leaf_is_rejected_not_panicked`]] test reproduced the
//! panic at `nectar-primitives/src/file/tree.rs:213` before the fix. The
//! allocation tests cover the other half: the root span is attacker-chosen and
//! used to size the output buffer, so it has to be capped before allocating.
//!
//! Run: `cargo test --test joiner_hardening`

use std::collections::HashMap;

use bytes::Bytes;
use hoverfly::erasure::{ErasureError, JoinLimits, WireGet, fetch_erasure_bytes};
use nectar_primitives::Chunk;
use nectar_primitives::bmt::{DEFAULT_BODY_SIZE, SPAN_SIZE};
use nectar_primitives::chunk::{AnyChunk, ChunkAddress, ContentChunk};
use nectar_primitives::file::split;

const CHUNK_SIZE: usize = 4096;
const MEDIUM_LEVEL: u8 = 0x81; // span top byte: Level::Medium | 0x80

// ── chunk/tree builders ──────────────────────────────────────────────────

/// Build `span_LE_8 || payload` and its address. nectar enforces `span == len`
/// for leaves (`span <= 4096`), so every chunk built through here is a chunk
/// bee itself would accept.
fn chunk(span: u64, payload: &[u8]) -> (ChunkAddress, Vec<u8>) {
    let mut wire = Vec::with_capacity(SPAN_SIZE + payload.len());
    wire.extend_from_slice(&span.to_le_bytes());
    wire.extend_from_slice(payload);
    let address = *ContentChunk::<DEFAULT_BODY_SIZE>::try_from(Bytes::from(wire.clone()))
        .expect("individually valid chunk")
        .address();
    (address, wire)
}

fn leaf(span: u64, payload: &[u8]) -> (ChunkAddress, Vec<u8>) {
    chunk(span, payload)
}

/// An intermediate node: a span larger than a chunk plus a body of 32-byte refs.
fn intermediate(span: u64, refs: &[ChunkAddress]) -> (ChunkAddress, Vec<u8>) {
    let mut payload = Vec::with_capacity(refs.len() * 32);
    for r in refs {
        payload.extend_from_slice(r.as_bytes());
    }
    chunk(span, &payload)
}

fn level_encoded_len(len: u64, level_byte: u8) -> u64 {
    // The level byte occupies the span's top byte, leaving 56 bits of length.
    assert!(
        len < (1u64 << 56),
        "level-encoded span is limited to 56 bits"
    );
    let mut span = len.to_le_bytes();
    span[SPAN_SIZE - 1] = level_byte;
    u64::from_le_bytes(span)
}

/// Wire form of a nectar chunk: `span_LE_8 || payload`.
fn wire_form(chunk: &AnyChunk<DEFAULT_BODY_SIZE>) -> Vec<u8> {
    let mut wire = Vec::with_capacity(SPAN_SIZE + chunk.data().len());
    wire.extend_from_slice(&chunk.span().to_le_bytes());
    wire.extend_from_slice(chunk.data());
    wire
}

struct WireStore(HashMap<ChunkAddress, Vec<u8>>);

impl WireStore {
    fn new(entries: impl IntoIterator<Item = (ChunkAddress, Vec<u8>)>) -> Self {
        Self(entries.into_iter().collect())
    }
}

impl WireGet for WireStore {
    type Error = String;

    async fn get_wire(&self, address: &ChunkAddress) -> Result<Vec<u8>, Self::Error> {
        self.0
            .get(address)
            .cloned()
            .ok_or_else(|| format!("not found: {address}"))
    }
}

fn join(
    entries: impl IntoIterator<Item = (ChunkAddress, Vec<u8>)>,
    root: ChunkAddress,
) -> Result<Vec<u8>, ErasureError> {
    let store = WireStore::new(entries);
    futures::executor::block_on(fetch_erasure_bytes(&store, root))
}

// ── the plain path must stay byte-exact ──────────────────────────────────

/// Plain (non-redundant) trees produced by nectar's own splitter must round-trip
/// byte-for-byte through the validated walk: this is what `client::join_target`
/// now runs for every non-level-encoded root.
#[test]
fn plain_canonical_trees_round_trip() {
    let sizes = [
        0,
        1,
        CHUNK_SIZE - 1,
        CHUNK_SIZE,
        CHUNK_SIZE + 1,
        3 * CHUNK_SIZE + 123,
        // Exactly one full intermediate node, then one that needs a second.
        128 * CHUNK_SIZE,
        128 * CHUNK_SIZE + CHUNK_SIZE,
        // Three levels: a root over intermediates.
        129 * CHUNK_SIZE + 7,
        200 * CHUNK_SIZE + 5,
    ];

    for size in sizes {
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let (root, store) = split::<DEFAULT_BODY_SIZE>(&data).expect("split");
        let chunks = store
            .into_chunks()
            .into_iter()
            .map(|(addr, chunk)| (addr, wire_form(&chunk)))
            .collect::<Vec<_>>();

        let got = join(chunks, root).expect("plain tree must join");
        assert_eq!(got, data, "size {size} must round-trip byte-exactly");
    }
}

// ── plain path: non-canonical trees are rejected, not panicked on ───────

/// A two-leaf tree whose last leaf is one byte shorter than the size the root's
/// span implies. Both leaves are valid chunks; only the sum is wrong.
///
/// Reproduced before the fix: `assemble_range` sliced the short body at the
/// assumed length and panicked (`range end index 4096 out of range for slice of
/// length 4095`).
#[test]
fn plain_short_leaf_is_rejected_not_panicked() {
    let root_span = 2 * CHUNK_SIZE as u64; // implies two full leaves
    let (a0, w0) = leaf(CHUNK_SIZE as u64, &[b'A'; CHUNK_SIZE]);
    let (a1, w1) = leaf(CHUNK_SIZE as u64 - 1, &[b'B'; CHUNK_SIZE - 1]);
    let (root, wroot) = intermediate(root_span, &[a0, a1]);

    let got = join([(a0, w0), (a1, w1), (root, wroot)], root);
    assert!(
        got.is_err(),
        "non-canonical tree must be rejected, got {got:?}"
    );
}

/// Same tree with the short leaf first: the outcome must not depend on which
/// leaf is short.
#[test]
fn plain_short_leaf_first_is_rejected_not_panicked() {
    let root_span = 2 * CHUNK_SIZE as u64;
    let (a0, w0) = leaf(CHUNK_SIZE as u64 - 1, &[b'A'; CHUNK_SIZE - 1]);
    let (a1, w1) = leaf(CHUNK_SIZE as u64, &[b'B'; CHUNK_SIZE]);
    let (root, wroot) = intermediate(root_span, &[a0, a1]);

    let got = join([(a0, w0), (a1, w1), (root, wroot)], root);
    assert!(
        got.is_err(),
        "non-canonical tree must be rejected, got {got:?}"
    );
}

/// A root claiming *more* bytes than its children cover used to assemble
/// whatever was present and return it as if complete.
#[test]
fn plain_underfull_tree_is_rejected() {
    let (a0, w0) = leaf(CHUNK_SIZE as u64, &[b'A'; CHUNK_SIZE]);
    // Declares two leaves' worth of bytes, holds one.
    let (root, wroot) = intermediate(2 * CHUNK_SIZE as u64, &[a0]);

    let got = join([(a0, w0), (root, wroot)], root);
    assert!(got.is_err(), "underfull tree must be rejected, got {got:?}");
}

// ── erasure path (hoverfly's own joiner) ─────────────────────────────────

/// A MEDIUM-encoded root declaring 8192 bytes whose two data children only
/// cover 4096 + 3000. Each child is valid; the sum is not the root's span.
///
/// Reproduced before the fix: `Ok` with 7096 bytes — a silently truncated file,
/// because the join ended with `out.truncate(span)` and never checked the
/// length it actually assembled.
#[test]
fn erasure_children_spans_must_sum_to_parent_span() {
    let declared = 2 * CHUNK_SIZE as u64; // 8192
    let (a0, w0) = leaf(CHUNK_SIZE as u64, &[b'A'; CHUNK_SIZE]);
    let (a1, w1) = leaf(3000, &[b'B'; 3000]);

    // MEDIUM with 2 data refs implies 3 parities; they are unreachable here,
    // which the joiner tolerates because both data shards arrive.
    let mut payload = Vec::new();
    payload.extend_from_slice(a0.as_bytes());
    payload.extend_from_slice(a1.as_bytes());
    payload.extend_from_slice(&[0xEEu8; 32][..]);
    payload.extend_from_slice(&[0xEEu8; 32][..]);
    payload.extend_from_slice(&[0xEEu8; 32][..]);
    let (root, wroot) = chunk(level_encoded_len(declared, MEDIUM_LEVEL), &payload);

    let got = join([(a0, w0), (a1, w1), (root, wroot)], root);
    assert!(
        got.is_err(),
        "children must cover the parent span, got {got:?}"
    );
}

/// The same check in the other direction: children claiming *more* than their
/// parent must be rejected too, not assembled past the declared length.
#[test]
fn erasure_oversized_children_are_rejected() {
    let declared = CHUNK_SIZE as u64; // 4096 declared…
    let (a0, w0) = leaf(CHUNK_SIZE as u64, &[b'A'; CHUNK_SIZE]);
    let (a1, w1) = leaf(CHUNK_SIZE as u64, &[b'B'; CHUNK_SIZE]); // …but 8192 present

    let mut payload = Vec::new();
    payload.extend_from_slice(a0.as_bytes());
    payload.extend_from_slice(a1.as_bytes());
    let (root, wroot) = chunk(level_encoded_len(declared, MEDIUM_LEVEL), &payload);

    let got = join([(a0, w0), (a1, w1), (root, wroot)], root);
    assert!(got.is_err(), "overfull tree must be rejected, got {got:?}");
}

// ── attacker-controlled sizes and depth ─────────────────────────────────

/// A root span far larger than anything we would serve must be refused
/// *before* allocating for it. A level-encoded span carries up to 56 bits of
/// length, so one root chunk can ask for ~72 PB.
///
/// Reproduced before the fix: `Vec::with_capacity(span as usize)` → `memory
/// allocation of 72057594037927935 bytes failed` → SIGABRT. One chunk, one
/// connection, process gone.
#[test]
fn erasure_huge_root_span_is_refused_before_allocating() {
    let huge = (1u64 << 56) - 1;
    let (root, wroot) = chunk(level_encoded_len(huge, MEDIUM_LEVEL), &[0u8; 32]);

    match join([(root, wroot)], root) {
        Err(ErasureError::TooLarge { span, .. }) => assert_eq!(span, huge),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

/// Same, for a plain (level-less) root: the span is just as attacker-chosen.
#[test]
fn plain_huge_root_span_is_refused_before_allocating() {
    let huge = 1u64 << 40; // 1 TiB
    let (root, wroot) = chunk(huge, &[0u8; 32]);

    match join([(root, wroot)], root) {
        Err(ErasureError::TooLarge { span, .. }) => assert_eq!(span, huge),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

/// A chain of single-reference intermediates nests arbitrarily deep; the walk is
/// recursive, so it must be depth-bounded rather than left to overflow the
/// stack.
#[test]
fn deep_chain_is_rejected_not_stack_overflowed() {
    let span = 8192u64;
    // Build the chain back to front: 100 nodes, each holding one reference to
    // the next, each declaring the same span (so every sum check passes and the
    // walk keeps descending).
    let mut next = chunk(span, &[0u8; 32]);
    let mut entries = Vec::new();
    for _ in 0..100 {
        entries.push(next.clone());
        next = intermediate(span, &[next.0]);
    }
    let (root, wroot) = next;
    entries.push((root, wroot));

    match join(entries, root) {
        Err(ErasureError::TooDeep { max }) => assert_eq!(max, JoinLimits::default().max_depth),
        other => panic!("expected TooDeep, got {:?}", other.map(|b| b.len())),
    }
}
