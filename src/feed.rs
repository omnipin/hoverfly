//! Swarm feed **retrieval** (read-only).
//!
//! A feed is a sequence of single-owner chunks (SOC) published by one owner
//! under a topic, letting a stable address resolve to mutable content (this is
//! how feed-backed ENS sites like `swarm.eth` stay updatable). This module
//! resolves the *latest* update of a sequence-indexed feed and extracts the
//! content reference it points at. Creating/publishing feeds is out of scope.
//!
//! Algorithm (mirrors bee `pkg/feeds`):
//!
//! 1. A feed is `{ owner: 20-byte eth address, topic: 32 bytes }`.
//! 2. The update at sequence index `i` lives at a SOC address derived as
//!    `id = keccak256(topic || u64_be(i))` then
//!    `addr = keccak256(id || owner)` (== SOC `CreateAddress`).
//! 3. To find the latest update we bracket the head with a concurrent
//!    exponential probe (fan out doubling indices at once), then narrow it with
//!    a concurrent k-ary search of the bracket — both fully parallel so a
//!    high-index head resolves in a few round-trips, not one-per-index.
//! 4. The found chunk is a SOC. Its wrapped CAC body is the feed *payload*,
//!    laid out (legacy/v1) as `span(8) || timestamp(8) || reference(32[|64])`.
//!    The `reference` after the 16-byte prefix is the content manifest root.
//!
//! Feed parameters come from a **feed manifest**: a normal mantaray manifest
//! whose root (`/`) entry carries metadata keys `swarm-feed-owner`,
//! `swarm-feed-topic`, `swarm-feed-type` (see [`crate::manifest`]). ENS Swarm
//! contenthashes for mutable sites resolve to such a manifest.

use core::time::Duration;

use alloy_primitives::{Address, Keccak256};
use nectar_primitives::store::{ChunkGet, ChunkStoreError};
use nectar_primitives::{AnyChunk, ChunkAddress, DEFAULT_BODY_SIZE};

/// Metadata keys bee writes into a feed manifest's root entry
/// (`pkg/api/feed.go`).
pub const FEED_OWNER_KEY: &str = "swarm-feed-owner";
pub const FEED_TOPIC_KEY: &str = "swarm-feed-topic";
pub const FEED_TYPE_KEY: &str = "swarm-feed-type";

/// Sequence-feed index width (`uint64` big-endian), per bee `sequence.index`.
const INDEX_BYTES: usize = 8;

/// Legacy/v1 feed payload prefix preceding the wrapped content reference.
///
/// Bee's `feeds.legacyPayload` skips 16 bytes (`span(8) || timestamp(8)`) of
/// the raw CAC data. We operate on nectar's `BmtBody::data()`, which already
/// strips the 8-byte span (it's stored as a separate field), so the payload we
/// see is `timestamp(8) || reference(32)` and we skip only the 8-byte
/// timestamp.
const PAYLOAD_PREFIX: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedError {
    BadOwner(String),
    BadTopic(String),
    UnsupportedType(String),
    /// The fetched update payload was too short to contain a reference.
    ShortPayload(usize),
    /// No update was ever published for this feed.
    NoUpdate,
}

impl core::fmt::Display for FeedError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FeedError::BadOwner(s) => write!(f, "invalid feed owner: {s}"),
            FeedError::BadTopic(s) => write!(f, "invalid feed topic: {s}"),
            FeedError::UnsupportedType(s) => {
                write!(
                    f,
                    "unsupported feed type '{s}' (only Sequence is supported)"
                )
            }
            FeedError::ShortPayload(n) => write!(f, "feed update payload too short: {n} bytes"),
            FeedError::NoUpdate => write!(f, "feed has no updates"),
        }
    }
}
impl std::error::Error for FeedError {}

/// A sequence-indexed feed identified by its owner and topic.
#[derive(Debug, Clone)]
pub struct Feed {
    pub owner: Address,
    /// 32-byte topic (already hashed/raw as stored in the manifest).
    pub topic: [u8; 32],
}

impl Feed {
    /// Build a feed from the hex strings stored in a feed manifest's metadata.
    /// `owner_hex` is a 20-byte eth address; `topic_hex` is 32 bytes; `ty` must
    /// be the sequence type (case-insensitive "sequence").
    pub fn from_manifest_meta(
        owner_hex: &str,
        topic_hex: &str,
        ty: &str,
    ) -> Result<Self, FeedError> {
        if !ty.eq_ignore_ascii_case("sequence") {
            return Err(FeedError::UnsupportedType(ty.to_string()));
        }
        let owner_bytes = decode_hex(owner_hex).map_err(FeedError::BadOwner)?;
        if owner_bytes.len() != 20 {
            return Err(FeedError::BadOwner(format!(
                "expected 20 bytes, got {}",
                owner_bytes.len()
            )));
        }
        let owner = Address::from_slice(&owner_bytes);

        let topic_bytes = decode_hex(topic_hex).map_err(FeedError::BadTopic)?;
        if topic_bytes.len() != 32 {
            return Err(FeedError::BadTopic(format!(
                "expected 32 bytes, got {}",
                topic_bytes.len()
            )));
        }
        let mut topic = [0u8; 32];
        topic.copy_from_slice(&topic_bytes);

        Ok(Feed { owner, topic })
    }

    /// SOC address of the update at sequence index `i`:
    /// `keccak256( keccak256(topic || u64_be(i)) || owner )`.
    pub fn update_address(&self, i: u64) -> [u8; 32] {
        // id = keccak256(topic || index_be)
        let mut h = Keccak256::new();
        h.update(self.topic);
        let mut idx = [0u8; INDEX_BYTES];
        idx.copy_from_slice(&i.to_be_bytes());
        h.update(idx);
        let id = h.finalize();

        // addr = keccak256(id || owner)
        let mut h2 = Keccak256::new();
        h2.update(id);
        h2.update(self.owner.as_slice());
        h2.finalize().into()
    }
}

/// Extract the wrapped content reference from a feed update's payload (the
/// SOC's wrapped CAC body, *without* the chunk span — i.e. the inner data).
///
/// Layout (legacy/v1): `timestamp(8) || reference(32)` when the caller has
/// already stripped the CAC's own 8-byte span, OR `span(8) || timestamp(8) ||
/// reference(32)` when passed the full CAC data. We accept the full CAC data
/// and skip the 16-byte `span+timestamp` prefix (bee `cacData[16:]`).
///
/// Returns the 32-byte reference (encrypted/64-byte refs are not supported for
/// retrieval here and yield only the first 32 bytes' worth — callers should
/// treat a 64-byte payload tail as unsupported).
pub fn reference_from_payload(cac_data: &[u8]) -> Result<[u8; 32], FeedError> {
    if cac_data.len() < PAYLOAD_PREFIX + 32 {
        return Err(FeedError::ShortPayload(cac_data.len()));
    }
    let mut r = [0u8; 32];
    r.copy_from_slice(&cac_data[PAYLOAD_PREFIX..PAYLOAD_PREFIX + 32]);
    Ok(r)
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s).map_err(|e| e.to_string())
}

/// Pick up to `k` distinct, evenly-spaced candidate indices strictly inside the
/// open interval `(low, high)` for one concurrent k-ary search round. Returns
/// indices in ascending order with none equal to `low`/`high` and no
/// duplicates. Empty if the interval has no interior point.
fn bracket_candidates(low: u64, high: u64, k: u32) -> Vec<u64> {
    if high <= low + 1 {
        return Vec::new(); // no interior indices
    }
    let span = high - low - 1; // count of interior indices
    let probes = (k as u64).min(span);
    let mut out = Vec::with_capacity(probes as usize);
    for j in 1..=probes {
        // Evenly distribute j across the interior: positions span/(probes+1).
        let idx = low + (span * j) / (probes + 1) + 1;
        if idx > low && idx < high && out.last() != Some(&idx) {
            out.push(idx);
        }
    }
    out
}

/// Errors from resolving a feed over the network.
#[derive(Debug)]
pub enum ResolveError {
    Feed(FeedError),
    /// A chunk fetch failed for a reason other than "not found".
    Fetch(String),
    /// The update chunk wasn't a valid single-owner chunk.
    BadUpdate(String),
}

impl core::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ResolveError::Feed(e) => write!(f, "{e}"),
            ResolveError::Fetch(s) => write!(f, "feed update fetch failed: {s}"),
            ResolveError::BadUpdate(s) => write!(f, "invalid feed update chunk: {s}"),
        }
    }
}
impl std::error::Error for ResolveError {}
impl From<FeedError> for ResolveError {
    fn from(e: FeedError) -> Self {
        ResolveError::Feed(e)
    }
}

/// Maximum sequence index we'll probe when searching for the feed head. Guards
/// against an unbounded scan on a malformed feed; 2^20 updates is far beyond
/// any real feed-backed site.
const MAX_PROBE_INDEX: u64 = 1 << 20;

/// How many doubling levels to probe concurrently per exponential-search round.
/// Each level is one SOC network round-trip; fanning out a band of them hides
/// the per-chunk latency. 12 levels covers indices up to 2^12 = 4096 in a
/// single concurrent round.
const PROBE_BAND: u32 = 12;

/// Per-probe deadline for head-finding. The retrieval layer is built to
/// *exhaustively* locate a chunk that exists (racing every peer, full dial
/// budget, skiplist retries) — but feed head-finding deliberately probes
/// indices that DON'T exist, and without a deadline every such "miss" pays the
/// full give-up cost, making resolution glacial. Mirroring bee's asyncFinder
/// (which bounds each probe to 1s), we cap each probe so a missing index fails
/// fast and the search advances. Slightly more generous than bee's 1s to
/// absorb browser ws round-trip latency.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Resolve the **latest** update of a sequence feed and return the content
/// reference it points at.
///
/// Finds the head with an exponential probe (find the first missing index by
/// doubling) followed by a binary search in `[last_present, first_missing)`,
/// which costs ~`2·log2(n)` chunk fetches instead of `n`. Then parses the head
/// update as a single-owner chunk (via nectar) and extracts the wrapped
/// content reference from its CAC body.
pub async fn resolve_latest<S>(store: &S, feed: &Feed) -> Result<ChunkAddress, ResolveError>
where
    S: ChunkGet<DEFAULT_BODY_SIZE, Error = ChunkStoreError>,
{
    use futures::stream::{FuturesUnordered, StreamExt};

    tracing::info!(
        target: "hoverfly::feed",
        "resolving sequence feed: owner={} topic={}",
        feed.owner,
        hex::encode(feed.topic)
    );
    // Index 0 must exist for the feed to have any update at all.
    let chunk0 = match get_update(store, feed, 0).await? {
        Some(c) => c,
        None => return Err(ResolveError::Feed(FeedError::NoUpdate)),
    };

    // --- Phase 1: concurrent exponential boundary search. ---
    // Each SOC lookup is a slow network round-trip, so probe a whole band of
    // doubling indices (1,2,4,8,…) AT ONCE rather than one-at-a-time (mirrors
    // bee's asyncFinder doubling fan-out). From the batch results, `lo` = the
    // highest index found present, `hi` = the lowest index found absent. The
    // true head is somewhere in [lo, hi).
    let mut lo = 0u64; // highest present
    let mut last_chunk = chunk0;
    let mut hi = u64::MAX; // lowest absent (MAX = "not yet found")

    // Probe levels 0..PROBE_BAND (indices 2^1..2^BAND) concurrently per round;
    // if all present, advance the base and probe the next band.
    let mut base = 1u64;
    'outer: loop {
        let mut futs = FuturesUnordered::new();
        for k in 0..PROBE_BAND {
            let idx = base.saturating_mul(1u64 << k);
            if idx > MAX_PROBE_INDEX {
                break;
            }
            futs.push(async move { (idx, get_update(store, feed, idx).await) });
        }
        if futs.is_empty() {
            break;
        }
        let mut any_present = false;
        let mut top = base; // highest index probed this round
        while let Some((idx, res)) = futs.next().await {
            top = top.max(idx);
            match res? {
                Some(c) => {
                    any_present = true;
                    if idx > lo {
                        lo = idx;
                        last_chunk = c;
                    }
                }
                None => {
                    if idx < hi {
                        hi = idx;
                    }
                }
            }
        }
        // If we found an absent index, the boundary is bracketed; stop probing.
        if hi != u64::MAX {
            break 'outer;
        }
        // All present in this band — jump the base past the top we probed.
        if !any_present || top >= MAX_PROBE_INDEX {
            break;
        }
        base = top.saturating_mul(2);
    }

    // --- Phase 2: concurrent k-ary search of the bracket (lo present, hi absent).
    // A plain binary search here is SEQUENTIAL — each step waits a full round-
    // trip (and a miss waits the whole PROBE_TIMEOUT), which dominated total
    // resolution time. Instead, split the open interval (low, high) into
    // PROBE_BAND equally-spaced candidate indices and probe them ALL at once;
    // the results narrow the bracket to (highest-present, lowest-absent) in a
    // single round. This collapses ~log2(gap) sequential probes into
    // ~log_PROBE_BAND(gap) concurrent rounds (a 64-wide gap: 1 round vs ~6).
    if hi != u64::MAX {
        let mut low = lo;
        let mut high = hi;
        while high - low > 1 {
            let candidates = bracket_candidates(low, high, PROBE_BAND);
            let mut futs = FuturesUnordered::new();
            for idx in candidates {
                futs.push(async move { (idx, get_update(store, feed, idx).await) });
            }
            if futs.is_empty() {
                break;
            }
            let mut round_low = low;
            let mut round_high = high;
            let mut round_chunk: Option<AnyChunk<DEFAULT_BODY_SIZE>> = None;
            while let Some((idx, res)) = futs.next().await {
                match res? {
                    Some(c) => {
                        if idx > round_low {
                            round_low = idx;
                            round_chunk = Some(c);
                        }
                    }
                    None => {
                        if idx < round_high {
                            round_high = idx;
                        }
                    }
                }
            }
            // Make progress guard: if the round somehow didn't tighten the
            // bracket (shouldn't happen), stop to avoid an infinite loop.
            if round_low == low && round_high == high {
                break;
            }
            if let Some(c) = round_chunk {
                last_chunk = c;
                lo = round_low;
            }
            low = round_low;
            high = round_high;
        }
    }

    // `last_chunk` is the head update SOC, parsed+validated by the retrieval
    // layer. `AnyChunk::data()` on a single-owner chunk returns its *wrapped*
    // CAC body — exactly the feed payload (`span+timestamp+reference`) — so we
    // extract the content reference from it directly.
    let reference = reference_from_payload(last_chunk.data().as_ref())?;
    tracing::info!(
        target: "hoverfly::feed",
        "resolved feed head: index={} -> content ref {}",
        lo,
        hex::encode(reference)
    );
    Ok(ChunkAddress::new(reference))
}

/// Fetch the feed update at index `i`. Returns `Ok(None)` if the update chunk
/// doesn't exist (a network "not found"), `Ok(Some(chunk))` if it does, and
/// `Err` only on a genuine fetch failure (so a missing index ends the search
/// rather than aborting it).
async fn get_update<S>(
    store: &S,
    feed: &Feed,
    i: u64,
) -> Result<Option<AnyChunk<DEFAULT_BODY_SIZE>>, ResolveError>
where
    S: ChunkGet<DEFAULT_BODY_SIZE, Error = ChunkStoreError>,
{
    let addr = ChunkAddress::new(feed.update_address(i));

    // Bound the probe: race the retrieval against a deadline. A probe that
    // doesn't resolve within PROBE_TIMEOUT is treated as "absent" for the
    // search (bee's asyncFinder does the same with a 1s per-probe ctx). This is
    // what makes head-finding fast — missing indices no longer pay the full
    // exhaustive-retrieval give-up cost. `tokio::time::timeout` is portable
    // here: native uses real tokio, wasm uses tokio_with_wasm (both have the
    // `time` feature, gloo-timer backed on wasm).
    match tokio::time::timeout(PROBE_TIMEOUT, store.get(&addr)).await {
        Ok(Ok(c)) => Ok(Some(c)),
        Ok(Err(ChunkStoreError::NotFound { .. })) => Ok(None),
        // "no peer found" / "all peers failed" => nobody is serving this SOC
        // address right now; treat as absent for head-finding.
        Ok(Err(ChunkStoreError::Other(msg))) if is_absent_retrieval(&msg) => Ok(None),
        Ok(Err(e)) => Err(ResolveError::Fetch(e.to_string())),
        // Timed out — treat as absent.
        Err(_) => Ok(None),
    }
}

/// Heuristic: a retrieval error string that indicates the chunk simply isn't
/// being served (vs. a transport/protocol failure we should surface).
fn is_absent_retrieval(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("no peer found") || m.contains("not found") || m.contains("all peers failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_sequence_type() {
        let err = Feed::from_manifest_meta(
            "00112233445566778899aabbccddeeff00112233",
            &"11".repeat(32),
            "epoch",
        )
        .unwrap_err();
        assert!(matches!(err, FeedError::UnsupportedType(_)));
    }

    #[test]
    fn rejects_bad_lengths() {
        assert!(matches!(
            Feed::from_manifest_meta("00", &"11".repeat(32), "sequence").unwrap_err(),
            FeedError::BadOwner(_)
        ));
        assert!(matches!(
            Feed::from_manifest_meta(
                "00112233445566778899aabbccddeeff00112233",
                "1122",
                "sequence"
            )
            .unwrap_err(),
            FeedError::BadTopic(_)
        ));
    }

    #[test]
    fn update_address_is_deterministic_and_index_sensitive() {
        let f = Feed::from_manifest_meta(
            "00112233445566778899aabbccddeeff00112233",
            &"22".repeat(32),
            "Sequence",
        )
        .unwrap();
        let a0 = f.update_address(0);
        let a0_again = f.update_address(0);
        let a1 = f.update_address(1);
        assert_eq!(a0, a0_again);
        assert_ne!(a0, a1);
    }

    #[test]
    fn payload_reference_skips_span_and_timestamp() {
        let mut data = vec![0u8; PAYLOAD_PREFIX];
        let reference = [0xABu8; 32];
        data.extend_from_slice(&reference);
        assert_eq!(reference_from_payload(&data).unwrap(), reference);
    }

    #[test]
    fn short_payload_errors() {
        assert!(matches!(
            reference_from_payload(&[0u8; 10]).unwrap_err(),
            FeedError::ShortPayload(10)
        ));
    }

    #[test]
    fn bracket_candidates_are_interior_sorted_unique() {
        // Wide gap, k=12: up to 12 distinct interior, all strictly inside.
        let c = bracket_candidates(64, 128, 12);
        assert!(!c.is_empty());
        assert!(c.len() <= 12);
        assert!(c.iter().all(|&i| i > 64 && i < 128), "all interior: {c:?}");
        assert!(c.windows(2).all(|w| w[0] < w[1]), "sorted+unique: {c:?}");
    }

    #[test]
    fn bracket_candidates_edges() {
        assert!(bracket_candidates(10, 11, 12).is_empty()); // adjacent: no interior
        assert!(bracket_candidates(10, 10, 12).is_empty()); // empty
        // Single interior index -> exactly one candidate.
        assert_eq!(bracket_candidates(10, 12, 12), vec![11]);
        // Gap smaller than k: at most `span` candidates, no dupes.
        let c = bracket_candidates(0, 5, 12); // interior 1,2,3,4
        assert!(c.len() <= 4);
        assert!(c.iter().all(|&i| (1..=4).contains(&i)));
        assert!(c.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn bracket_candidates_converge() {
        // Repeatedly narrowing (low,high) by picking the candidate just below a
        // fixed head must reach the head in few rounds (the concurrency win).
        let head = 1000u64;
        let (mut low, mut high) = (0u64, 4096u64); // head bracketed
        let mut rounds = 0;
        while high - low > 1 {
            rounds += 1;
            let cand = bracket_candidates(low, high, 12);
            assert!(!cand.is_empty());
            // Simulate: present iff idx <= head.
            let mut rl = low;
            let mut rh = high;
            for &idx in &cand {
                if idx <= head {
                    rl = rl.max(idx);
                } else {
                    rh = rh.min(idx);
                }
            }
            assert!(rl > low || rh < high, "must make progress");
            low = rl;
            high = rh;
        }
        assert_eq!(low, head);
        assert!(
            rounds <= 4,
            "12-ary over 4096 should converge in <=4 rounds, took {rounds}"
        );
    }

    /// Cross-check the SOC address derivation `keccak256(id || owner)` against
    /// bee's `TestCreateAddress` vector (pkg/soc/soc_test.go): id = 32 zero
    /// bytes, owner = 8d3766…e632 -> 9d453ebb…6d61dc85. This guards the inner
    /// half of `update_address` (the id→address step) against any keccak
    /// ordering/encoding drift vs. the network.
    #[test]
    fn soc_create_address_matches_bee_vector() {
        let id = [0u8; 32];
        let owner =
            Address::from_slice(&decode_hex("8d3766440f0d7b949a5e32995d09619a7f86e632").unwrap());
        let mut h = Keccak256::new();
        h.update(id);
        h.update(owner.as_slice());
        let addr: [u8; 32] = h.finalize().into();
        assert_eq!(
            hex::encode(addr),
            "9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85"
        );
    }
}
