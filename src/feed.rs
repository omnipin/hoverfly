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
/// fast and the search advances. Once the search is concurrent this is the
/// dominant cost (each round waits its slowest *absent* probe), so keep it
/// tight; 1.5s leaves headroom over bee's 1s for browser ws round-trip latency
/// while roughly halving per-round cost vs the initial 3s.
const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// How many times to re-probe the *anchor* index before concluding a feed has
/// no updates. The anchor is special: index 0 must exist for any published
/// feed, and a cached hint index was proven present on a prior resolve, so a
/// bounded "absent" there (a `PROBE_TIMEOUT` expiry or a transient "all peers
/// failed" while the session pool is still warming) is almost always a FALSE
/// negative — not proof the feed is empty. Interior probes can over-report
/// "absent" harmlessly (it just ends a search band), but a false-absent anchor
/// aborts the whole resolve with `NoUpdate`. So we retry the anchor with a
/// short backoff to let the daemon warm dialable peers, and only surface
/// `NoUpdate` after exhausting these attempts on a genuine, repeated miss.
const ANCHOR_ATTEMPTS: u32 = 4;
/// Backoff between anchor re-probes (gives the session pool time to dial more
/// `/ws` peers — scarce on mainnet, so the first probe of a cold session often
/// races a barely-warm pool).
const ANCHOR_RETRY_DELAY: Duration = Duration::from_millis(750);

/// Resolve the **latest** update of a sequence feed and return
/// `(content_reference, resolved_index)`.
///
/// `after` is a hint of the last known head index (0 if unknown). Because a
/// feed's head only moves forward, a good hint usually resolves the head in a
/// single fast round; the caller should persist the returned index and pass it
/// back next time (see [`crate::client::RetrievalCache`] feed-index cache).
///
/// Search strategy, by cost: a *present* probe answers fast (a close peer has
/// it); an *absent* probe is the expensive one (it waits `PROBE_TIMEOUT` for a
/// miss). So we minimize rounds that hinge on an absent probe:
///
/// 1. **Forward gallop from `after`** (steady-state fast path): probe a small
///    band `after+1, after+2, …` concurrently. If none are present, `after` is
///    still the head — done in one round of cheap present-or-fast-miss probes.
///    If some are present, gallop the band forward (doubling) until a round has
///    a missing index, which brackets the head.
/// 2. **Cold bracket** (no/!stale hint): concurrent exponential boundary search
///    from index 0 to bracket the head.
/// 3. **Concurrent k-ary narrowing** of the final bracket.
pub async fn resolve_latest<S>(
    store: &S,
    feed: &Feed,
    after: u64,
) -> Result<(ChunkAddress, u64), ResolveError>
where
    S: ChunkGet<DEFAULT_BODY_SIZE, Error = ChunkStoreError>,
{
    use futures::stream::{FuturesUnordered, StreamExt};

    tracing::info!(
        target: "hoverfly::feed",
        "resolving sequence feed: owner={} topic={} after={}",
        feed.owner,
        hex::encode(feed.topic),
        after
    );

    // Anchor the search. With a hint, confirm `after` is present and gallop
    // forward from it; otherwise confirm index 0 exists at all. A stale/too-high
    // hint falls back to a cold search from 0 (index 0 must exist for any feed).
    // `lo` = highest index proven present (+ its chunk); `hi` = lowest proven
    // absent (u64::MAX = not yet bracketed).
    //
    // The anchor probe is hardened against false-absents (timeout / transient
    // "all peers failed" on a cold session pool): unlike interior probes, an
    // anchor miss aborts the whole resolve, so we retry it before believing it.
    // See `confirm_anchor`.
    let (mut lo, mut last_chunk): (u64, AnyChunk<DEFAULT_BODY_SIZE>) =
        match confirm_anchor(store, feed, after).await? {
            Some(c) => (after, c),
            // Hint missed even after retries: fall back to a cold search from 0,
            // also hardened. If index 0 is genuinely absent, the feed is empty.
            None if after != 0 => match confirm_anchor(store, feed, 0).await? {
                Some(c) => (0, c),
                None => return Err(ResolveError::Feed(FeedError::NoUpdate)),
            },
            None => return Err(ResolveError::Feed(FeedError::NoUpdate)),
        };
    let mut hi: u64 = u64::MAX;

    // --- Forward boundary search: gallop doubling bands from `lo` until a band
    // contains a missing index (which sets `hi`). The very first band starting
    // just above a good hint usually finds the boundary immediately. ---
    let mut step = 1u64;
    'outer: while hi == u64::MAX {
        let mut futs = FuturesUnordered::new();
        let mut probed_top = lo;
        for k in 0..PROBE_BAND {
            // Probe lo+step, lo+2·step, lo+4·step, … within this band.
            let off = step.saturating_mul(1u64 << k);
            let idx = lo.saturating_add(off);
            if idx > MAX_PROBE_INDEX {
                break;
            }
            probed_top = probed_top.max(idx);
            futs.push(async move { (idx, get_update(store, feed, idx).await) });
        }
        if futs.is_empty() {
            break;
        }
        let mut any_present = false;
        while let Some((idx, res)) = futs.next().await {
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
        if hi != u64::MAX {
            break 'outer; // bracketed
        }
        if !any_present || probed_top >= MAX_PROBE_INDEX {
            break; // head is `lo` (no higher index exists)
        }
        // All present — advance: next band starts above what we probed, with a
        // larger step so a far-ahead head is reached in a few doubling rounds.
        step = (probed_top - lo).saturating_add(1);
    }

    // --- Concurrent k-ary narrowing of the bracket (lo present, hi absent). ---
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
            if round_low == low && round_high == high {
                break; // no progress guard
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
    // CAC body — exactly the feed payload (`span+timestamp+reference`).
    let reference = reference_from_payload(last_chunk.data().as_ref())?;
    tracing::info!(
        target: "hoverfly::feed",
        "resolved feed head: index={} -> content ref {}",
        lo,
        hex::encode(reference)
    );
    Ok((ChunkAddress::new(reference), lo))
}

/// Outcome of a single bounded probe, distinguishing *why* a chunk was absent.
/// Interior head-finding collapses all absences to "advance the search", but
/// the anchor needs to tell a definitive miss apart from a transient one.
enum Probe {
    /// The update chunk was retrieved.
    Present(AnyChunk<DEFAULT_BODY_SIZE>),
    /// Definitive network "not found": no peer holds this SOC address. For an
    /// anchor (index 0 or a previously-resolved hint) this is the only signal
    /// strong enough to conclude the feed is empty.
    Absent,
    /// Inconclusive: the probe hit `PROBE_TIMEOUT` or a transient retrieval
    /// failure ("all peers failed" / "no peer found") — likely a cold/warming
    /// session pool, not proof of absence. Retryable.
    Unresolved,
}

/// One bounded probe of update index `i`. Races retrieval against
/// `PROBE_TIMEOUT` so a miss fails fast (bee's asyncFinder bounds each probe to
/// 1s likewise). `tokio::time::timeout` is portable: native uses real tokio,
/// wasm uses tokio_with_wasm (gloo-timer backed).
async fn probe_update<S>(store: &S, feed: &Feed, i: u64) -> Result<Probe, ResolveError>
where
    S: ChunkGet<DEFAULT_BODY_SIZE, Error = ChunkStoreError>,
{
    let addr = ChunkAddress::new(feed.update_address(i));
    match tokio::time::timeout(PROBE_TIMEOUT, store.get(&addr)).await {
        Ok(Ok(c)) => Ok(Probe::Present(c)),
        Ok(Err(ChunkStoreError::NotFound { .. })) => Ok(Probe::Absent),
        // "no peer found" / "all peers failed" => nobody is serving this SOC
        // right now. For the search this means "advance", but it is NOT a
        // definitive not-found, so mark it Unresolved (retryable at the anchor).
        Ok(Err(ChunkStoreError::Other(msg))) if is_absent_retrieval(&msg) => Ok(Probe::Unresolved),
        Ok(Err(e)) => Err(ResolveError::Fetch(e.to_string())),
        // Timed out — inconclusive, retryable at the anchor.
        Err(_) => Ok(Probe::Unresolved),
    }
}

/// Fetch the feed update at index `i`. Returns `Ok(None)` if the update chunk
/// is absent (definitive not-found OR a bounded/transient miss — both end the
/// search band), `Ok(Some(chunk))` if present, and `Err` only on a genuine
/// fetch failure. Used for interior head-finding probes, where over-reporting
/// "absent" is harmless.
async fn get_update<S>(
    store: &S,
    feed: &Feed,
    i: u64,
) -> Result<Option<AnyChunk<DEFAULT_BODY_SIZE>>, ResolveError>
where
    S: ChunkGet<DEFAULT_BODY_SIZE, Error = ChunkStoreError>,
{
    match probe_update(store, feed, i).await? {
        Probe::Present(c) => Ok(Some(c)),
        Probe::Absent | Probe::Unresolved => Ok(None),
    }
}

/// Confirm whether an *anchor* index (a previously-resolved hint, or index 0)
/// is present, retrying through inconclusive misses.
///
/// Returns `Ok(Some(chunk))` if present, `Ok(None)` only after a **definitive**
/// not-found or after `ANCHOR_ATTEMPTS` exhausted on repeated inconclusive
/// misses. This is the fix for the rare "feed has no updates" flake: a single
/// timed-out or transiently-failed probe of a chunk that must exist no longer
/// aborts the whole resolve.
async fn confirm_anchor<S>(
    store: &S,
    feed: &Feed,
    i: u64,
) -> Result<Option<AnyChunk<DEFAULT_BODY_SIZE>>, ResolveError>
where
    S: ChunkGet<DEFAULT_BODY_SIZE, Error = ChunkStoreError>,
{
    for attempt in 0..ANCHOR_ATTEMPTS {
        match probe_update(store, feed, i).await? {
            Probe::Present(c) => return Ok(Some(c)),
            // A definitive not-found is trustworthy: stop immediately.
            Probe::Absent => return Ok(None),
            // Inconclusive (timeout / transient peer failure): back off and
            // retry — the session pool may still be warming dialable peers.
            Probe::Unresolved => {
                if attempt + 1 < ANCHOR_ATTEMPTS {
                    tracing::debug!(
                        target: "hoverfly::feed",
                        "anchor probe index={} inconclusive (attempt {}/{}); retrying",
                        i, attempt + 1, ANCHOR_ATTEMPTS
                    );
                    tokio::time::sleep(ANCHOR_RETRY_DELAY).await;
                }
            }
        }
    }
    Ok(None)
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

    // ---- resolve_latest anchor-resilience (the "feed has no updates" flake) ----

    use nectar_primitives::chunk::ContentChunk;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn test_feed() -> Feed {
        Feed::from_manifest_meta(
            "00112233445566778899aabbccddeeff00112233",
            &"22".repeat(32),
            "Sequence",
        )
        .unwrap()
    }

    /// A content chunk whose body is a valid feed payload pointing at `reference`
    /// (`span(8) || timestamp(8) || reference(32)`), used as a stand-in head SOC
    /// update — `resolve_latest` only reads `.data()` and parses the payload.
    fn head_chunk(reference: [u8; 32]) -> AnyChunk<DEFAULT_BODY_SIZE> {
        let mut body = vec![0u8; PAYLOAD_PREFIX];
        body.extend_from_slice(&reference);
        AnyChunk::from(ContentChunk::<DEFAULT_BODY_SIZE>::new(body).unwrap())
    }

    /// Per-address scripted outcome for one `get` call.
    enum Outcome {
        Present(AnyChunk<DEFAULT_BODY_SIZE>),
        NotFound,
        Transient, // "all peers failed" -> Probe::Unresolved (retryable)
    }

    /// Mock store: returns a scripted sequence of outcomes per address, so we can
    /// simulate transient anchor failures that resolve on retry. Thread-safe via
    /// a Mutex (ChunkGet requires Send + Sync).
    struct MockStore {
        scripts: Mutex<HashMap<[u8; 32], std::collections::VecDeque<Outcome>>>,
        present: Mutex<HashMap<[u8; 32], AnyChunk<DEFAULT_BODY_SIZE>>>,
    }

    impl ChunkGet<DEFAULT_BODY_SIZE> for MockStore {
        type Error = ChunkStoreError;
        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
            let key: [u8; 32] = <[u8; 32]>::try_from(address.as_ref())
                .expect("chunk address is 32 bytes");
            if let Some(q) = self.scripts.lock().unwrap().get_mut(&key) {
                if let Some(o) = q.pop_front() {
                    return match o {
                        Outcome::Present(c) => Ok(c),
                        Outcome::NotFound => Err(ChunkStoreError::not_found(address)),
                        Outcome::Transient => {
                            Err(ChunkStoreError::Other("all peers failed".into()))
                        }
                    };
                }
            }
            if let Some(c) = self.present.lock().unwrap().get(&key) {
                return Ok(c.clone());
            }
            Err(ChunkStoreError::not_found(address))
        }
    }

    /// A single transient (then-successful) probe of index 0 must NOT abort the
    /// resolve with `NoUpdate` — `confirm_anchor` retries through it. This is the
    /// regression guard for the rare `swarm.eth` "feed has no updates" flake.
    #[tokio::test]
    async fn anchor_retries_through_transient_then_resolves() {
        let feed = test_feed();
        let reference = [0x7Au8; 32];
        let a0 = feed.update_address(0);
        let a1 = feed.update_address(1);

        let mut scripts: HashMap<_, std::collections::VecDeque<Outcome>> = HashMap::new();
        // index 0: two transient failures, then present (head).
        scripts.insert(
            a0,
            [Outcome::Transient, Outcome::Transient, Outcome::Present(head_chunk(reference))]
                .into_iter()
                .collect(),
        );
        // index 1: genuinely absent -> head is 0.
        scripts.insert(a1, [Outcome::NotFound].into_iter().collect());

        let store = MockStore {
            scripts: Mutex::new(scripts),
            present: Mutex::new(HashMap::new()),
        };

        let (addr, index) = resolve_latest(&store, &feed, 0)
            .await
            .expect("must resolve through transient anchor misses, not error NoUpdate");
        assert_eq!(index, 0);
        assert_eq!(*addr.as_ref(), reference);
    }

    /// A definitive not-found at index 0 (no transient noise) still concludes the
    /// feed is empty immediately — we don't paper over genuinely empty feeds.
    #[tokio::test]
    async fn definitive_absent_anchor_reports_no_updates() {
        let feed = test_feed();
        let store = MockStore {
            scripts: Mutex::new(HashMap::new()), // all addresses -> NotFound
            present: Mutex::new(HashMap::new()),
        };
        let err = resolve_latest(&store, &feed, 0)
            .await
            .expect_err("empty feed must error");
        assert!(matches!(err, ResolveError::Feed(FeedError::NoUpdate)), "got {err:?}");
    }

    /// Exhausting all anchor attempts on persistent transient failure surfaces
    /// `NoUpdate` (bounded), rather than hanging forever.
    #[tokio::test]
    async fn anchor_gives_up_after_persistent_transient() {
        let feed = test_feed();
        let a0 = feed.update_address(0);
        let mut scripts: HashMap<_, std::collections::VecDeque<Outcome>> = HashMap::new();
        // Always transient at index 0, more than ANCHOR_ATTEMPTS times.
        scripts.insert(
            a0,
            std::iter::repeat_with(|| Outcome::Transient)
                .take(ANCHOR_ATTEMPTS as usize + 2)
                .collect(),
        );
        let store = MockStore {
            scripts: Mutex::new(scripts),
            present: Mutex::new(HashMap::new()),
        };
        let err = resolve_latest(&store, &feed, 0)
            .await
            .expect_err("persistent transient must bound out to NoUpdate");
        assert!(matches!(err, ResolveError::Feed(FeedError::NoUpdate)), "got {err:?}");
    }
}
