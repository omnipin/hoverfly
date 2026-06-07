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
//! 2. The update at sequence index `i` lives at a SOC address derived as:
//!      `id   = keccak256(topic || u64_be(i))`
//!      `addr = keccak256(id || owner)`              (== SOC `CreateAddress`)
//! 3. To find the latest update we probe indices upward from 0 until a fetch
//!    misses; the last index that resolved is the current head. (Bee uses a
//!    concurrent doubling search; we use a bounded binary/exponential search
//!    that keeps the per-chunk fetch count low.)
//! 4. The found chunk is a SOC. Its wrapped CAC body is the feed *payload*,
//!    laid out (legacy/v1) as `span(8) || timestamp(8) || reference(32[|64])`.
//!    The `reference` after the 16-byte prefix is the content manifest root.
//!
//! Feed parameters come from a **feed manifest**: a normal mantaray manifest
//! whose root (`/`) entry carries metadata keys `swarm-feed-owner`,
//! `swarm-feed-topic`, `swarm-feed-type` (see [`crate::manifest`]). ENS Swarm
//! contenthashes for mutable sites resolve to such a manifest.

use alloy_primitives::{Address, Keccak256};

/// Metadata keys bee writes into a feed manifest's root entry
/// (`pkg/api/feed.go`).
pub const FEED_OWNER_KEY: &str = "swarm-feed-owner";
pub const FEED_TOPIC_KEY: &str = "swarm-feed-topic";
pub const FEED_TYPE_KEY: &str = "swarm-feed-type";

/// Sequence-feed index width (`uint64` big-endian), per bee `sequence.index`.
const INDEX_BYTES: usize = 8;

/// Legacy/v1 feed payload prefix: `span(8) || timestamp(8)` precede the
/// wrapped content reference. See bee `feeds.legacyPayload` (`cacData[16:]`).
const PAYLOAD_PREFIX: usize = 16;

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
                write!(f, "unsupported feed type '{s}' (only Sequence is supported)")
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

    /// Cross-check the SOC address derivation `keccak256(id || owner)` against
    /// bee's `TestCreateAddress` vector (pkg/soc/soc_test.go): id = 32 zero
    /// bytes, owner = 8d3766…e632 -> 9d453ebb…6d61dc85. This guards the inner
    /// half of `update_address` (the id→address step) against any keccak
    /// ordering/encoding drift vs. the network.
    #[test]
    fn soc_create_address_matches_bee_vector() {
        let id = [0u8; 32];
        let owner = Address::from_slice(
            &decode_hex("8d3766440f0d7b949a5e32995d09619a7f86e632").unwrap(),
        );
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
