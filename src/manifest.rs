//! Minimal mantaray v0.1/v0.2 decoder + walker.
//!
//! Why not use `nectar-mantaray`? The historical reason — its decoder rejecting
//! bee's `ref_size = 0` empty terminal nodes — was fixed upstream
//! (nxm-rs/nectar#35) and we now pin a rev that includes it. But two structural
//! gaps still keep this hand-rolled decoder in place:
//!
//! 1. **No async traversal.** nectar's only public manifest walk
//!    (`Manifest::walk` / `lookup` / `entries`) is bounded on `SyncChunkGet`.
//!    Our store is an async libp2p networked store that can't implement the
//!    synchronous trait, and on wasm there's no blocking adapter to bridge it.
//!    Tracked upstream at nxm-rs/nectar#37.
//! 2. **No public fork/metadata access.** `Node`'s `forks`, `metadata`, and
//!    `entry` fields are `pub(crate)`, so even decoding a node with nectar
//!    leaves us unable to drive our own parallel fork descent or read per-fork
//!    `Content-Type` / feed metadata through the public API.
//!
//! So this stays a self-contained decoder + walker: it parses both
//! `ref_size = 0` and `ref_size = 32` nodes and follows the fork structure per
//! the bee spec and weeb-3's reference impl. Once nectar#37 lands (async walk)
//! and the `Node` accessors are public, this can be retired in favour of
//! nectar's decoder — that's a separate change needing end-to-end manifest
//! verification. (Note: the *encoder* side already uses `nectar-mantaray`; see
//! `build_single_entry_manifest` / `build_collection_manifest`.)

use nectar_primitives::chunk::ChunkAddress;
use std::collections::BTreeMap;
use thiserror::Error;

const OBFUSCATION_KEY_SIZE: usize = 32;
const VERSION_HASH_SIZE: usize = 31;
const NODE_HEADER_SIZE: usize = OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE + 1; // 64
const FORK_PRE_REF_SIZE: usize = 32; // 1 (type) + 1 (prefix_len) + 30 (prefix)
const FORK_REF_SIZE: usize = 32;
const INDEX_SIZE: usize = 32;

const VERSION_HASH_V01: [u8; VERSION_HASH_SIZE] = [
    0x02, 0x51, 0x84, 0x78, 0x9d, 0x63, 0x63, 0x57, 0x66, 0xd7, 0x8c, 0x41, 0x90, 0x01, 0x96, 0xb5,
    0x7d, 0x74, 0x00, 0x87, 0x5e, 0xbe, 0x4d, 0x9b, 0x5d, 0x1e, 0x76, 0xbd, 0x96, 0x52, 0xa9,
];
const VERSION_HASH_V02: [u8; VERSION_HASH_SIZE] = [
    0x57, 0x68, 0xb3, 0xb6, 0xa7, 0xdb, 0x56, 0xd2, 0x1d, 0x1a, 0xbf, 0xf4, 0x0d, 0x41, 0xce, 0xbf,
    0xc8, 0x34, 0x48, 0xfe, 0xd8, 0xd7, 0xe9, 0xb0, 0x6e, 0xc0, 0xd3, 0xb0, 0x73, 0xf2, 0x8f,
];

const NT_METADATA: u8 = 16;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("data too short: need {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("unrecognized mantaray version hash")]
    InvalidVersion,
    #[error("unsupported ref_size: {0}")]
    UnsupportedRefSize(u8),
    #[error("invalid prefix length: {0}")]
    InvalidPrefixLength(u8),
    #[error("path not found: {0}")]
    PathNotFound(String),
    #[error("invalid metadata JSON: {0}")]
    Metadata(String),
}

#[derive(Debug, Clone)]
pub struct Node {
    pub entry: Option<ChunkAddress>,
    pub forks: BTreeMap<u8, Fork>,
}

#[derive(Debug, Clone)]
pub struct Fork {
    pub node_type: u8,
    pub prefix: Vec<u8>,
    pub reference: ChunkAddress,
    pub metadata: BTreeMap<String, String>,
}

/// Decode a mantaray node from the chunk's payload bytes (no span prefix).
pub fn decode_node(payload: &[u8]) -> Result<Node, ManifestError> {
    if payload.len() < NODE_HEADER_SIZE {
        return Err(ManifestError::TooShort {
            expected: NODE_HEADER_SIZE,
            actual: payload.len(),
        });
    }

    // XOR-decrypt everything past the obfuscation key in-place on a local copy.
    let key: [u8; OBFUSCATION_KEY_SIZE] = payload[..OBFUSCATION_KEY_SIZE].try_into().unwrap();
    let mut data = payload.to_vec();
    for (i, byte) in data.iter_mut().enumerate().skip(OBFUSCATION_KEY_SIZE) {
        *byte ^= key[(i - OBFUSCATION_KEY_SIZE) % OBFUSCATION_KEY_SIZE];
    }

    let version_hash = &data[OBFUSCATION_KEY_SIZE..OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE];
    if version_hash != VERSION_HASH_V01 && version_hash != VERSION_HASH_V02 {
        return Err(ManifestError::InvalidVersion);
    }

    let ref_size = data[OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE];
    if ref_size != 0 && ref_size as usize != FORK_REF_SIZE {
        return Err(ManifestError::UnsupportedRefSize(ref_size));
    }

    let mut offset = NODE_HEADER_SIZE;

    let entry = if ref_size > 0 {
        let entry_bytes = &data[offset..offset + ref_size as usize];
        offset += ref_size as usize;
        if entry_bytes.iter().all(|&b| b == 0) {
            None
        } else {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&entry_bytes[..32]);
            Some(ChunkAddress::new(arr))
        }
    } else {
        None
    };

    if data.len() < offset + INDEX_SIZE {
        return Err(ManifestError::TooShort {
            expected: offset + INDEX_SIZE,
            actual: data.len(),
        });
    }
    let mut bitfield = [0u8; INDEX_SIZE];
    bitfield.copy_from_slice(&data[offset..offset + INDEX_SIZE]);
    offset += INDEX_SIZE;

    let mut forks = BTreeMap::new();
    for b in 0..=u8::MAX {
        if bitfield[(b as usize) / 8] & (1 << ((b as usize) % 8)) == 0 {
            continue;
        }
        if data.len() < offset + FORK_PRE_REF_SIZE + FORK_REF_SIZE {
            return Err(ManifestError::TooShort {
                expected: offset + FORK_PRE_REF_SIZE + FORK_REF_SIZE,
                actual: data.len(),
            });
        }
        let node_type = data[offset];
        let prefix_len = data[offset + 1];
        if prefix_len == 0 || prefix_len > 30 {
            return Err(ManifestError::InvalidPrefixLength(prefix_len));
        }
        let prefix = data[offset + 2..offset + 2 + prefix_len as usize].to_vec();
        offset += FORK_PRE_REF_SIZE;

        let mut ref_arr = [0u8; 32];
        ref_arr.copy_from_slice(&data[offset..offset + FORK_REF_SIZE]);
        let reference = ChunkAddress::new(ref_arr);
        offset += FORK_REF_SIZE;

        let metadata = if node_type & NT_METADATA != 0 {
            if data.len() < offset + 2 {
                return Err(ManifestError::TooShort {
                    expected: offset + 2,
                    actual: data.len(),
                });
            }
            let mlen = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;
            if data.len() < offset + mlen {
                return Err(ManifestError::TooShort {
                    expected: offset + mlen,
                    actual: data.len(),
                });
            }
            let json = &data[offset..offset + mlen];
            offset += mlen;
            // Trim trailing 0x0a padding before JSON decoding.
            let trimmed = trim_padding(json);
            let map: BTreeMap<String, String> = serde_json::from_slice(trimmed)
                .map_err(|e| ManifestError::Metadata(e.to_string()))?;
            map
        } else {
            BTreeMap::new()
        };

        forks.insert(
            b,
            Fork {
                node_type,
                prefix,
                reference,
                metadata,
            },
        );
    }

    Ok(Node { entry, forks })
}

fn trim_padding(b: &[u8]) -> &[u8] {
    let end = b
        .iter()
        .rposition(|&x| x != 0x0a)
        .map(|i| i + 1)
        .unwrap_or(0);
    &b[..end]
}

/// Bee mantaray metadata keys (matching `pkg/manifest/manifest.go`).
pub const ENTRY_METADATA_CONTENT_TYPE_KEY: &str = "Content-Type";
pub const ENTRY_METADATA_FILENAME_KEY: &str = "Filename";
pub const WEBSITE_INDEX_DOCUMENT_SUFFIX_KEY: &str = "website-index-document";
pub const WEBSITE_ERROR_DOCUMENT_PATH_KEY: &str = "website-error-document";
/// Bee's manifest root-path marker for website metadata.
pub const ROOT_PATH: &str = "/";

/// Feed-manifest metadata keys (bee `pkg/api/feed.go`). A feed manifest is a
/// normal mantaray manifest whose root (`/`) entry carries these, encoding the
/// feed to resolve instead of pointing directly at content.
pub const FEED_OWNER_KEY: &str = "swarm-feed-owner";
pub const FEED_TOPIC_KEY: &str = "swarm-feed-topic";
pub const FEED_TYPE_KEY: &str = "swarm-feed-type";

/// If `node` is a feed manifest, return its `(owner_hex, topic_hex, type)`
/// metadata. Bee writes these on the root-path (`/`) fork's metadata; some
/// encoders place them on the node's own metadata — check both. Returns `None`
/// for an ordinary content manifest.
pub fn extract_feed_meta(node: &Node) -> Option<(String, String, String)> {
    // Bee stores feed metadata on the root-path (`/`) fork's metadata map.
    for fork in node.forks.values() {
        let m = &fork.metadata;
        if let (Some(owner), Some(topic)) = (m.get(FEED_OWNER_KEY), m.get(FEED_TOPIC_KEY)) {
            // type defaults to "sequence" if omitted (bee only writes sequence).
            let ty = m
                .get(FEED_TYPE_KEY)
                .cloned()
                .unwrap_or_else(|| "sequence".to_string());
            return Some((owner.clone(), topic.clone(), ty));
        }
    }
    None
}

/// One file entry to include in a collection manifest.
pub struct CollectionEntry {
    /// In-manifest path (e.g. `"index.html"`, `"static/app.css"`). Used as
    /// both the trie key and as the `Filename` metadata value.
    pub path: String,
    /// Reference to the file's content (its BMT root).
    pub reference: ChunkAddress,
    /// Optional `Content-Type`; set this to whatever the client should
    /// receive when fetching this entry.
    pub content_type: Option<String>,
}

/// Build a single-entry mantaray manifest with `path` -> `file_root` and the
/// given `Content-Type`. Returns `(manifest_root, chunks)` where each chunk's
/// payload is ready to be wrapped in `span_LE_8 || payload` and pushed via
/// pushsync. Uses nectar-mantaray's encoder (its decoder rejects bee's
/// ref_size=0 nodes, but its encoder is fine).
pub fn build_single_entry_manifest(
    path: &str,
    file_root: ChunkAddress,
    content_type: Option<&str>,
) -> Result<(ChunkAddress, Vec<(ChunkAddress, bytes::Bytes)>), ManifestError> {
    let entries = vec![CollectionEntry {
        path: path.to_string(),
        reference: file_root,
        content_type: content_type.map(str::to_string),
    }];
    build_collection_manifest(&entries, None, None)
}

/// Build a multi-entry "collection" mantaray manifest as bee produces when
/// you POST a tar to `/bzz` with `Content-Type: application/x-tar`. Each
/// entry becomes a fork in the trie at its `path`, carrying `Content-Type`
/// and `Filename` metadata. Optional `index_document` / `error_document`
/// are written as root-path metadata so that gateways resolve `/<root>/`
/// to the index and 404s to the error doc.
///
/// Returns `(manifest_root, chunks_in_wire_form)`.
pub fn build_collection_manifest(
    entries: &[CollectionEntry],
    index_document: Option<&str>,
    error_document: Option<&str>,
) -> Result<(ChunkAddress, Vec<(ChunkAddress, bytes::Bytes)>), ManifestError> {
    use nectar_mantaray::PlainManifest;
    use nectar_primitives::DEFAULT_BODY_SIZE;
    use nectar_primitives::DefaultMemoryStore;

    let store = DefaultMemoryStore::new();
    let mut manifest: PlainManifest<_, DEFAULT_BODY_SIZE> = PlainManifest::new(store);

    for entry in entries {
        let mut metadata: BTreeMap<String, String> = BTreeMap::new();
        if let Some(ct) = &entry.content_type {
            metadata.insert(ENTRY_METADATA_CONTENT_TYPE_KEY.to_string(), ct.clone());
        }
        // Filename is the basename of the path (matching bee's behaviour).
        let filename = entry
            .path
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(entry.path.as_str())
            .to_string();
        metadata.insert(ENTRY_METADATA_FILENAME_KEY.to_string(), filename);

        manifest
            .add_with_metadata(&entry.path, entry.reference, metadata)
            .map_err(|e| ManifestError::Metadata(e.to_string()))?;
    }

    if index_document.is_some() || error_document.is_some() {
        let mut root_meta: BTreeMap<String, String> = BTreeMap::new();
        if let Some(idx) = index_document {
            root_meta.insert(
                WEBSITE_INDEX_DOCUMENT_SUFFIX_KEY.to_string(),
                idx.to_string(),
            );
        }
        if let Some(err) = error_document {
            root_meta.insert(WEBSITE_ERROR_DOCUMENT_PATH_KEY.to_string(), err.to_string());
        }
        // bee uses swarm.ZeroAddress for the root entry's reference.
        manifest
            .add_with_metadata(ROOT_PATH, ChunkAddress::new([0u8; 32]), root_meta)
            .map_err(|e| ManifestError::Metadata(e.to_string()))?;
    }

    let root = manifest
        .save()
        .map_err(|e| ManifestError::Metadata(e.to_string()))?;

    let (_node, store) = manifest.into_parts();
    let chunks: Vec<(ChunkAddress, bytes::Bytes)> = store
        .into_chunks()
        .into_iter()
        .map(|(addr, chunk)| (addr, chunk.into_bytes()))
        .collect();

    Ok((root, chunks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_empty_intermediate_node_with_ref_size_zero() {
        // 96 bytes: obfuscation_key(32) || version_hash(31) || ref_size=0(1) || index(32)
        let mut data = vec![0u8; 96];
        data[32..63].copy_from_slice(&VERSION_HASH_V02);
        // ref_size = 0
        // index = all zeros
        let node = decode_node(&data).unwrap();
        assert!(node.entry.is_none());
        assert!(node.forks.is_empty());
    }

    /// Real `swarm.eth` feed-manifest root chunk (ref
    /// 03b80b08…, fetched from mainnet). Its `/` fork metadata carries the
    /// feed params; `extract_feed_meta` must find them. Guards both the
    /// mantaray decoder (fork metadata) and the feed-key extraction against
    /// real-world data, not just synthetic fixtures.
    #[test]
    fn extract_feed_meta_from_swarm_eth_manifest() {
        use base64::Engine;
        // Base64 of the 384-byte mantaray node (CAC data, span already stripped).
        const B64: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABXaLO2p9tW0h0av/QNQc6/yDRI/tjX6bBuwNOwc/KPIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAASAS8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIUE8qEHypQL6vxM4vbJqfCWjGKltYk/8OTh4pgwSNJ2AL57InN3YXJtLWZlZWQtb3duZXIiOiJmNzdhMTNkYzJmNzg2YjQ1MjNmNWUwYmY2ZGI2NzU3ZjRjZjYwZWJiIiwic3dhcm0tZmVlZC10b3BpYyI6ImUwZDdkZWY1MDc0ZGI5ZDk4YzBhNWVkNmYzOTFhMjIwNTY4NDM2NzdlZWFkZTkwNzE4MDAyZjY5NmQ2MzZhZjEiLCJzd2FybS1mZWVkLXR5cGUiOiJTZXF1ZW5jZSJ9CgoKCgoKCgoKCgoK";
        let data = base64::engine::general_purpose::STANDARD
            .decode(B64)
            .unwrap();
        let node = decode_node(&data).expect("decode swarm.eth feed manifest node");
        let (owner, topic, ty) =
            extract_feed_meta(&node).expect("swarm.eth root must be detected as a feed manifest");
        assert_eq!(owner, "f77a13dc2f786b4523f5e0bf6db6757f4cf60ebb");
        assert_eq!(
            topic,
            "e0d7def5074db9d98c0a5ed6f391a22056843677eeade90718002f696d636af1"
        );
        assert_eq!(ty, "Sequence");
    }
}
