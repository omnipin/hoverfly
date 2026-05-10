//! Minimal mantaray v0.1/v0.2 decoder + walker.
//!
//! Why not use `nectar-mantaray`? Its decoder rejects nodes with `ref_size = 0`
//! (intermediate trie nodes that carry no entry), which bee produces in the wild.
//! This walker accepts both `ref_size = 0` and `ref_size = 32` and follows the
//! fork structure as described in the bee spec and weeb-3's reference impl.

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
        return Err(ManifestError::TooShort { expected: NODE_HEADER_SIZE, actual: payload.len() });
    }

    // XOR-decrypt everything past the obfuscation key in-place on a local copy.
    let key: [u8; OBFUSCATION_KEY_SIZE] = payload[..OBFUSCATION_KEY_SIZE].try_into().unwrap();
    let mut data = payload.to_vec();
    for (i, byte) in data.iter_mut().enumerate().skip(OBFUSCATION_KEY_SIZE) {
        *byte ^= key[(i - OBFUSCATION_KEY_SIZE) % OBFUSCATION_KEY_SIZE];
    }

    let version_hash =
        &data[OBFUSCATION_KEY_SIZE..OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE];
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
    let end = b.iter().rposition(|&x| x != 0x0a).map(|i| i + 1).unwrap_or(0);
    &b[..end]
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
}
