//! Swarm reference ↔ CIDv1 multibase encoding.
//!
//! bzz.limo (and any gateway that follows the `ENS contenthash` spec for
//! swarm) routes a content request by the multibase-encoded CID of the
//! root, not the raw 32-byte reference hex. The encoding mirrors what
//! omnipin's ENS helper produces:
//!
//! ```text
//! CID = varint(1) || varint(0xfa) || varint(0x1b) || varint(32) || ref32
//! cid_string = "b" + base32_lower_no_pad(CID)
//! ```
//!
//! - `0xfa` is the swarm manifest codec (`SWARM_MANIFEST_CODEC`).
//! - `0x1b` is the keccak-256 multihash code.
//! - Multibase prefix `b` indicates RFC 4648 lowercase base32, no padding.

/// Length-1 multicodec for "CID version 1".
const CID_V1: u64 = 1;
/// Multicodec for a swarm manifest root.
const SWARM_MANIFEST_CODEC: u64 = 0xfa;
/// Multihash code for keccak-256.
const KECCAK_256_MULTIHASH: u64 = 0x1b;

/// Encode a Swarm reference (32-byte content/manifest address) as a
/// CIDv1 multibase string suitable for `https://<cid>.bzz.limo/...`.
pub fn reference_to_cid(reference: &[u8; 32]) -> String {
    let mut bytes = Vec::with_capacity(8 + reference.len());
    push_varint(&mut bytes, CID_V1);
    push_varint(&mut bytes, SWARM_MANIFEST_CODEC);
    push_varint(&mut bytes, KECCAK_256_MULTIHASH);
    push_varint(&mut bytes, reference.len() as u64);
    bytes.extend_from_slice(reference);
    let mut out = String::with_capacity(1 + (bytes.len() * 8 + 4) / 5);
    out.push('b');
    encode_base32_lower(&bytes, &mut out);
    out
}

/// Minimal varint encoder (unsigned LEB128). `multiformats::varint` would
/// be the canonical dep, but a 10-line implementation is enough for
/// values up to a u64.
fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// RFC 4648 base32 lowercase, no padding. We avoid pulling in the
/// `base32` crate to keep deps minimal — the encoding is ~20 lines and
/// only used for the CID conversion path.
fn encode_base32_lower(input: &[u8], out: &mut String) {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for &byte in input {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1f) as usize;
            out.push(ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1f) as usize;
        out.push(ALPHABET[idx] as char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector_matches_omnipin_encoder() {
        // Reference: bah5acgza7uz732yevwl4jlqysqdx3j235zxwtpd4xlna5fodvswxjrw3542q
        // (computed earlier in this codebase by a hand-rolled python encoder
        // sharing the same formula).
        let mut r = [0u8; 32];
        let hex = "fd33fdeb04ad97c4ae1894077da75bee6f69bc7cbada0e95c3acad74c6dbef35";
        for i in 0..32 {
            r[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        let cid = reference_to_cid(&r);
        assert_eq!(
            cid,
            "bah5acgza7uz732yevwl4jlqysqdx3j235zxwtpd4xlna5fodvswxjrw3542q"
        );
    }
}
