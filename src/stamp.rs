//! Bee postage stamp validator (signature-only).
//!
//! Bee chunks travel with a 113-byte attached stamp in the wire
//! format
//!
//!   `[batchID:32][index:8][timestamp:8][sig:65] = 113 bytes`
//!
//! (See `~/Coding/forks/bee/pkg/postage/stamp.go::MarshalBinary`.)
//!
//! The signature is over
//! `keccak256(chunkAddr[32] || batchID[32] || index[8] || timestamp[8])`,
//! using a standard secp256k1 EIP-191-like ecrecover. The signer is
//! the **batch owner's Ethereum address** — the same address that
//! purchased the postage batch on-chain.
//!
//! ## What this validator does
//!
//! Verify the stamp's signature recovers to *some* valid 20-byte
//! Ethereum address. Returns `Err` on:
//! - wrong length (not 113 bytes)
//! - malformed signature (bad recovery byte, point-at-infinity, etc.)
//! - signer recovers to the zero address (signature-by-zero-key fraud)
//!
//! ## What this validator does NOT do
//!
//! It does NOT verify the recovered address actually owns the batch
//! on-chain. That would require an RPC call to bee's
//! `postageContract.batches(batchID)` getter. Per the AGENTS.md "no
//! on-chain RPC" rule, we deliberately don't make that call. So a
//! signature signed by ANY valid key passes this check, even if the
//! signer doesn't own the claimed batch.
//!
//! For an upload-only client this is acceptable: we don't ingest
//! chunks into a long-term reserve where bad stamps would matter.
//! hoverfly doesn't accept pullsync (we don't store chunks), so the
//! missing batch-owner check has no operational consequence here.

use sha3::{Digest, Keccak256};
use thiserror::Error;

/// Size of a serialized bee postage stamp.
pub const STAMP_SIZE: usize = 32 + 8 + 8 + 65;

#[derive(Debug, Error)]
pub enum StampError {
    #[error("expected {STAMP_SIZE}-byte stamp, got {0}")]
    BadLength(usize),
    #[error("malformed signature: {0}")]
    BadSignature(String),
    #[error("recovered signer is zero address (forged stamp)")]
    ZeroSigner,
}

/// Validated stamp fields, useful for callers that want to inspect
/// the recovered signer (e.g. to bucket chunks by batch owner).
#[derive(Debug, Clone)]
pub struct ValidStamp<'a> {
    pub batch_id: &'a [u8],
    pub index: &'a [u8],
    pub timestamp: &'a [u8],
    pub signature: &'a [u8],
    /// Recovered batch owner's 20-byte Ethereum address.
    pub signer: [u8; 20],
}

/// Validate a postage stamp's signature against a chunk address.
///
/// Returns the recovered batch-owner Ethereum address on success.
/// See module docs for what this does and doesn't verify.
pub fn validate<'a>(chunk_addr: &[u8; 32], stamp: &'a [u8]) -> Result<ValidStamp<'a>, StampError> {
    if stamp.len() != STAMP_SIZE {
        return Err(StampError::BadLength(stamp.len()));
    }
    let batch_id = &stamp[0..32];
    let index = &stamp[32..40];
    let timestamp = &stamp[40..48];
    let signature = &stamp[48..113];

    // Build the signed digest exactly as bee does in
    // `postage/stamp.go::ToSignDigest`: keccak256 of the
    // concatenation, no EIP-191 prefix.
    let mut h = Keccak256::new();
    h.update(chunk_addr);
    h.update(batch_id);
    h.update(index);
    h.update(timestamp);
    let digest: [u8; 32] = h.finalize().into();

    // Recover the signer. Bee's `crypto.Recover` expects the
    // signature in r||s||v form with `v ∈ {0,1}`. We accept the
    // 27/28 Ethereum form too and normalize.
    let signer = recover_secp256k1_address(&digest, signature)?;
    if signer == [0u8; 20] {
        return Err(StampError::ZeroSigner);
    }

    Ok(ValidStamp {
        batch_id,
        index,
        timestamp,
        signature,
        signer,
    })
}

/// secp256k1 ECDSA address recovery. Mirrors what
/// `signer.rs::recover_eth_address_from_handshake` does internally,
/// but operates on a raw 32-byte digest rather than an EIP-191
/// prefixed payload (since bee's stamp signer does NOT use the
/// EIP-191 prefix — see `postage/stamp.go::ToSignDigest`).
fn recover_secp256k1_address(digest: &[u8; 32], signature: &[u8]) -> Result<[u8; 20], StampError> {
    use k256::ecdsa::{RecoveryId, Signature as K256Sig, VerifyingKey};

    if signature.len() != 65 {
        return Err(StampError::BadSignature(format!(
            "expected 65-byte sig, got {}",
            signature.len()
        )));
    }
    let mut v = signature[64];
    if v >= 27 {
        v -= 27;
    }
    if v > 1 {
        return Err(StampError::BadSignature(format!(
            "bad v byte: {}",
            signature[64]
        )));
    }
    let k_sig = K256Sig::from_slice(&signature[..64])
        .map_err(|e| StampError::BadSignature(format!("k256 sig parse: {e}")))?;
    let rec_id = RecoveryId::try_from(v)
        .map_err(|e| StampError::BadSignature(format!("k256 recovery id: {e}")))?;
    let vk = VerifyingKey::recover_from_prehash(digest, &k_sig, rec_id)
        .map_err(|e| StampError::BadSignature(format!("k256 recover: {e}")))?;
    let point = vk.to_encoded_point(false);
    let pub_bytes = &point.as_bytes()[1..];
    let hash: [u8; 32] = Keccak256::digest(pub_bytes).into();
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&hash[12..]);
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_length_rejected() {
        let chunk = [0u8; 32];
        assert!(matches!(
            validate(&chunk, &[]),
            Err(StampError::BadLength(0))
        ));
        assert!(matches!(
            validate(&chunk, &[0u8; 100]),
            Err(StampError::BadLength(100))
        ));
    }

    #[test]
    fn bad_signature_rejected() {
        let chunk = [0u8; 32];
        let mut stamp = [0u8; STAMP_SIZE];
        // All-zero signature recovers to zero address or fails parse.
        stamp[112] = 27;
        // Either ZeroSigner or BadSignature is acceptable.
        let res = validate(&chunk, &stamp);
        assert!(res.is_err(), "all-zero stamp must not validate");
    }
}
