//! Bee postage stamp validator (signature-only).
//!
//! Bee chunks travel with a 113-byte attached stamp in the wire
//! format
//!
//!   `[batchID:32][index:8][timestamp:8][sig:65] = 113 bytes`
//!
//! (See `~/Coding/forks/bee/pkg/postage/stamp.go::MarshalBinary`.)
//!
//! The signature is EIP-191 personal-message signing over the prehash
//! `keccak256(chunkAddr[32] || batchID[32] || index[8] || timestamp[8])`
//! — i.e. `sign(keccak256("\x19Ethereum Signed Message:\n32" || prehash))`.
//! The signer is the **batch owner's Ethereum address** — the same
//! address that purchased the postage batch on-chain.
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

    // Prehash: keccak256(chunkAddr || batchID || index || timestamp),
    // exactly bee's `postage/stamp.go::toSignDigest`.
    let mut h = Keccak256::new();
    h.update(chunk_addr);
    h.update(batch_id);
    h.update(index);
    h.update(timestamp);
    let prehash: [u8; 32] = h.finalize().into();

    // The stamp signature is EIP-191 personal-message signing over that
    // prehash — nectar's issuer signs with `sign_message_sync(prehash)`
    // (see nectar-postage-issuer stamper.rs), and bee verifies the same
    // way (it accepts these stamps). So recover over the prefixed digest
    // `keccak256("\x19Ethereum Signed Message:\n32" || prehash)`, NOT the
    // raw prehash.
    let mut p = Keccak256::new();
    p.update(b"\x19Ethereum Signed Message:\n32");
    p.update(prehash);
    let digest: [u8; 32] = p.finalize().into();

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

/// secp256k1 ECDSA address recovery over an already-final 32-byte
/// digest. The caller is responsible for constructing the digest,
/// including the EIP-191 prefix wrap that stamp signing uses (see
/// [`validate`]).
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

    /// Round-trip: a stamp produced by the real signing path must
    /// recover to the signing key's address. Guards the EIP-191 wrap —
    /// nectar signs stamps with `sign_message_sync` (personal-message /
    /// EIP-191), so a raw-digest recovery yields a consistent WRONG
    /// address and every push is rejected as "not the batch owner".
    #[test]
    fn validate_recovers_real_stamp_signer() {
        use crate::signer::SwarmSigner;
        let key = "0x2cfe73bcd53cc2708a35f6f2238e2aeeb0448b65339f43d398e736102a211569";
        let signer = SwarmSigner::from_hex_with_nonce(
            key,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            1,
        )
        .unwrap();
        let batch = "0x2c18bcb885649cb468732c98d70d9cb0280aaffb30ffd0c882fccd8e22cd7408";
        let data = b"hoverfly pusher stamp round-trip payload".repeat(8);
        let (_root, work) =
            crate::client::prepare_upload_bytes(&signer, batch, 19, false, &data).unwrap();
        let vs = validate(&work[0].addr, &work[0].stamp).expect("stamp must validate");
        assert_eq!(
            vs.signer,
            *signer.eth_address(),
            "recovered stamp signer must equal the signing key address"
        );
    }
}
