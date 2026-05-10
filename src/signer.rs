//! Bee handshake signer.
//!
//! Wraps an alloy-local secp256k1 key and exposes:
//! - the 20-byte Ethereum address,
//! - the 32-byte Swarm overlay (`keccak256(eth_addr || network_id_LE_8 || nonce_32)`),
//! - a `sign_handshake` that signs `"bee-handshake-" || underlay || overlay || network_id_BE_8`
//!   with an EIP-191 prefix and produces a 65-byte (r || s || v) signature where v ∈ {27, 28}.

use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use sha3::{Digest, Keccak256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SignerError {
    #[error("invalid private key length: expected 32 bytes, got {0}")]
    BadKeyLen(usize),
    #[error("hex decode: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("alloy signer: {0}")]
    Alloy(String),
}

#[derive(Clone, Debug)]
pub struct SwarmSigner {
    inner: PrivateKeySigner,
    overlay: [u8; 32],
    eth_address: [u8; 20],
    nonce: [u8; 32],
    network_id: u64,
}

impl SwarmSigner {
    pub fn from_bytes(key: &[u8; 32], network_id: u64) -> Result<Self, SignerError> {
        let inner = PrivateKeySigner::from_slice(key).map_err(|e| SignerError::Alloy(e.to_string()))?;
        let eth_address = inner.address().0.0;
        let nonce = random_nonce();
        let overlay = derive_overlay(&eth_address, network_id, &nonce);
        Ok(Self { inner, overlay, eth_address, nonce, network_id })
    }

    pub fn from_hex(hex_key: &str, network_id: u64) -> Result<Self, SignerError> {
        let bytes = hex::decode(hex_key.trim_start_matches("0x"))?;
        if bytes.len() != 32 {
            return Err(SignerError::BadKeyLen(bytes.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Self::from_bytes(&arr, network_id)
    }

    pub fn random(network_id: u64) -> Self {
        let inner = PrivateKeySigner::random();
        let eth_address = inner.address().0.0;
        let nonce = random_nonce();
        let overlay = derive_overlay(&eth_address, network_id, &nonce);
        Self { inner, overlay, eth_address, nonce, network_id }
    }

    pub const fn overlay(&self) -> &[u8; 32] { &self.overlay }
    pub const fn eth_address(&self) -> &[u8; 20] { &self.eth_address }
    pub const fn nonce(&self) -> &[u8; 32] { &self.nonce }
    pub const fn network_id(&self) -> u64 { self.network_id }
    pub const fn alloy_signer(&self) -> &PrivateKeySigner { &self.inner }

    /// Sign bee handshake payload. Returns 65-byte (r || s || v) signature with v ∈ {27, 28}.
    pub fn sign_handshake(&self, underlay: &[u8]) -> Result<[u8; 65], SignerError> {
        let payload = generate_sign_data(underlay, &self.overlay, self.network_id);
        self.sign_eip191(&payload)
    }

    /// Sign arbitrary bytes with the EIP-191 prefix `\x19Ethereum Signed Message:\n{len}{data}`.
    pub fn sign_eip191(&self, data: &[u8]) -> Result<[u8; 65], SignerError> {
        let sig = self
            .inner
            .sign_message_sync(data)
            .map_err(|e| SignerError::Alloy(e.to_string()))?;
        let mut bytes = sig.as_bytes();
        // Ensure v is in Ethereum form 27/28 (alloy's as_bytes may return parity 0/1).
        if bytes[64] < 27 {
            bytes[64] += 27;
        }
        Ok(bytes)
    }
}

fn random_nonce() -> [u8; 32] {
    let mut buf = [0u8; 32];
    getrandom::fill(&mut buf).expect("os rng");
    buf
}

/// `keccak256(eth_addr || network_id_LE_8 || nonce_32)`
fn derive_overlay(eth_address: &[u8; 20], network_id: u64, nonce: &[u8; 32]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(eth_address);
    h.update(network_id.to_le_bytes());
    h.update(nonce);
    h.finalize().into()
}

/// `b"bee-handshake-" || underlay || overlay || network_id_BE_8`
pub fn generate_sign_data(underlay: &[u8], overlay: &[u8; 32], network_id: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(14 + underlay.len() + 32 + 8);
    data.extend_from_slice(b"bee-handshake-");
    data.extend_from_slice(underlay);
    data.extend_from_slice(overlay);
    data.extend_from_slice(&network_id.to_be_bytes());
    data
}
