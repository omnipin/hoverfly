//! Bee handshake signer.
//!
//! Wraps an alloy-local secp256k1 key and exposes:
//! - the 20-byte Ethereum address,
//! - the 32-byte Swarm overlay (`keccak256(eth_addr || network_id_LE_8 || nonce_32)`),
//! - a `sign_handshake` that signs `"bee-handshake-" || underlay || overlay || network_id_BE_8`
//!   with an EIP-191 prefix and produces a 65-byte (r || s || v) signature where v ∈ {27, 28}.

use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{sol, Eip712Domain};
use sha3::{Digest, Keccak256};
use thiserror::Error;

// EIP-712 cheque struct matching bee's `pkg/settlement/swap/chequebook/cheque.go::ChequeTypes`:
//   Cheque(address chequebook, address beneficiary, uint256 cumulativePayout)
// Field names and types must match byte-for-byte; the resulting type hash
// must equal what bee's `RecoverCheque` computes, otherwise signature
// recovery yields a different address and bee rejects the cheque as
// `ErrChequeInvalid`.
sol! {
    struct Cheque {
        address chequebook;
        address beneficiary;
        uint256 cumulativePayout;
    }
}

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

    /// Like [`Self::from_bytes`] but with an explicit caller-supplied
    /// nonce. The overlay is `keccak256(eth_addr || network_id || nonce)`,
    /// so a stable nonce across runs gives a stable overlay.
    ///
    /// **Why this matters for bee citizenship:** bee's kademlia drives
    /// long-term peer admission via hive gossip — when bee A learns about
    /// peer X from some other bee's hive broadcast, A adds X to its
    /// `knownPeers` and may later dial X outbound. That outbound dial
    /// admits X to kademlia with `forceConnection=true`, bypassing the
    /// bin-saturation check. But the whole mechanism is keyed on overlay
    /// stability: if X advertises a new overlay every restart, bees
    /// can never match a learned overlay to a dial-back attempt.
    /// `from_bytes` (above) randomises the nonce on every call, so two
    /// daemon restarts produce two different overlays — defeating
    /// kademlia memory. This constructor lets the daemon persist the
    /// nonce alongside the identity to keep the overlay stable.
    pub fn from_bytes_with_nonce(
        key: &[u8; 32],
        nonce: &[u8; 32],
        network_id: u64,
    ) -> Result<Self, SignerError> {
        let inner = PrivateKeySigner::from_slice(key)
            .map_err(|e| SignerError::Alloy(e.to_string()))?;
        let eth_address = inner.address().0.0;
        let overlay = derive_overlay(&eth_address, network_id, nonce);
        Ok(Self {
            inner,
            overlay,
            eth_address,
            nonce: *nonce,
            network_id,
        })
    }

    /// Like [`Self::from_hex`] but with an explicit hex-encoded nonce.
    /// See [`Self::from_bytes_with_nonce`] for the bee-citizenship
    /// rationale.
    pub fn from_hex_with_nonce(
        hex_key: &str,
        hex_nonce: &str,
        network_id: u64,
    ) -> Result<Self, SignerError> {
        let key_bytes = hex::decode(hex_key.trim_start_matches("0x"))?;
        if key_bytes.len() != 32 {
            return Err(SignerError::BadKeyLen(key_bytes.len()));
        }
        let nonce_bytes = hex::decode(hex_nonce.trim_start_matches("0x"))?;
        if nonce_bytes.len() != 32 {
            return Err(SignerError::BadKeyLen(nonce_bytes.len()));
        }
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 32];
        key.copy_from_slice(&key_bytes);
        nonce.copy_from_slice(&nonce_bytes);
        Self::from_bytes_with_nonce(&key, &nonce, network_id)
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

    /// Sign a SWAP cheque (EIP-712). Returns the 65-byte (r || s || v)
    /// signature bee's `RecoverCheque` (`chequestore.go:190`) recovers
    /// against the chequebook's on-chain `issuer()`. For the signature
    /// to validate, `self.eth_address()` must equal the chequebook's
    /// `issuer()` — i.e. the same key that signs our BzzAddress handshake
    /// must own/control the chequebook contract on chain.
    ///
    /// Domain: `{ Name: "Chequebook", Version: "1.0", ChainId: <chain_id> }`.
    /// Mainnet Swarm sits on Gnosis (chain_id = 100); testnet (Sepolia
    /// for Swarm) uses 11155111. The chain_id is part of the EIP-712
    /// domain separator and therefore part of the signed hash, so a
    /// cheque signed for one chain will not validate on another.
    pub fn sign_cheque(
        &self,
        chequebook: &[u8; 20],
        beneficiary: &[u8; 20],
        cumulative_payout: alloy_primitives::U256,
        chain_id: u64,
    ) -> Result<[u8; 65], SignerError> {
        let cheque = Cheque {
            chequebook: alloy_primitives::Address::from(*chequebook),
            beneficiary: alloy_primitives::Address::from(*beneficiary),
            cumulativePayout: cumulative_payout,
        };
        let domain = Eip712Domain {
            name: Some("Chequebook".into()),
            version: Some("1.0".into()),
            chain_id: Some(alloy_primitives::U256::from(chain_id)),
            verifying_contract: None,
            salt: None,
        };
        let sig = self
            .inner
            .sign_typed_data_sync(&cheque, &domain)
            .map_err(|e| SignerError::Alloy(e.to_string()))?;
        let mut bytes = sig.as_bytes();
        if bytes[64] < 27 {
            bytes[64] += 27;
        }
        Ok(bytes)
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
pub fn derive_overlay(eth_address: &[u8; 20], network_id: u64, nonce: &[u8; 32]) -> [u8; 32] {
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

/// Recover the Ethereum address of the signer of a bee BzzAddress.
///
/// Bee's `bzz/address.go::ParseAddress` does exactly this: it recovers
/// the secp256k1 public key from the EIP-191-prefixed `generate_sign_data`
/// payload + signature and converts it to a 20-byte Ethereum address
/// via `keccak256(pubkey)[12..]`. We need this for SWAP cheque issuance
/// because the cheque's `Beneficiary` field is the peer's Ethereum
/// address, and bee will only accept a cheque whose beneficiary matches
/// what it derived from our own BzzAddress signature — symmetrically,
/// we use *its* signature to derive the address we put in cheques we
/// send to it.
///
/// `signature` is the 65-byte `r || s || v` from `BzzAddress.signature`,
/// with `v` in Ethereum form (27 or 28).
pub fn recover_eth_address_from_handshake(
    underlay: &[u8],
    overlay: &[u8; 32],
    network_id: u64,
    signature: &[u8],
) -> Result<[u8; 20], SignerError> {
    use alloy_primitives::Signature;
    use k256::ecdsa::{RecoveryId, Signature as K256Sig, VerifyingKey};

    if signature.len() != 65 {
        return Err(SignerError::Alloy(format!(
            "bad signature length: {} (want 65)",
            signature.len()
        )));
    }
    let payload = generate_sign_data(underlay, overlay, network_id);

    // EIP-191 prefix hash: keccak256("\x19Ethereum Signed Message:\n{len}{payload}")
    let mut prefixed = Vec::with_capacity(46 + payload.len());
    prefixed.extend_from_slice(b"\x19Ethereum Signed Message:\n");
    prefixed.extend_from_slice(payload.len().to_string().as_bytes());
    prefixed.extend_from_slice(&payload);
    let digest: [u8; 32] = Keccak256::digest(&prefixed).into();

    // Normalize v: bee sends 27/28; k256 expects 0/1.
    let mut v = signature[64];
    if v >= 27 {
        v -= 27;
    }
    if v > 1 {
        return Err(SignerError::Alloy(format!("bad v byte: {}", signature[64])));
    }
    let _ = Signature::try_from(signature)
        .map_err(|e| SignerError::Alloy(format!("alloy sig parse: {e}")))?;

    let k_sig = K256Sig::from_slice(&signature[..64])
        .map_err(|e| SignerError::Alloy(format!("k256 sig: {e}")))?;
    let rec_id = RecoveryId::try_from(v)
        .map_err(|e| SignerError::Alloy(format!("k256 recovery id: {e}")))?;
    let vk = VerifyingKey::recover_from_prehash(&digest, &k_sig, rec_id)
        .map_err(|e| SignerError::Alloy(format!("k256 recover: {e}")))?;
    let point = vk.to_encoded_point(false); // uncompressed: 0x04 || X(32) || Y(32)
    let pub_bytes = &point.as_bytes()[1..]; // strip the 0x04 tag, 64 bytes
    let hash: [u8; 32] = Keccak256::digest(pub_bytes).into();
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&hash[12..]);
    Ok(addr)
}
