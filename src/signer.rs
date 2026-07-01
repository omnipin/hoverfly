//! Bee handshake signer.
//!
//! Wraps an alloy-local secp256k1 key and exposes:
//! - the 20-byte Ethereum address,
//! - the 32-byte Swarm overlay (`keccak256(eth_addr || network_id_LE_8 || nonce_32)`),
//! - a `sign_handshake` that signs `"bee-handshake-" || underlay || overlay || network_id_BE_8`
//!   with an EIP-191 prefix and produces a 65-byte (r || s || v) signature where v ∈ {27, 28}.

use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{Eip712Domain, sol};
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
    /// Cache of `(underlay, chequebook) -> (timestamp, signature)` for
    /// v15 handshakes. Bee 2.8.0 rejects gossip records whose
    /// timestamp advances by less than `MinimumUpdateInterval = 300 s`
    /// since the existing record (`ErrTimestampTooSoon` in
    /// `pkg/bzz/timestamp.go`). When our daemon's pool churns and
    /// re-handshakes the same peer with a fresh timestamp every time,
    /// the peer's hive gossip about us produces a flood of
    /// too-soon records that get silently dropped by other bees —
    /// our overlay's kademlia membership ages out across the network
    /// and bins re-saturate against us.
    ///
    /// Caching the `(timestamp, signature)` pair per (underlay,
    /// chequebook) gives us byte-identical re-presentation on every
    /// reconnect: bee's handshake check `newTimestamp < existing`
    /// accepts equal timestamps, and gossip never sees a "newer"
    /// record because we never produce one. Cache lives for the
    /// daemon's lifetime; restart clears it, which is fine because
    /// the next handshake produces `now_unix() > any past timestamp`.
    handshake_cache_v15: std::sync::Arc<std::sync::Mutex<HandshakeCacheV15>>,
}

#[derive(Debug, Default)]
struct HandshakeCacheV15 {
    entries: std::collections::HashMap<(Vec<u8>, [u8; 20]), (i64, [u8; 65])>,
}

impl SwarmSigner {
    pub fn from_bytes(key: &[u8; 32], network_id: u64) -> Result<Self, SignerError> {
        let inner =
            PrivateKeySigner::from_slice(key).map_err(|e| SignerError::Alloy(e.to_string()))?;
        let eth_address = inner.address().0.0;
        let nonce = random_nonce();
        let overlay = derive_overlay(&eth_address, network_id, &nonce);
        Ok(Self {
            inner,
            overlay,
            eth_address,
            nonce,
            network_id,
            handshake_cache_v15: std::sync::Arc::new(std::sync::Mutex::new(
                HandshakeCacheV15::default(),
            )),
        })
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
        let inner =
            PrivateKeySigner::from_slice(key).map_err(|e| SignerError::Alloy(e.to_string()))?;
        let eth_address = inner.address().0.0;
        let overlay = derive_overlay(&eth_address, network_id, nonce);
        Ok(Self {
            inner,
            overlay,
            eth_address,
            nonce: *nonce,
            network_id,
            handshake_cache_v15: std::sync::Arc::new(std::sync::Mutex::new(
                HandshakeCacheV15::default(),
            )),
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
        Self {
            inner,
            overlay,
            eth_address,
            nonce,
            network_id,
            handshake_cache_v15: std::sync::Arc::new(std::sync::Mutex::new(
                HandshakeCacheV15::default(),
            )),
        }
    }

    pub const fn overlay(&self) -> &[u8; 32] {
        &self.overlay
    }
    pub const fn eth_address(&self) -> &[u8; 20] {
        &self.eth_address
    }
    pub const fn nonce(&self) -> &[u8; 32] {
        &self.nonce
    }
    pub const fn network_id(&self) -> u64 {
        self.network_id
    }
    pub const fn alloy_signer(&self) -> &PrivateKeySigner {
        &self.inner
    }

    /// Sign bee handshake payload (v14 / pre-bee-2.8.0 format).
    /// Payload: `b"bee-handshake-" || underlay || overlay || network_id_BE_8`.
    /// Returns 65-byte (r || s || v) signature with v ∈ {27, 28}.
    pub fn sign_handshake(&self, underlay: &[u8]) -> Result<[u8; 65], SignerError> {
        let payload = generate_sign_data(underlay, &self.overlay, self.network_id);
        self.sign_eip191(&payload)
    }

    /// Sign bee handshake payload (v15 / bee 2.8.0+ format).
    /// Payload:
    ///   `b"bee-handshake-" || underlay || overlay || network_id_BE_8
    ///     || nonce || timestamp_BE_8 || chequebook_address_20`
    ///
    /// `timestamp` is the seconds-since-epoch the peer will validate
    /// against `time.Now() ± MaxClockSkew` (bee uses 60 s). Pass a
    /// reasonably-fresh value (i.e. `now_unix()`) per handshake;
    /// the same record can be re-presented later but each reconnect
    /// should advance it.
    ///
    /// `chequebook_address` is the 20-byte address of our chequebook
    /// contract. We run no chequebook, so this is always the zero
    /// address `[0; 20]`. Bee only enforces a non-zero chequebook when
    /// the peer has `--chequebook-verification` enabled AND we advertise
    /// `full_node = true` (which we do). No mainnet peer enables that
    /// flag by default, so the zero address is accepted in practice.
    pub fn sign_handshake_v15(
        &self,
        underlay: &[u8],
        nonce: &[u8; 32],
        timestamp: i64,
        chequebook_address: &[u8; 20],
    ) -> Result<[u8; 65], SignerError> {
        let payload = generate_sign_data_v15(
            underlay,
            &self.overlay,
            self.network_id,
            nonce,
            timestamp,
            chequebook_address,
        );
        self.sign_eip191(&payload)
    }

    /// Like [`Self::sign_handshake_v15`] but returns a cached
    /// `(timestamp, signature)` pair on repeat calls with the same
    /// `(underlay, chequebook_address)`. Uses `self.nonce()` as the
    /// signing nonce (the only correct value for our overlay).
    ///
    /// First call for a given `(underlay, chequebook_address)`:
    /// generates `now_unix()` as timestamp, signs, caches, returns.
    /// Subsequent calls: return the cached pair byte-for-byte.
    ///
    /// Why this matters for bee 2.8.0: when our session pool churns
    /// and we re-handshake the same peer, bee's hive will gossip our
    /// new record to other bees. Bee 2.8.0's
    /// `bzz.CheckTimestamp(source=Gossip)` rejects gossip records
    /// whose timestamp is less than `existing.Timestamp + 300 s` —
    /// our reconnect every minute or two produces `ErrTimestampTooSoon`
    /// errors at every gossip recipient, so other bees stop learning
    /// about us and our kademlia membership across the network ages
    /// out. By replaying the same `(timestamp, signature)` on every
    /// reconnect, we present a single stable record per peer for the
    /// lifetime of the daemon — gossip recipients see no update
    /// after the first one and our presence stays cached.
    ///
    /// Daemon restarts clear the cache (it lives in memory). The next
    /// run's timestamps will be strictly newer than the previous run's,
    /// so handshake `newTimestamp < existing` passes; and the new
    /// gossip records advance the existing-record timestamp by more
    /// than 300 s (the daemon has been down for at least that long
    /// in normal operation).
    pub fn sign_handshake_v15_cached(
        &self,
        underlay: &[u8],
        chequebook_address: &[u8; 20],
    ) -> Result<(i64, [u8; 65]), SignerError> {
        let key = (underlay.to_vec(), *chequebook_address);
        {
            let cache = self
                .handshake_cache_v15
                .lock()
                .expect("handshake cache mutex poisoned");
            if let Some(&(ts, sig)) = cache.entries.get(&key) {
                return Ok((ts, sig));
            }
        }
        // Cache miss: sign fresh. Generate `now_unix()` outside the
        // mutex to keep lock hold time short; if a concurrent caller
        // beats us into the cache, we accept their entry (they'll
        // already have populated it with a different timestamp, but
        // the signature is valid against that timestamp so functionally
        // equivalent).
        let timestamp = crate::peers::now_unix() as i64;
        let signature =
            self.sign_handshake_v15(underlay, &self.nonce, timestamp, chequebook_address)?;
        let mut cache = self
            .handshake_cache_v15
            .lock()
            .expect("handshake cache mutex poisoned");
        let entry = cache.entries.entry(key).or_insert((timestamp, signature));
        Ok((entry.0, entry.1))
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

/// v14 / pre-bee-2.8.0 handshake sign payload:
/// `b"bee-handshake-" || underlay || overlay || network_id_BE_8`
pub fn generate_sign_data(underlay: &[u8], overlay: &[u8; 32], network_id: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(14 + underlay.len() + 32 + 8);
    data.extend_from_slice(b"bee-handshake-");
    data.extend_from_slice(underlay);
    data.extend_from_slice(overlay);
    data.extend_from_slice(&network_id.to_be_bytes());
    data
}

/// v15 / bee-2.8.0+ handshake sign payload:
/// `b"bee-handshake-" || underlay || overlay || network_id_BE_8
///   || nonce_32 || timestamp_BE_8 || chequebook_address_20`
///
/// Mirrors bee's `pkg/bzz/address.go::generateSignData` byte-for-byte.
/// `timestamp` is treated as `int64` and serialized as big-endian
/// `uint64` (bee does `binary.BigEndian.PutUint64(buf, uint64(timestamp))`,
/// so negative values would wrap; we reject them at the higher
/// level via `ErrTimestampInvalid` semantics).
pub fn generate_sign_data_v15(
    underlay: &[u8],
    overlay: &[u8; 32],
    network_id: u64,
    nonce: &[u8; 32],
    timestamp: i64,
    chequebook_address: &[u8; 20],
) -> Vec<u8> {
    let mut data = Vec::with_capacity(14 + underlay.len() + 32 + 8 + 32 + 8 + 20);
    data.extend_from_slice(b"bee-handshake-");
    data.extend_from_slice(underlay);
    data.extend_from_slice(overlay);
    data.extend_from_slice(&network_id.to_be_bytes());
    data.extend_from_slice(nonce);
    data.extend_from_slice(&(timestamp as u64).to_be_bytes());
    data.extend_from_slice(chequebook_address);
    data
}

/// Recover the Ethereum address of the signer of a bee v14 BzzAddress.
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
    let payload = generate_sign_data(underlay, overlay, network_id);
    recover_eth_from_eip191(&payload, signature)
}

/// Recover the Ethereum address of the signer of a bee v15 BzzAddress
/// (bee 2.8.0+). Mirrors v14's [`recover_eth_address_from_handshake`]
/// but uses the v15 sign payload that includes nonce, timestamp, and
/// chequebook address.
pub fn recover_eth_address_from_handshake_v15(
    underlay: &[u8],
    overlay: &[u8; 32],
    network_id: u64,
    nonce: &[u8; 32],
    timestamp: i64,
    chequebook_address: &[u8; 20],
    signature: &[u8],
) -> Result<[u8; 20], SignerError> {
    let payload = generate_sign_data_v15(
        underlay,
        overlay,
        network_id,
        nonce,
        timestamp,
        chequebook_address,
    );
    recover_eth_from_eip191(&payload, signature)
}

/// Recover the 20-byte Ethereum address from a 65-byte (r || s || v)
/// EIP-191 signature over `payload`. `v` is expected in Ethereum form
/// (27 or 28); k256 normalises to 0/1 internally.
fn recover_eth_from_eip191(payload: &[u8], signature: &[u8]) -> Result<[u8; 20], SignerError> {
    use alloy_primitives::Signature;
    use k256::ecdsa::{RecoveryId, Signature as K256Sig, VerifyingKey};

    if signature.len() != 65 {
        return Err(SignerError::Alloy(format!(
            "bad signature length: {} (want 65)",
            signature.len()
        )));
    }

    // EIP-191 prefix hash: keccak256("\x19Ethereum Signed Message:\n{len}{payload}")
    let mut prefixed = Vec::with_capacity(46 + payload.len());
    prefixed.extend_from_slice(b"\x19Ethereum Signed Message:\n");
    prefixed.extend_from_slice(payload.len().to_string().as_bytes());
    prefixed.extend_from_slice(payload);
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
