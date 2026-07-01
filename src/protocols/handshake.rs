//! Bee handshake protocol.
//!
//! Two on-wire versions are supported:
//!
//!   * v14 — `/swarm/handshake/14.0.0/handshake`, used by bee ≤ 2.7.x.
//!     `BzzAddress = { underlay, signature, overlay }`; signature is
//!     over `"bee-handshake-" || underlay || overlay || network_id_BE_8`.
//!     `Ack.nonce` carries the overlay nonce out-of-band.
//!
//!   * v15 — `/swarm/handshake/15.0.0/handshake`, introduced in bee 2.8.0
//!     (network-wide upgrade; old nodes no longer interoperate with new
//!     ones). `BzzAddress` gains `nonce`, `timestamp`, and
//!     `chequebook_address` fields, all of which are now part of the
//!     signed payload. `Ack.nonce` becomes unused (the nonce inside the
//!     embedded BzzAddress is canonical).
//!
//! Wire flow (client -> server) is identical between versions:
//!   1. Send `Syn { observed_underlay }`.
//!   2. Receive `SynAck { syn, ack }`.
//!   3. Send `Ack { address: BzzAddress, network_id, full_node, ... }`.
//!
//! `full_node` is a parameter of [`run`]; the value hoverfly actually
//! advertises is chosen by the caller (`transport.rs` passes
//! `full_node = true` — a deliberate throughput/accounting tactic, see
//! that call site's comment). We run no on-chain chequebook, so we
//! always send the zero `chequebook_address` in v15 to signal "no
//! chequebook"; that's safe under either `full_node` value because bee's
//! chequebook-verification gate only fires when the *peer* runs
//! `--chequebook-verification` AND we advertise `full_node = true` — and
//! no mainnet peer enables that flag by default.

use crate::proto::handshake as pb;
use crate::protocols::framing::{FrameError, read_message, write_message};
use crate::signer::SwarmSigner;
use libp2p::{Multiaddr, PeerId};
use thiserror::Error;

/// Protocol identifier for the bee 2.8.0+ handshake. The micro-client
/// upgrades to this when both sides agree at the libp2p multistream
/// level; otherwise it falls back to v14.
pub const PROTOCOL_V15: &str = "/swarm/handshake/15.0.0/handshake";

/// Protocol identifier for the bee 2.7.x and older handshake.
/// Kept for legacy peers that haven't upgraded yet.
pub const PROTOCOL_V14: &str = "/swarm/handshake/14.0.0/handshake";

/// Default-current protocol. Existing callers that don't yet thread the
/// version through use this; once they do, prefer the explicit
/// [`PROTOCOL_V14`] or [`PROTOCOL_V15`] consts.
pub const PROTOCOL: &str = PROTOCOL_V15;

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
    #[error("signer: {0}")]
    Signer(#[from] crate::signer::SignerError),
    #[error("missing ack from server")]
    MissingAck,
    #[error("missing address in ack")]
    MissingAddress,
    #[error("network id mismatch: ours={ours}, theirs={theirs}")]
    NetworkIdMismatch { ours: u64, theirs: u64 },
    #[error("v15 timestamp out of range or invalid: {0}")]
    TimestampInvalid(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V14,
    V15,
}

impl Version {
    /// Map a negotiated multistream protocol id back to a version
    /// enum. Returns `None` for unknown ids.
    pub fn from_protocol(proto: &str) -> Option<Self> {
        match proto {
            PROTOCOL_V14 => Some(Self::V14),
            PROTOCOL_V15 => Some(Self::V15),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HandshakeResult {
    pub peer_overlay: [u8; 32],
    pub peer_underlay: Vec<u8>,
    pub peer_eth_signature: Vec<u8>,
    pub peer_full_node: bool,
    pub peer_nonce: Vec<u8>,
    /// Bee v15 timestamp from the peer's BzzAddress. Always 0 for v14
    /// peers (the field doesn't exist in v14).
    pub peer_timestamp: i64,
    /// Bee v15 chequebook address from the peer's BzzAddress. 20-byte
    /// zero address means "no chequebook" or v14 peer.
    pub peer_chequebook: [u8; 20],
    /// Recovered 20-byte Ethereum address of the peer (their cheque
    /// beneficiary). `None` if recovery failed (malformed signature);
    /// SWAP cheque issuance is skipped in that case.
    pub peer_eth_address: Option<[u8; 20]>,
    /// The multiaddr bytes WE sent as our own underlay in the Ack.
    /// Mirrors what bee stored in its addressbook for us. Needed for
    /// outbound hive announcement so we send a self-BzzAddress
    /// whose signature matches bee's verification expectation. See
    /// `protocols::hive::announce_self`.
    pub our_underlay: Vec<u8>,
    /// 65-byte EIP-191 signature over our handshake payload. Stored
    /// alongside `our_underlay` so the caller can rebuild the same
    /// `BzzAddress { underlay, signature, overlay, [nonce, timestamp,
    /// chequebook]}` we sent in the Ack, without recomputing.
    pub our_signature: Vec<u8>,
    /// v15 timestamp we put in our outgoing BzzAddress. 0 when we
    /// negotiated v14. Stored so the caller can replay the exact same
    /// signed record over hive without resigning.
    pub our_timestamp: i64,
    /// Protocol version actually negotiated. Tells the caller whether
    /// the v15-specific fields in this struct were sent on the wire.
    pub version: Version,
}

/// Run the bee handshake on an open libp2p stream.
///
/// `version` is the negotiated multistream protocol version. Callers
/// pick this based on the substream id their `open_stream` resolved
/// to; if you only opened by id with no fallback path, just pass
/// [`Version::V15`] and let v14 peers reject upstream at the
/// multistream layer.
///
/// `observed_underlay` is the multiaddr we observed for the remote peer
/// (just the underlay portion, e.g. `/ip4/x.x.x.x/tcp/443/wss/p2p/<id>`).
///
/// `advertised_underlay` is the multiaddr we tell the bee peer we listen
/// on. Pass `Some(addr)` for daemon-serving mode (bee will add us to its
/// kademlia table and route retrieval lookups back to us); pass `None`
/// for ephemeral clients, in which case we advertise a synthetic
/// 127.0.0.1 loopback that bee accepts but can't dial.
pub async fn run<S>(
    stream: &mut S,
    signer: &SwarmSigner,
    network_id: u64,
    observed_underlay: &Multiaddr,
    advertised_underlay: Option<&Multiaddr>,
    our_peer_id: &PeerId,
    our_full_node: bool,
    version: Version,
) -> Result<HandshakeResult, HandshakeError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // 1. Send Syn.
    let syn = pb::Syn {
        observed_underlay: observed_underlay.to_vec(),
    };
    write_message(stream, &syn).await?;

    // 2. Receive SynAck.
    let synack: pb::SynAck = read_message(stream).await?;
    let their_ack = synack.ack.ok_or(HandshakeError::MissingAck)?;
    let their_addr = their_ack.address.ok_or(HandshakeError::MissingAddress)?;

    if their_ack.network_id != network_id {
        return Err(HandshakeError::NetworkIdMismatch {
            ours: network_id,
            theirs: their_ack.network_id,
        });
    }

    // 3. Send Ack with our BzzAddress.
    let our_underlay = match advertised_underlay {
        Some(ma) => ma.to_vec(),
        None => client_loopback_underlay(our_peer_id),
    };

    // We don't run a chequebook contract on chain, so we advertise the
    // zero address. Bee's chequebook verifier only fires when our peer
    // enables --chequebook-verification AND we advertise full_node=true.
    // We DO advertise full_node=true (see transport.rs call site), but no
    // mainnet peer enables --chequebook-verification by default, so the
    // gate stays dormant and the zero chequebook is accepted.
    let our_chequebook = [0u8; 20];

    let (our_signature, our_timestamp, our_addr) = match version {
        Version::V14 => {
            let sig = signer.sign_handshake(&our_underlay)?.to_vec();
            (
                sig.clone(),
                0i64,
                pb::BzzAddress {
                    underlay: our_underlay.clone(),
                    signature: sig,
                    overlay: signer.overlay().to_vec(),
                    nonce: Vec::new(),
                    timestamp: 0,
                    chequebook_address: Vec::new(),
                },
            )
        }
        Version::V15 => {
            // Cached `(timestamp, signature)` per `(our_underlay,
            // our_chequebook)`. First call signs fresh; subsequent calls
            // replay the same record. See
            // `SwarmSigner::sign_handshake_v15_cached` for why bee 2.8.0
            // requires this — repeatedly bumping the timestamp on every
            // reconnect ages our overlay out of other bees' addressbooks
            // via the `MinimumUpdateInterval` gossip-reject path.
            let (timestamp, sig_bytes) =
                signer.sign_handshake_v15_cached(&our_underlay, &our_chequebook)?;
            if timestamp <= 0 {
                return Err(HandshakeError::TimestampInvalid(timestamp));
            }
            let sig = sig_bytes.to_vec();
            (
                sig.clone(),
                timestamp,
                pb::BzzAddress {
                    underlay: our_underlay.clone(),
                    signature: sig,
                    overlay: signer.overlay().to_vec(),
                    nonce: signer.nonce().to_vec(),
                    timestamp,
                    chequebook_address: our_chequebook.to_vec(),
                },
            )
        }
    };

    let ack = pb::Ack {
        address: Some(our_addr),
        network_id,
        full_node: our_full_node,
        // v14 ack-level nonce. v15 ignores this field (the canonical
        // nonce lives inside BzzAddress), but proto3 forces us to fill
        // it. Sending the real nonce is harmless either way.
        nonce: signer.nonce().to_vec(),
        welcome_message: String::new(),
    };
    write_message(stream, &ack).await?;

    if their_addr.overlay.len() != 32 {
        return Err(HandshakeError::MissingAddress);
    }
    let mut peer_overlay = [0u8; 32];
    peer_overlay.copy_from_slice(&their_addr.overlay);

    // Per-version peer-record interpretation.
    // v14: nonce comes from Ack.nonce; timestamp + chequebook absent.
    // v15: nonce/timestamp/chequebook all inside BzzAddress and
    //      covered by the signature.
    let (peer_nonce_vec, peer_timestamp, peer_chequebook) = match version {
        Version::V14 => (their_ack.nonce.clone(), 0i64, [0u8; 20]),
        Version::V15 => {
            let mut cb = [0u8; 20];
            if their_addr.chequebook_address.len() == 20 {
                cb.copy_from_slice(&their_addr.chequebook_address);
            }
            (their_addr.nonce.clone(), their_addr.timestamp, cb)
        }
    };

    // Recover the peer's Ethereum address from the BzzAddress
    // signature so we can use it as the cheque beneficiary later.
    // Best-effort: a malformed signature shouldn't fail the handshake
    // (the peer is otherwise usable for pseudosettle-only).
    let peer_eth_address = match version {
        Version::V14 => crate::signer::recover_eth_address_from_handshake(
            &their_addr.underlay,
            &peer_overlay,
            network_id,
            &their_addr.signature,
        )
        .ok(),
        Version::V15 => {
            let mut nonce32 = [0u8; 32];
            if peer_nonce_vec.len() == 32 {
                nonce32.copy_from_slice(&peer_nonce_vec);
            }
            crate::signer::recover_eth_address_from_handshake_v15(
                &their_addr.underlay,
                &peer_overlay,
                network_id,
                &nonce32,
                peer_timestamp,
                &peer_chequebook,
                &their_addr.signature,
            )
            .ok()
        }
    };

    Ok(HandshakeResult {
        peer_overlay,
        peer_underlay: their_addr.underlay,
        peer_eth_signature: their_addr.signature,
        peer_full_node: their_ack.full_node,
        peer_nonce: peer_nonce_vec,
        peer_timestamp,
        peer_chequebook,
        peer_eth_address,
        our_underlay,
        our_signature,
        our_timestamp,
        version,
    })
}

fn client_loopback_underlay(peer_id: &PeerId) -> Vec<u8> {
    let s = format!("/ip4/127.0.0.1/tcp/1634/p2p/{peer_id}");
    let ma: Multiaddr = s.parse().expect("static loopback multiaddr is valid");
    ma.to_vec()
}
