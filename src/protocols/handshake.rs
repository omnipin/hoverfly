//! Bee handshake protocol — `/swarm/handshake/13.0.0/handshake`.
//!
//! Wire flow (client -> server):
//!   1. Send `Syn { observed_underlay }`.
//!   2. Receive `SynAck { syn, ack }`.
//!   3. Send `Ack { address: BzzAddress, network_id, full_node, nonce }`.
//!
//! We don't fully verify the peer's BzzAddress signature in this micro-client;
//! we just persist the overlay it advertised. (Hive verification is the
//! security-sensitive path; handshake simply opens the channel.)

use crate::proto::handshake as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use crate::signer::SwarmSigner;
use libp2p::{Multiaddr, PeerId};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/handshake/14.0.0/handshake";

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
}

#[derive(Debug, Clone)]
pub struct HandshakeResult {
    pub peer_overlay: [u8; 32],
    pub peer_underlay: Vec<u8>,
    pub peer_eth_signature: Vec<u8>,
    pub peer_full_node: bool,
    pub peer_nonce: Vec<u8>,
}

/// Run the bee handshake on an open libp2p stream.
///
/// `observed_underlay` is the multiaddr we observed for the remote peer
/// (just the underlay portion, e.g. `/ip4/x.x.x.x/tcp/443/wss/p2p/<id>`).
/// We pass the full multiaddr serialized.
pub async fn run<S>(
    stream: &mut S,
    signer: &SwarmSigner,
    network_id: u64,
    observed_underlay: &Multiaddr,
    our_peer_id: &PeerId,
    our_full_node: bool,
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
    // Bee expects OUR listen underlay, not the peer's. Clients don't listen,
    // so we synthesize a stable loopback underlay; bee verifies the signature
    // over those bytes and doesn't care that the host is unreachable.
    let _ = observed_underlay;
    let our_underlay = client_loopback_underlay(our_peer_id);
    let our_signature = signer.sign_handshake(&our_underlay)?;
    let our_addr = pb::BzzAddress {
        underlay: our_underlay,
        signature: our_signature.to_vec(),
        overlay: signer.overlay().to_vec(),
    };
    let ack = pb::Ack {
        address: Some(our_addr),
        network_id,
        full_node: our_full_node,
        nonce: signer.nonce().to_vec(),
        welcome_message: String::new(),
    };
    write_message(stream, &ack).await?;

    let peer_overlay = {
        if their_addr.overlay.len() != 32 {
            return Err(HandshakeError::MissingAddress);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&their_addr.overlay);
        arr
    };

    Ok(HandshakeResult {
        peer_overlay,
        peer_underlay: their_addr.underlay,
        peer_eth_signature: their_addr.signature,
        peer_full_node: their_ack.full_node,
        peer_nonce: their_ack.nonce,
    })
}

fn client_loopback_underlay(peer_id: &PeerId) -> Vec<u8> {
    let s = format!("/ip4/127.0.0.1/tcp/1634/p2p/{peer_id}");
    let ma: Multiaddr = s.parse().expect("static loopback multiaddr is valid");
    ma.to_vec()
}
