//! Bee hive protocol — `/swarm/hive/1.1.0/peers`.
//!
//! Server-push only: after handshake the peer announces its known peers via
//! repeated `Peers` messages. Each `BzzAddress.underlay` may contain either a
//! single multiaddr or a list of multiaddrs prefixed with `0x99`.
//! We drain messages from the stream until it ends.

use crate::peers::Peer;
use crate::proto::headers as hdr;
use crate::proto::hive as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use libp2p::Multiaddr;
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/hive/1.1.0/peers";

const UNDERLAY_LIST_PREFIX: u8 = 0x99;

#[derive(Debug, Error)]
pub enum HiveError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
}

/// Drain Peers envelopes from the hive stream until EOF, returning all
/// announced peers.
pub async fn read_peers<S>(stream: &mut S) -> Result<Vec<Peer>, HiveError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // Bee opens the hive stream as listener: it expects us (dialer) to send empty
    // request headers first, then it replies with empty response headers and
    // streams `Peers` envelopes.
    let _: hdr::Headers = read_message(stream).await?;
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;

    let mut out = Vec::new();
    loop {
        match read_message::<_, pb::Peers>(stream).await {
            Ok(env) => {
                for entry in env.peers {
                    if entry.overlay.len() != 32 {
                        continue;
                    }
                    let underlays: Vec<String> = match deserialize_underlays(&entry.underlay) {
                        Ok(addrs) => addrs.iter().map(Multiaddr::to_string).collect(),
                        Err(_) => continue,
                    };
                    if underlays.is_empty() {
                        continue;
                    }
                    out.push(Peer {
                        overlay: hex::encode(&entry.overlay),
                        underlays,
                        eth_address: None,
                        nonce: if entry.nonce.is_empty() {
                            None
                        } else {
                            Some(hex::encode(&entry.nonce))
                        },
                        ..Default::default()
                    });
                }
            }
            Err(FrameError::Io(_)) => break, // stream closed
            Err(e) => return Err(e.into()),
        }
    }
    Ok(out)
}

/// A BzzAddress encoded for the wire (overlay + underlay + signature
/// + nonce). Built once per session by the caller from the
/// `HandshakeResult` of our outbound bee handshake (where signature
/// and nonce are produced) plus our advertised underlay.
#[derive(Clone, Debug)]
pub struct OwnBzzAddress {
    pub overlay: Vec<u8>,
    pub underlay: Vec<u8>,
    pub signature: Vec<u8>,
    pub nonce: Vec<u8>,
}

/// Open the hive stream to bee as DIALER and broadcast ourselves.
/// Bee's `peersHandler` reads one `Peers` envelope, verifies each
/// announced address by dial-probing it, and adds successfully
/// verified entries to its `knownPeers`. From `knownPeers` the
/// manage loop may later dial us OUTBOUND, which admits us to
/// kademlia with `forceConnection=true` — bypassing the
/// bin-saturation check that rejects normal inbound clients.
///
/// This is the key bee-citizenship lever: a stable overlay (see
/// `SwarmSigner::from_bytes_with_nonce`) plus active hive
/// announcement causes the network to *remember* us, so bees
/// progressively dial us in over time and we accumulate kademlia
/// membership beyond what a single-session client could ever build.
///
/// Bee's libp2p framework runs `sendHeaders` automatically on the
/// dialer side: we write empty request headers, read empty response
/// headers, then write the actual `Peers` message. Mirrors what
/// `pkg/p2p/libp2p/headers.go::sendHeaders` does internally.
pub async fn announce_self<S>(stream: &mut S, me: &OwnBzzAddress) -> Result<(), HiveError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // Framework preamble: empty headers exchange.
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let _: hdr::Headers = read_message(stream).await?;

    let envelope = pb::Peers {
        peers: vec![pb::BzzAddress {
            overlay: me.overlay.clone(),
            underlay: me.underlay.clone(),
            signature: me.signature.clone(),
            nonce: me.nonce.clone(),
        }],
    };
    write_message(stream, &envelope).await?;
    Ok(())
}

/// Server-side hive: bee opens an inbound `peers` stream to us
/// expecting an empty `Peers` envelope (or our list of known peers).
/// We respond with an empty list — bee's hive lets us indicate "I
/// don't know any peers right now" without erroring out, which is
/// fine for an edge daemon that doesn't maintain a kademlia table.
pub async fn respond_empty<S>(stream: &mut S) -> Result<(), HiveError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // bee writes empty headers first → we ack, then send a single
    // empty Peers envelope.
    let _: hdr::Headers = read_message(stream).await?;
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    write_message(stream, &pb::Peers { peers: vec![] }).await?;
    Ok(())
}

fn deserialize_underlays(data: &[u8]) -> Result<Vec<Multiaddr>, ()> {
    if data.is_empty() {
        return Err(());
    }
    if data[0] == UNDERLAY_LIST_PREFIX {
        return deserialize_list(&data[1..]);
    }
    Multiaddr::try_from(data.to_vec()).map(|m| vec![m]).map_err(|_| ())
}

fn deserialize_list(data: &[u8]) -> Result<Vec<Multiaddr>, ()> {
    let mut addrs = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let (len, n) = read_varint(&data[pos..])?;
        pos += n;
        if pos + len > data.len() {
            return Err(());
        }
        if let Ok(ma) = Multiaddr::try_from(data[pos..pos + len].to_vec()) {
            addrs.push(ma);
        }
        pos += len;
    }
    Ok(addrs)
}

fn read_varint(data: &[u8]) -> Result<(usize, usize), ()> {
    let mut value: usize = 0;
    let mut shift = 0;
    for (i, &b) in data.iter().enumerate() {
        value |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(());
        }
    }
    Err(())
}
