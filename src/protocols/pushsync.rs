//! Bee pushsync protocol — `/swarm/pushsync/1.3.0/pushsync`.
//!
//! Client opens stream → sends `Headers { headers: [] }` → sends
//! `Delivery { address, data, stamp }` → reads `Headers` → reads `Receipt`.

use crate::proto::headers as hdr;
use crate::proto::pushsync as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/pushsync/1.3.1/pushsync";

#[derive(Debug, Error)]
pub enum PushsyncError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
    #[error("peer error: {0}")]
    Peer(String),
}

#[derive(Debug, Clone)]
pub struct PushsyncReceipt {
    pub address: Vec<u8>,
    pub signature: Vec<u8>,
    pub nonce: Vec<u8>,
    pub storage_radius: u32,
}

impl PushsyncReceipt {
    /// Recover the overlay address of the bee node that signed this
    /// receipt. The signature is over the chunk address (EIP-191
    /// prefixed, keccak256-hashed by alloy's recovery helper); the
    /// overlay is then `keccak(eth_addr || network_id_LE_8 || nonce)`.
    /// Returns `None` if signature recovery or layout checks fail.
    pub fn storer_overlay(&self, network_id: u64) -> Option<[u8; 32]> {
        use alloy_signer::Signature;
        if self.address.len() != 32 || self.signature.len() != 65 || self.nonce.len() != 32 {
            return None;
        }
        let sig = Signature::from_raw(&self.signature).ok()?;
        let eth = sig.recover_address_from_msg(&self.address).ok()?;
        let mut nonce = [0u8; 32];
        nonce.copy_from_slice(&self.nonce);
        Some(crate::signer::derive_overlay(&eth.0.0, network_id, &nonce))
    }

    /// Returns `true` when the receipt's signing peer was *not* in the
    /// chunk's storage neighborhood. Bee's check (mirrored from
    /// `pkg/pushsync/pushsync.go::checkReceipt`) compares
    /// `proximity(storer_overlay, chunk_addr)` against the receipt's
    /// claimed `storage_radius`. A shallow receipt means the chunk was
    /// only forwarded, not durably stored in any peer's reserve, and
    /// the upload should retry against a different peer.
    pub fn is_shallow(&self, network_id: u64) -> bool {
        let Some(overlay) = self.storer_overlay(network_id) else {
            return true;
        };
        if self.address.len() != 32 {
            return true;
        }
        let mut addr = [0u8; 32];
        addr.copy_from_slice(&self.address);
        let po = crate::transport::proximity(&overlay, &addr);
        u32::from(po) < self.storage_radius
    }
}

/// Push a single chunk and read the receipt.
///
/// `chunk_data` must already include the 8-byte LE span prefix (i.e. the
/// nectar `ContentChunk::data()` framing).
pub async fn push<S>(
    stream: &mut S,
    address: &[u8; 32],
    chunk_data: &[u8],
    stamp: &[u8],
) -> Result<PushsyncReceipt, PushsyncError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // 1. Send empty request headers.
    let req_headers = hdr::Headers { headers: vec![] };
    write_message(stream, &req_headers).await?;

    // 2. Read response headers (ignored).
    let _resp_headers: hdr::Headers = read_message(stream).await?;

    // 3. Send delivery.
    let delivery = pb::Delivery {
        address: address.to_vec(),
        data: chunk_data.to_vec(),
        stamp: stamp.to_vec(),
    };
    write_message(stream, &delivery).await?;

    // 4. Read receipt.
    let receipt: pb::Receipt = read_message(stream).await?;
    if !receipt.err.is_empty() {
        return Err(PushsyncError::Peer(receipt.err));
    }
    Ok(PushsyncReceipt {
        address: receipt.address,
        signature: receipt.signature,
        nonce: receipt.nonce,
        storage_radius: receipt.storage_radius,
    })
}
