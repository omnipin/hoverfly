//! Bee pushsync protocol — `/swarm/pushsync/1.3.0/pushsync`.
//!
//! Client opens stream → sends `Headers { headers: [] }` → sends
//! `Delivery { address, data, stamp }` → reads `Headers` → reads `Receipt`.

use crate::proto::headers as hdr;
use crate::proto::pushsync as pb;
use crate::protocols::framing::{FrameError, read_message, write_message};
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
    // Per-phase timing emitted at the end as a single tracing line on
    // the `isheika::profile` target. Run with `RUST_LOG=isheika::profile=trace`
    // and pipe through `awk` to get a histogram of where push time goes.
    let t_start = web_time::Instant::now();

    // 1. Send empty request headers.
    let req_headers = hdr::Headers { headers: vec![] };
    write_message(stream, &req_headers).await?;
    let t_hdr_sent = web_time::Instant::now();

    // 2. Read response headers (ignored).
    let _resp_headers: hdr::Headers = read_message(stream).await?;
    let t_hdr_recv = web_time::Instant::now();

    // 3. Send delivery.
    let delivery = pb::Delivery {
        address: address.to_vec(),
        data: chunk_data.to_vec(),
        stamp: stamp.to_vec(),
    };
    write_message(stream, &delivery).await?;
    let t_delivery_sent = web_time::Instant::now();

    // 4. Read receipt.
    let receipt: pb::Receipt = read_message(stream).await?;
    let t_receipt_recv = web_time::Instant::now();

    tracing::trace!(
        target: "isheika::profile",
        addr = %hex::encode(address),
        hdr_send_us = (t_hdr_sent - t_start).as_micros() as u64,
        hdr_recv_us = (t_hdr_recv - t_hdr_sent).as_micros() as u64,
        delivery_send_us = (t_delivery_sent - t_hdr_recv).as_micros() as u64,
        receipt_recv_us = (t_receipt_recv - t_delivery_sent).as_micros() as u64,
        total_us = (t_receipt_recv - t_start).as_micros() as u64,
        chunk_bytes = chunk_data.len(),
        stamp_bytes = stamp.len(),
        "pushsync_phases",
    );

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
