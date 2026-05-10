//! Bee pseudosettle protocol — `/swarm/pseudosettle/1.0.0/pseudosettle`.
//!
//! Time-based credit refreshment. Without it, bee tracks our cumulative
//! debt (chunks pushed × per-chunk price) against our overlay and
//! blocklists us once we exceed `payment_threshold × (1 + tolerance/100)`
//! ≈ 16.875M PLUR by default (13.5M × 1.25). Every chunk costs
//! `(MaxPO - proximity + 1) × 10_000` PLUR (10K–320K).
//!
//! By sending a `Payment { amount }` we tell the peer "credit me back this
//! much". They accept up to `(now − last_settlement_ts) × refresh_rate`
//! PLUR, bounded by our actual debt with them, and return the accepted
//! amount in `PaymentAck`. The very first call from a given overlay has
//! `last_settlement_ts == 0`, so the accepted amount is bounded only by
//! our debt — i.e. the first refresh can pay off arbitrarily many chunks.
//!
//! Wire: `client → server`: `Headers` (framework) then `Payment`. `server
//! → client`: `Headers` then `PaymentAck`. We rely on the caller having
//! already exchanged headers (see `transport.rs::write_then_read_empty_headers`).

use crate::proto::pseudosettle as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/pseudosettle/1.0.0/pseudosettle";

#[derive(Debug, Error)]
pub enum PseudoSettleError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
}

#[derive(Debug, Clone)]
pub struct PaymentAck {
    /// Amount the peer accepted (≤ what we asked for).
    pub amount_plur: u128,
    /// Peer's Unix timestamp at the moment of credit.
    pub timestamp: i64,
}

/// Send a refresh payment of `amount_plur` PLUR, read back the
/// `PaymentAck`. Caller must already have exchanged empty Headers on the
/// stream.
pub async fn pay<S>(stream: &mut S, amount_plur: u128) -> Result<PaymentAck, PseudoSettleError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let payment = pb::Payment {
        amount: amount_plur.to_be_bytes().to_vec(),
    };
    write_message(stream, &payment).await?;

    let ack: pb::PaymentAck = read_message(stream).await?;

    // Decode big-endian variable-length amount.
    let mut buf = [0u8; 16];
    let take = ack.amount.len().min(16);
    let pad = 16 - take;
    buf[pad..].copy_from_slice(&ack.amount[..take]);
    Ok(PaymentAck {
        amount_plur: u128::from_be_bytes(buf),
        timestamp: ack.timestamp,
    })
}
