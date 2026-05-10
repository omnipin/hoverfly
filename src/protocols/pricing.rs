//! Bee pricing protocol — `/swarm/pricing/1.0.0/pricing`.
//!
//! Both peers exchange a payment threshold after handshake. We send a
//! sensible default (`13_500_000` PLUR, in the bee-valid range) and read
//! the peer's threshold without acting on it — chunk fetches in this
//! micro-client are well below any threshold.

use crate::proto::pricing as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/pricing/1.0.0/pricing";

const DEFAULT_THRESHOLD: u128 = 13_500_000;

#[derive(Debug, Error)]
pub enum PricingError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
}

pub async fn announce<S>(stream: &mut S) -> Result<(), PricingError>
where
    S: futures::AsyncWrite + Unpin,
{
    let msg = pb::AnnouncePaymentThreshold {
        payment_threshold: DEFAULT_THRESHOLD.to_be_bytes().to_vec(),
    };
    write_message(stream, &msg).await?;
    Ok(())
}

pub async fn read_announcement<S>(stream: &mut S) -> Result<u128, PricingError>
where
    S: futures::AsyncRead + Unpin,
{
    let msg: pb::AnnouncePaymentThreshold = read_message(stream).await?;
    let mut buf = [0u8; 16];
    let take = msg.payment_threshold.len().min(16);
    let pad = 16 - take;
    buf[pad..].copy_from_slice(&msg.payment_threshold[..take]);
    Ok(u128::from_be_bytes(buf))
}
