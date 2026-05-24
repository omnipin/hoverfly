//! Bee pull-sync protocol — `/swarm/pullsync/1.4.0/{cursors,pullsync}`.
//!
//! Bee runs pullsync between connected kademlia neighbors to
//! continuously synchronise their chunk reserves. There are TWO
//! sub-streams under the same `pullsync` protocol:
//!
//! - **`cursors`** (`/swarm/pullsync/1.4.0/cursors`): one-shot
//!   exchange. Dialer sends `Syn{}`, listener replies with
//!   `Ack{Cursors: [u64; 32], Epoch: u64}` — one cursor (the highest
//!   BinID seen so far) per PO bin, plus a monotonic epoch that
//!   identifies the listener's reserve instance.
//!
//! - **`pullsync`** (`/swarm/pullsync/1.4.0/pullsync`): the actual
//!   chunk-fetch exchange. Dialer sends `Get{Bin, Start}`, listener
//!   replies with `Offer{Topmost, Chunks: [Chunk]}`. If the offer is
//!   non-empty the dialer follows with `Want{BitVector}` and the
//!   listener delivers each wanted chunk as a separate `Delivery`
//!   message. If the offer is empty the exchange ends immediately
//!   (bee's `handler` at `pkg/pullsync/pullsync.go:183` returns nil
//!   on `len(offer.Chunks) == 0`).
//!
//! ## Inbound-respond-only implementation
//!
//! We're an upload client, not a real reserve maintainer. We don't
//! have chunks-by-bin to offer. But by **responding at all** to
//! these protocol streams (even with empty cursors and empty
//! offers) we look more like a bee citizen than a peer that
//! silently rejects the protocol. Bee's salud probe and kademlia
//! membership decisions are influenced by which protocols a peer
//! speaks; pullsync is one of them.
//!
//! See PERFORMANCE.md "Bee-citizenship: pullsync" for the strategic
//! rationale and measured impact.

use crate::proto::pullsync as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use thiserror::Error;

pub const CURSORS_PROTOCOL: &str = "/swarm/pullsync/1.4.0/cursors";
pub const PULLSYNC_PROTOCOL: &str = "/swarm/pullsync/1.4.0/pullsync";

/// Number of proximity-order bins. Mirrors bee's `swarm.MaxBins`.
pub const NUM_BINS: usize = 32;

#[derive(Debug, Error)]
pub enum PullsyncError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
}

/// Respond to one inbound `cursors` substream. Reads `Syn`, writes
/// back `Ack` with all-zero cursors and a static epoch.
///
/// Bee accepts all-zero cursors as a valid "I have nothing synced
/// yet" response. The epoch is bee's notion of our reserve identity
/// — if it changes between cursor exchanges, bee assumes we wiped
/// and resynced. We pick a stable nonzero value derived from
/// session start time so multiple cursor probes during one session
/// see the same epoch.
pub async fn respond_cursors<S>(stream: &mut S, epoch: u64) -> Result<(), PullsyncError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let _syn: pb::Syn = read_message(stream).await?;
    let ack = pb::Ack {
        cursors: vec![0u64; NUM_BINS],
        epoch,
    };
    write_message(stream, &ack).await?;
    Ok(())
}

/// Respond to one inbound `pullsync` substream. Reads `Get{Bin,
/// Start}`, writes back `Offer{Topmost: Start, Chunks: []}`. Bee's
/// handler short-circuits on empty offers
/// (`pkg/pullsync/pullsync.go:183`), so the dialer won't send a
/// `Want` and we don't need to deliver anything.
///
/// `Topmost` should be the highest BinID we've seen in this bin;
/// we report `Start` to indicate "I haven't received anything past
/// where you started looking." Bee accepts this without complaint.
pub async fn respond_pullsync_empty<S>(stream: &mut S) -> Result<(), PullsyncError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let get: pb::Get = read_message(stream).await?;
    let offer = pb::Offer {
        topmost: get.start,
        chunks: Vec::new(),
    };
    write_message(stream, &offer).await?;
    Ok(())
}
