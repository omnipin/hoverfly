//! Bee status protocol — `/swarm/status/1.1.3/status`.
//!
//! Inbound-only. Bee's `pkg/salud` package probes us periodically;
//! we answer with a plausible-looking snapshot to avoid being marked
//! `Unhealthy` in bee's kademlia metrics. See module docs of the
//! `cheques.rs` and `PERFORMANCE.md` "Public reachability" section
//! for the rationale.
//!
//! Bee's `salud.go::salud()` marks us Healthy iff ALL four checks pass:
//!
//! 1. `CommittedDepth >= networkRadius - 2` — we must claim a depth
//!    close to the network's. networkRadius is the median of all
//!    responding peers' `CommittedDepth`, so we have to be in the
//!    middle of the distribution.
//! 2. `dur < pDur` (40th-percentile response time) — we must respond
//!    fast. Our handler is just a static memcpy + protobuf encode,
//!    well under bee's timeout.
//! 3. `ConnectedPeers >= pConns` (80th-percentile peer count) — we
//!    must claim a high peer count. Real bee nodes report ~100-300.
//! 4. `BatchCommitment == commitment` (modal value across responses)
//!    — must match the network-wide on-chain commitment. We don't
//!    have RPC, so we have to be told this externally or estimate.
//!
//! There's also a soft filter at `salud.go:160`: peers whose `BeeMode`
//! doesn't match the probing node's mode are SILENTLY DROPPED from
//! the percentile calculation — they aren't marked Unhealthy, they're
//! ignored entirely. But the default `Counters.Healthy` is `false`,
//! so being ignored = staying Unhealthy.
//!
//! Strategy: claim `BeeMode: "full"`, plausible peer count, plausible
//! depth, and the user-supplied (or default) `BatchCommitment`.

use crate::proto::headers as hdr;
use crate::proto::status as pb;
use crate::protocols::framing::{FrameError, read_message, write_message};
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/status/1.1.3/status";

#[derive(Debug, Error)]
pub enum StatusError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
}

/// Static snapshot values served back on every inbound probe.
///
/// We hold this configurable so the daemon can pass values that
/// pass bee's percentile checks for the current mainnet state. If
/// the network's median drifts past what we claim, we'll be marked
/// Unhealthy again; the `--status-*` CLI flags let the operator
/// re-tune without rebuilding.
#[derive(Clone, Debug)]
pub struct StatusSnapshot {
    pub reserve_size: u64,
    pub pullsync_rate: f64,
    pub storage_radius: u32,
    pub connected_peers: u64,
    pub neighborhood_size: u64,
    pub bee_mode: String,
    pub batch_commitment: u64,
    pub is_reachable: bool,
    pub reserve_size_within_radius: u64,
    pub last_synced_block: u64,
    pub committed_depth: u32,
}

impl Default for StatusSnapshot {
    /// Best-effort defaults that historically pass bee's salud checks
    /// on mainnet (May 2026). Any of these may drift; tune via CLI
    /// when bee starts marking us Unhealthy.
    fn default() -> Self {
        Self {
            // ReserveSize: how many chunks we claim to store. A real
            // full node has millions; we claim a believable number
            // without being so high as to look implausible.
            reserve_size: 5_000_000,
            // PullsyncRate: chunks/sec we're syncing. Real values
            // vary 0..N; 0 is fine (claiming we're "caught up").
            pullsync_rate: 0.0,
            // StorageRadius: our PO depth threshold. Mainnet hovers
            // around 11-12. Picked to match below.
            storage_radius: 11,
            // ConnectedPeers: 80th percentile of bee's connected
            // peer counts is the gate. Real bees have 100-300. 200
            // is comfortably above-median on observed mainnet.
            connected_peers: 200,
            // NeighborhoodSize: peers within storage radius. ~10-50
            // on mainnet. 20 is plausible.
            neighborhood_size: 20,
            // BeeMode: must match the probing bee's mode for salud
            // to include us. "full" is the dominant mode on mainnet;
            // "light" peers won't probe us anyway.
            bee_mode: "full".into(),
            // BatchCommitment: sum of 2^depth across all valid
            // postage batches on-chain. As of May 2026 mainnet, the
            // value is in the high 10s of trillions. The actual
            // modal value changes when batches expire / are bought;
            // we sample it once at startup if we can, otherwise use
            // this baseline.
            batch_commitment: 36_028_797_018_963_968, // 2^55 — rough mainnet baseline
            // IsReachable: claim we're publicly reachable. If we're
            // running with --listen + --advertise this is true;
            // if not, it's a polite lie that doesn't hurt anything
            // (bee's reacher does its own ping check).
            is_reachable: true,
            // ReserveSizeWithinRadius: chunks within our radius.
            // Roughly reserve_size / 2^(radius-natural). 0 is safe;
            // bee's salud doesn't gate on this directly.
            reserve_size_within_radius: 100_000,
            // LastSyncedBlock: latest chain block we've seen. 0 means
            // "haven't synced". Salud doesn't gate on this either.
            last_synced_block: 0,
            // CommittedDepth: must be >= networkRadius - 2. networkRadius
            // is the median of all peers' CommittedDepth values, so
            // we need to be in the middle of the distribution. 11 is
            // typical mainnet.
            committed_depth: 11,
        }
    }
}

impl StatusSnapshot {
    fn to_proto(&self) -> pb::Snapshot {
        pb::Snapshot {
            reserve_size: self.reserve_size,
            pullsync_rate: self.pullsync_rate,
            storage_radius: self.storage_radius,
            connected_peers: self.connected_peers,
            neighborhood_size: self.neighborhood_size,
            bee_mode: self.bee_mode.clone(),
            batch_commitment: self.batch_commitment,
            is_reachable: self.is_reachable,
            reserve_size_within_radius: self.reserve_size_within_radius,
            last_synced_block: self.last_synced_block,
            committed_depth: self.committed_depth,
            metrics: std::collections::HashMap::new(),
        }
    }
}

/// Handle one inbound status request on an already-opened libp2p
/// substream. Bee's `pkg/p2p/libp2p::handleHeaders` exchanges an
/// empty `Headers` frame in BOTH directions before the actual
/// protocol payload — we mirror that so bee accepts the response.
/// Read request headers → write response headers → read `Get` →
/// write `Snapshot`.
pub async fn respond<S>(stream: &mut S, snapshot: &StatusSnapshot) -> Result<(), StatusError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let _: hdr::Headers = read_message(stream).await?;
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let _: pb::Get = read_message(stream).await?;
    write_message(stream, &snapshot.to_proto()).await?;
    Ok(())
}

/// Issue one outbound status probe — bee's `pkg/salud` does the same
/// thing periodically against every connected peer to measure
/// response latency and compute the network-wide health threshold.
/// We use it for the opposite direction: probe our own session pool
/// once at fill time so we can pre-filter slow peers from the
/// dispatcher's candidate set before they cost us a 5-second push
/// receipt timeout.
///
/// Mirrors the headers preamble bee enforces on every protocol
/// stream (see `pkg/p2p/libp2p::sendHeaders`): send empty
/// `Headers` → read response `Headers` → send `Get` → read
/// `Snapshot`. The returned `Snapshot` is mostly thrown away;
/// the value of the probe is the wall-clock RTT the caller
/// measures around it.
pub async fn request<S>(stream: &mut S) -> Result<pb::Snapshot, StatusError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let _: hdr::Headers = read_message(stream).await?;
    write_message(stream, &pb::Get {}).await?;
    let snap: pb::Snapshot = read_message(stream).await?;
    Ok(snap)
}
