//! Vendored, patched version of `libp2p_stream` (`protocols/stream` from
//! the rust-libp2p tree) with one critical change: outbound substream
//! opens are not serialised per-connection.
//!
//! Upstream `libp2p_stream::Handler` keeps a `pending_upgrade: Option<…>`
//! and refuses to start a second substream upgrade until the first one
//! finishes (see upstream `handler.rs:67-68`). With many concurrent
//! pushers per session — exactly our pushsync workload — this serialises
//! every chunk's substream open behind the previous one, dominating
//! per-chunk wall time (profiled at 277 ms p50, 1970 ms p95, 4937 ms p99
//! for the open phase alone on a 32-session pool).
//!
//! Our patched [`Handler`] replaces the singular `pending_upgrade` slot
//! with a `HashMap<UpgradeId, ...>` keyed by a monotonic `u64`. The
//! `OutboundOpenInfo` carries the id back through `FullyNegotiatedOutbound`
//! / `DialUpgradeError`, so multiple substream upgrades can be in flight
//! simultaneously and the handler never blocks on a previous one.
//!
//! Public API (`Behaviour`, `Control`, `IncomingStreams`,
//! `OpenStreamError`, `AlreadyRegistered`) is identical to upstream so
//! the rest of the crate doesn't need to know.
//!
//! Licensed MIT / Apache-2.0 (upstream's terms) — see the original at
//! <https://github.com/libp2p/rust-libp2p/tree/master/protocols/stream>.

mod behaviour;
mod control;
mod handler;
mod shared;
mod upgrade;

pub use behaviour::{AlreadyRegistered, Behaviour};
pub use control::{Control, IncomingStreams, OpenStreamError};
