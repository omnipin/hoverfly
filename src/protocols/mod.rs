//! Bee wire protocols layered over libp2p-stream.
//!
//! Each protocol speaks a small set of length-prefixed protobuf messages.
//! Headers (the `headers.proto` framing) are exchanged immediately after stream
//! open for retrieval and pushsync.

pub mod handshake;
pub mod hive;
pub mod pricing;
pub mod retrieval;
pub mod pushsync;
pub mod pseudosettle;
pub mod pullsync;
pub mod status;
pub mod swap;

pub mod framing;

pub mod stream_pool;
