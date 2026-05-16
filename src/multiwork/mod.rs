//! Multi-worker upload IPC: protocol types and helpers shared between
//! the coordinator (holds the batch-owner key, does stamping) and N
//! worker processes (hold their own ephemeral overlay keys, do
//! pushing).
//!
//! See `PERFORMANCE.md` ("Further work" item 1) and Phase 2 of the
//! multi-worker plan. Unix-only (uses `tokio::net::UnixStream` for
//! IPC); not built on wasm.

#[cfg(unix)]
pub mod protocol;
