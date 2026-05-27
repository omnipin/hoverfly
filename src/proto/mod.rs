//! Generated protobuf bindings for the bee wire protocols.
//!
//! These files are produced by `prost-build` from the `.proto` definitions
//! under `proto/`. They are **committed to the repository** so `cargo build`
//! works without `protoc` installed on the host, and so the WASM /
//! cross-compilation matrix doesn't need to install it on every target.
//!
//! Regenerate after editing any `proto/*.proto` file:
//!
//! ```sh
//! scripts/regen-protos.sh   # requires `protoc` on PATH
//! ```
//!
//! CI runs the same script and fails on a non-empty `git diff` to catch
//! drift between `proto/*.proto` and `src/proto/*.rs`.

#![allow(clippy::all, clippy::pedantic, missing_docs, non_snake_case)]

pub mod handshake;
pub mod headers;
pub mod hive;
pub mod pricing;
pub mod pseudosettle;
pub mod pushsync;
pub mod retrieval;
pub mod status;
pub mod swap;
