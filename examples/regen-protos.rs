//! Regenerate `src/proto/*.rs` from `proto/*.proto`.
//!
//! `prost-build` emits one `.rs` file per `.proto` package into `OUT_DIR`.
//! We point `OUT_DIR` at `src/proto/` so the generated files land
//! directly in the repository tree, where they're committed and used
//! as ordinary modules — no `build.rs`, no `protoc` requirement at
//! consumer build time.
//!
//! Usage:
//!
//! ```sh
//! scripts/regen-protos.sh
//! # or:
//! OUT_DIR=src/proto cargo run --example regen-protos
//! ```
//!
//! Requires `protoc` on `$PATH` (the maintainer's machine only).

use std::path::PathBuf;

fn main() {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let out_dir = std::env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/proto"));

    let files: Vec<PathBuf> = [
        "handshake",
        "headers",
        "hive",
        "pricing",
        "retrieval",
        "pushsync",
        "pseudosettle",
        "swap",
        "status",
    ]
    .iter()
    .map(|name| proto_dir.join(format!("{name}.proto")))
    .collect();

    // Safety: this binary is single-threaded — set OUT_DIR before any
    // thread or other side-effecting call.
    unsafe { std::env::set_var("OUT_DIR", &out_dir) };
    prost_build::compile_protos(&files, &[&proto_dir]).expect("prost_build::compile_protos");
    println!("wrote generated bindings to {}", out_dir.display());
}
