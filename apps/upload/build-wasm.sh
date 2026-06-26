#!/usr/bin/env bash
# Build the hoverfly wasm for the upload dApp — the NO-SHARED-MEMORY variant.
#
# Unlike the gateway (which builds with `--features wasm-threads` + the
# atomics/shared-memory rustflags in the repo's .cargo/config.toml), the upload
# dApp builds hoverfly WITHOUT the thread pool, so the resulting wasm uses a
# plain (non-shared) memory and needs no SharedArrayBuffer / no cross-origin
# isolation (no COOP/COEP). That's what lets it run on the eth.limo ENS gateway,
# which only sends CORP, not COOP/COEP.
#
# The trick is overriding the repo's global .cargo/config.toml wasm rustflags
# (which force +atomics / --shared-memory) with an empty RUSTFLAGS for this
# build, and omitting `--features wasm-threads` so nothing pulls
# wasm-bindgen-rayon (the dep that forces wasm-bindgen's threads transform).
#
# Output goes to apps/upload/pkg/ (gitignored), vendored into dist/ by build.js.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
out="$here/pkg"

: "${RUSTUP_TOOLCHAIN:=nightly}"
export RUSTUP_TOOLCHAIN

echo "→ building threadless (no-shared-memory) hoverfly wasm…"
# Empty RUSTFLAGS overrides .cargo/config.toml's atomics/shared-memory flags.
# build-std with plain std (no atomics) → a non-shared linear memory.
RUSTFLAGS="" CARGO_BUILD_RUSTFLAGS="" \
  cargo build --release --locked \
    --manifest-path "$repo/Cargo.toml" \
    --target wasm32-unknown-unknown \
    --no-default-features --lib \
    -Z build-std=std,panic_abort

echo "→ wasm-bindgen → $out"
wasm-bindgen --target web --out-dir "$out" \
  "$repo/target/wasm32-unknown-unknown/release/hoverfly.wasm"

echo "✓ no-shared-memory wasm ready in $out"
