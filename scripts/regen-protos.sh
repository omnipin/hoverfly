#!/usr/bin/env bash
# Regenerate the committed `src/proto/*.rs` bindings from `proto/*.proto`.
#
# Requires `protoc` on $PATH. Run after editing any `.proto` file, then
# `git diff src/proto/` and commit. CI re-runs this script and fails on
# a non-empty diff to catch drift.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v protoc >/dev/null 2>&1; then
    echo "::error::protoc not found on \$PATH. Install via your package manager:" >&2
    echo "  apt:  sudo apt install protobuf-compiler" >&2
    echo "  brew: brew install protobuf" >&2
    echo "  pkg:  pkg install protobuf  (FreeBSD)" >&2
    exit 1
fi

protoc --version
cargo run --example regen-protos

# Format the generated files so reruns produce stable diffs.
# Pass through rustfmt directly to avoid `cargo fmt` walking the whole crate.
rustfmt --edition 2024 src/proto/*.rs

echo "done. review with 'git diff src/proto/' and commit if intentional."
