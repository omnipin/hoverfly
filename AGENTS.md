# AGENTS.md

isheika is a Rust crate: a WS-only, WASM-portable Swarm (Ethereum Swarm) micro-client. Three operations: `discover`, `fetch`, `upload`. The `README.md`, `index.ts`, `package.json`, `bun.lock`, and `node_modules/` are vestigial `bun init` artifacts; ignore them.

## Build

- Native: `cargo build` / `cargo build --release`. Release is ~2-5× on crypto paths but ~10-15% end-to-end (network is dominant).
- WASM: **requires nightly + `build-std` + `--no-default-features`** (the `cli` feature is non-wasm). Atomics/bulk-memory rustflags are already set in `.cargo/config.toml`. After any lib change run:

  ```
  RUSTUP_TOOLCHAIN=nightly cargo check --target wasm32-unknown-unknown --no-default-features
  ```

  First-time setup: `rustup target add wasm32-unknown-unknown --toolchain nightly && rustup component add rust-src --toolchain nightly`.

- `build.rs` invokes `prost-build` on `proto/*.proto`. Add new wire types in `proto/` and re-export under `src/lib.rs::proto`.

## Binaries

- `isheika` (`src/bin/isheika.rs`) — the CLI.
- `sigcheck` (`src/bin/sigcheck.rs`) — signer reference comparison tool, not user-facing.

Both require `--features cli` (default). The `cli` feature gates `clap`, `tracing-subscriber`, `tar`.

## Tests / verification

There is no test suite. `dev-dependencies = tokio-test` exists but there are no `#[test]`s or integration tests. Verify changes by:

1. `cargo build` + the wasm check above (both must pass).
2. End-to-end against mainnet: `discover --healthcheck` → `upload` → cross-verify via `https://api.gateway.ethswarm.org/bzz/<root>/<path>`. Gateway can be flaky / rate-limited; HTTP 500 usually means the chunk neighborhood isn't yet retrievable from the public gateway, not a correctness bug.

## WASM constraints (will bite you)

- `tokio_with_wasm::time::{Sleep, Timeout, Interval}` are **not `Send`**. Several traits in `nectar-primitives` require `+ Send` on returned futures. Common workarounds in this repo:
  - `send_wrapper::SendWrapper::new(future)` to satisfy `+ Send` on wasm (single-threaded, safe). See `ChunkGet for NetworkedStore` in `src/client.rs`.
  - Per-target `impl` blocks gated by `#[cfg(target_arch = "wasm32")]` / `#[cfg(not(target_arch = "wasm32"))]`.
  - `futures::future::BoxFuture<'_>` (Send) vs `LocalBoxFuture<'_>` (not Send) cfg-gated when storing futures in `FuturesUnordered`.
- `tokio_with_wasm` is missing: `runtime::Handle`, `time::Instant`, `time::interval_at`, `Sleep::reset`. `BlockingNetworkedStore` is therefore gated to non-wasm. For sleep-resets, re-pin a fresh `Box::pin(tokio::time::sleep(d))`.
- `Cargo.toml` deliberately pulls three `getrandom` package versions (0.2, 0.3, 0.4) on wasm — alloy-primitives 1.5.x pulls 0.4 transitively. Do not "clean up" these duplicates without checking the transitive graph.

## Architecture map

- `src/transport.rs` — libp2p WS transport, per-peer `PeerSession` with a single swarm-driver task + concurrent pushes via `Arc<SessionState>` + cloned `libp2p_stream::Control`. Accounting (`reserve_plur`, `balance_plur`, pseudosettle) lives here, guarded by `tokio::sync::Mutex`.
- `src/client.rs` — high-level `discover`/`fetch`/`upload`. `NetworkedStore` implements nectar's `ChunkGet`; cache is shared via `Clone`. Upload uses an adaptive session pool, per-chunk peer racing with preemption, pre-warmed session rotation (`PREWARM_WATERMARK`).
- `src/protocols/` — bee wire protocols (handshake, pricing, retrieval, pushsync, pseudosettle, hive, framing).
- `src/peers.rs` — JSON-backed peer store. `Peer` carries a reachability cache (`last_dial_success_unix`, `last_dial_failure_unix`, `consecutive_failures`, `last_dial_rtt_ms`). `RECENT_FAILURE_SECS = 300` defines the deprioritization window.
- `src/manifest.rs` — mantaray encode/decode helpers.
- `src/wasm.rs` — wasm-bindgen façade.
- `src/lib.rs` — public re-exports; the canonical view of what's stable API.

## Repo conventions

- Two git remotes: `origin` → GitHub (`v1rtl/isheika.git`), `rad` → Radicle. `main` tracks `rad/main`; `git push` goes to Radicle, not GitHub.
- `peers.json` is gitignored. The CLI writes reachability observations back into it on every operation; respect existing fields on read.
- Hard constants worth knowing before tuning:
  - `transport.rs::MAX_PUSHES_PER_SESSION = 25` — bee's `ghostBalance` disconnect kicks in around 16.875M PLUR; raising past ~50 needs client-side ghostBalance tracking.
  - `transport.rs::REFRESH_RATE_PLUR = 4_500_000`, `SAFE_PEER_THRESHOLD_PLUR = REFRESH_RATE_PLUR * 2` — pseudosettle math, mirrors bee's `pkg/node/node.go::refreshRate`.
  - `client.rs::PREWARM_WATERMARK = 20`, `PREEMPT_INTERVAL = 2.5s`, `SESSION_DIAL_PARALLELISM = 32`, `DEFAULT_FETCH_CONCURRENCY = 5`, `DEFAULT_DISCOVER_CONCURRENCY = 16`.
- Network IDs: `1` = mainnet (default), `10` = testnet/sepolia. Bootnode: `/dnsaddr/mainnet.ethswarm.org`.
- CLI has split timeouts: `--timeout` (per-operation, default 10 s) ≠ `--dial-timeout` (session open, default 3 s). Don't conflate.
- Upload `--max-retries 0` means "uncapped"; fetch uses the same convention. Fetch default is 0; upload default is 10.

## When changing this code

- After any `transport.rs`, `client.rs`, or trait-bound change, run both the native build and the wasm check. Send-bound regressions on wasm are the most common breakage.
- Network behaviour is empirical; if you change defaults or constants, measure against mainnet with a fresh random file (bee dedupes by chunk address, so the same bytes re-upload instantly and don't reflect real perf).
- The reference Bee implementation lives at `~/Coding/bee-browser/bee/pkg/{pushsync,pusher,accounting,node}`. When in doubt about protocol semantics, read Bee, not docs.
