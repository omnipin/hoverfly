# AGENTS.md

isheika is a Rust crate: a minimal, WASM-portable Swarm (Ethereum Swarm)
micro-client. Three user-facing operations: `discover`, `fetch`, `upload`.
`README.md`, `index.ts`, `package.json`, `bun.lock`, and `node_modules/` are
vestigial `bun init` artifacts and should be ignored (don't touch them, don't
rely on them).

## Transport

Native (`cfg(not(target_arch = "wasm32"))`) and WASM differ:

- **Native** speaks plain TCP **and** TCP-over-WebSocket, combined via
  `or_transport` in `src/transport.rs::build_swarm_from`. libp2p picks the
  right inner transport from the multiaddr's protocol stack. Mainnet bees
  publish plain `/ip4/.../tcp/.../p2p/...` underlays (no `/ws`), so on a
  native CLI run almost every dial is raw TCP — the "WS-only" framing in
  earlier docs and a few stale comments is wrong; only WASM is WS-only
  (browsers can't open raw TCP sockets, so `src/transport.rs::build_swarm`
  uses `libp2p::websocket_websys` only).
- Dialability is gated at peerlist-ingestion time by
  `src/dnsaddr.rs::is_dialable_multiaddr`: requires `/ip4/` (no DNS resolver,
  no v6) and either `/ws[s]` or plain `/tcp/` on native, `/ws[s]` only on
  wasm. The peers.json store reuses the same predicate via
  `peers.rs::is_dialable_str` in `PeerStore::upsert`.

## Build

- Native: `cargo build` / `cargo build --release`. Release is ~2-5× faster on
  crypto paths but only ~10-15% end-to-end (network dominates).
- WASM: **nightly + `build-std` + `--no-default-features`** (the `cli`
  feature pulls non-wasm deps). `.cargo/config.toml` already sets the
  atomics/bulk-memory rustflags. After any lib change:

  ```
  RUSTUP_TOOLCHAIN=nightly cargo check --target wasm32-unknown-unknown --no-default-features
  ```

  First-time setup:
  ```
  rustup target add wasm32-unknown-unknown --toolchain nightly
  rustup component add rust-src --toolchain nightly
  ```

- `build.rs` runs `prost-build` over every file in `proto/`. New wire types
  go in `proto/` and are re-exported under `src/lib.rs::proto`.

## Binaries

- `isheika` (`src/bin/isheika.rs`) — the CLI.
- `sigcheck` (`src/bin/sigcheck.rs`) — signer/handshake reference comparison
  tool, not user-facing.

Both require `--features cli` (default). The `cli` feature gates `clap`,
`tracing-subscriber`, `tar`, and `indicatif`.

## Tests / verification

There is no test suite. `dev-dependencies = tokio-test` exists but no
`#[test]`s or integration tests do. Verify changes by:

1. `cargo build` (native) + the wasm check above. Both must pass.
2. End-to-end against mainnet:
   `discover --healthcheck` → `upload` → cross-verify via
   `https://api.gateway.ethswarm.org/bzz/<root>/<path>` or `https://bzz.limo/bzz/<root>/`.
   The public gateway is flaky/rate-limited; an HTTP 500 typically means
   the chunk neighborhood isn't yet retrievable from that gateway's view,
   not a correctness bug. Bee dedupes by chunk address — re-uploading the
   same bytes is a no-op, so always use a fresh random file for perf work.

## WASM constraints (will bite you)

- `tokio_with_wasm::time::{Sleep, Timeout, Interval}` are **not `Send`**.
  Several `nectar-primitives` traits require `+ Send` on returned futures.
  Workarounds in this repo:
  - `send_wrapper::SendWrapper::new(future)` to satisfy `+ Send` on wasm
    (single-threaded, safe). See `ChunkGet for NetworkedStore` in
    `src/client.rs`.
  - Per-target `impl` blocks gated by
    `#[cfg(target_arch = "wasm32")]` / `#[cfg(not(target_arch = "wasm32"))]`.
  - `futures::future::BoxFuture<'_>` (Send) vs `LocalBoxFuture<'_>`
    (not Send) cfg-gated when storing futures in `FuturesUnordered`.
- `tokio_with_wasm` is missing: `runtime::Handle`, `time::Instant`,
  `time::interval_at`, `Sleep::reset`. `BlockingNetworkedStore` is therefore
  gated to non-wasm. For sleep-resets, re-pin a fresh
  `Box::pin(tokio::time::sleep(d))`.
- `Cargo.toml` deliberately pulls three `getrandom` package versions
  (0.2, 0.3, 0.4) on wasm — alloy-primitives 1.5.x pulls 0.4 transitively.
  Do not "clean up" these duplicates without checking the transitive graph.

## Architecture map

- `src/transport.rs` — libp2p transport (dual TCP + WS on native, WS-only on
  wasm), per-peer `PeerSession` with a single swarm-driver task + concurrent
  pushes via `Arc<SessionState>` + cloned `libp2p_stream::Control`.
  Accounting (`reserve_plur`, `balance_plur`, pseudosettle) lives here,
  guarded by `tokio::sync::Mutex`. Client-side ghost-balance mirror retires
  the session at `GHOST_BALANCE_LIMIT_PLUR`; `MAX_PUSHES_PER_SESSION` is the
  defence-in-depth ceiling.
- `src/client.rs` — high-level `discover`/`fetch`/`upload`. `NetworkedStore`
  implements nectar's `ChunkGet`; cache is shared via `Clone`. Upload uses
  an adaptive session pool with pre-warmed rotation, proximity-sorted
  per-chunk peer ordering, and an in-flight buffer capped at 128. Public
  `SessionPool` lets the daemon reuse a warm pool across requests;
  `*_with_pool` variants of `upload_bytes` / `upload_file_with_manifest`
  call `push_chunks_with_pool` directly. Collections still go through the
  one-shot `upload_collection`.
- `src/daemon.rs` — `#[cfg(unix)]` only. Long-running daemon that owns a
  `Transport` + in-memory `PeerStore` + lazy `Arc<SessionPool>` reused across
  requests. Unix-socket IPC, `u32-LE length` + JSON wire protocol. File
  contents pass by absolute path (not inline). **Not a security boundary** —
  anyone with socket access can read/write the daemon's filesystem and sign
  uploads with whatever key they send.
- `src/inbound.rs` — `#[cfg(not(target_arch = "wasm32"))]` only. Optional
  daemon listener for serving retrieval requests from the local upload
  cache.
- `src/protocols/` — bee wire protocols: `handshake`, `pricing`,
  `retrieval`, `pushsync`, `pseudosettle`, `hive`, `framing`.
- `src/peers.rs` — JSON-backed peer store. Each `Peer` carries a
  reachability cache (`last_dial_success_unix`, `last_dial_failure_unix`,
  `consecutive_failures`, `last_dial_rtt_ms`). `RECENT_FAILURE_SECS = 300`
  defines the deprioritization window. `upsert` filters underlays via
  `is_dialable_str` (same predicate as the transport), so non-`/ip4/` and
  non-dialable entries are silently dropped on ingestion.
- `src/manifest.rs` — mantaray encode/decode helpers.
- `src/wasm.rs` — `wasm-bindgen` façade. WASM-only module.
- `src/cache.rs`, `src/cid.rs`, `src/doh.rs`, `src/dnsaddr.rs`, `src/mime.rs`,
  `src/signer.rs` — support modules.
- `src/lib.rs` — public re-exports; canonical view of what's stable API.

## Repo conventions

- Two git remotes: `origin` → GitHub (`v1rtl/isheika.git`), `rad` → Radicle.
  `main` tracks `rad/main`; `git push` goes to Radicle, not GitHub. Push to
  `origin` explicitly when you want GitHub.
- `peers.json` is gitignored. The CLI writes reachability observations back
  into it on every operation; respect existing fields on read (see
  `apply_log` / `record_dial_{success,failure}`).
- Hard constants worth knowing before tuning (file:line):
  - `transport.rs:120  MAX_PUSHES_PER_SESSION = 10_000` — defence-in-depth
    safety net; normal rotation is driven by ghost balance, not this.
  - `transport.rs:131  GHOST_BALANCE_LIMIT_PLUR = 12_000_000` — client-side
    mirror of bee's `ghostBalance` disconnect threshold (~16.875M PLUR on
    bee, with headroom for in-flight pushes). Session retires when crossed.
  - `transport.rs:137-138  GHOST_BALANCE_PREWARM_{NUMERATOR,DENOMINATOR} = 2/3`
    — fraction of the limit at which a replacement session is pre-dialed.
  - `transport.rs:41,47  REFRESH_RATE_PLUR = 4_500_000`,
    `SAFE_PEER_THRESHOLD_PLUR = REFRESH_RATE_PLUR * 2` — pseudosettle math,
    mirrors bee's `pkg/node/node.go::refreshRate`.
  - `client.rs:79,84,678  DEFAULT_FETCH_CONCURRENCY = 5`,
    `DEFAULT_DISCOVER_CONCURRENCY = 16`,
    `DEFAULT_UPLOAD_CONCURRENCY = 8`.
  - `client.rs:1329  PREEMPT_INTERVAL = 30s` — only fires when no in-flight
    push has returned. With `CHUNK_PEER_PARALLELISM = 1` (intentional, no
    per-chunk racing) it's effectively a hung-session detector, not a
    racing timer.
  - `client.rs:1206,1213  DEAD_SKIP_SECS = 60`, `DEAD_STRIKES = 3` — how
    long to park a session entry, and how many rotation-dial failures
    trigger parking. Sized to outlast bee's ghost-overdraw blocklist
    and a rotation-dial cluster at high `--concurrency`.
  - `client.rs:1340  MAX_CHUNK_RETRIES = 10` with linear backoff
    `1000ms × (1+n)` capped at 10s (≈55 s total) — outer pusher-layer
    retry budget per chunk, sized to outlast DEAD_SKIP_SECS.
    Independent of `--max-retries`.
  - `transport.rs:118  is_connection_dead` deliberately excludes
    `Timeout` — a single slow op shouldn't retire the whole session
    on which dozens of other pushes might still be in flight.
  - `client.rs:1920  SESSION_DIAL_PARALLELISM = 32` — in-flight window
    while filling the session pool.
- Network IDs: `1` = mainnet (default), `10` = testnet/sepolia. Bootnode:
  `/dnsaddr/mainnet.ethswarm.org`.
- CLI has split timeouts: `--timeout` (per-operation, default 10 s, applies
  to pushsync / retrieval / pseudosettle substreams) ≠ `--dial-timeout`
  (session open: dial + identify + handshake + pricing, default 3 s). Don't
  conflate them. Bee's internal `pushsync.defaultTTL` is 30 s; setting
  `--timeout` below ~10-15 s on slow links causes spurious timeouts that
  bee then logs as ghost-balance overdraw on our overlay.
- CLI `--max-retries` per chunk: see `client.rs:1372`
  `cap = max_retries.max(1).min(order.len())`. `0` is silently promoted
  to `1` (one attempt); the value is also capped by the live pool size,
  so on a small or attrited pool the user-supplied number is the upper
  bound, not the guarantee.

## When changing this code

- After any `transport.rs`, `client.rs`, or trait-bound change, run both
  the native build and the wasm check. `Send`-bound regressions on wasm
  are by far the most common breakage.
- Network behaviour is empirical. If you change defaults or the constants
  above, measure against mainnet with a freshly randomised file (bee
  dedupes by chunk address: identical bytes re-upload in O(stamp) and tell
  you nothing about real throughput).
- The reference Bee implementation lives at
  `~/Coding/bee-browser/bee/pkg/{pushsync,pusher,accounting,node}`. When in
  doubt about protocol semantics — pushsync receipts, accounting,
  pseudosettle wall-second rule, ghostBalance/blocklist windows — read Bee
  directly; the upstream docs lag behind the code.
