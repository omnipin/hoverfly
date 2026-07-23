# AGENTS.md

hoverfly is a Rust crate: a minimal, WASM-portable **Swarm (Ethereum Swarm)
light client**. It speaks the real Bee wire protocols (handshake, hive,
pricing, pseudosettle, pushsync, retrieval, swap, status) directly over
libp2p, so it participates in the mainnet network as a first-class light
peer — it dials bees, gets dialled back, accounts/settles, uploads and
retrieves chunks — without running a full Bee node or storing the network's
chunks. It is *not* a thin HTTP wrapper around a gateway and not a full node.

User-facing operations: `discover`, `fetch` (incl. mantaray manifest path
resolution and mutable **feed**/ENS resolution), `upload`, plus on-chain
helpers (`batch`, `bridge`) and a long-running `daemon`.

`README.md`, `index.ts`, `package.json`, `bun.lock`, and `node_modules/` at
the repo root are vestigial `bun init` artifacts — ignore them (don't touch,
don't rely on them). `README.npm.md` is the real published-package readme.
The `apps/` dir holds two browser front-ends that embed the wasm build
(see "Apps").

## Transport

Native (`cfg(not(target_arch = "wasm32"))`) and WASM differ:

- **Native** speaks plain TCP **and** TCP-over-WebSocket, combined via
  `or_transport` in `src/transport.rs::build_swarm_from`. libp2p picks the
  right inner transport from the multiaddr's protocol stack. Mainnet bees
  publish plain `/ip4/.../tcp/.../p2p/...` underlays (no `/ws`), so on a
  native CLI run almost every dial is raw TCP; only WASM is WS-only
  (browsers can't open raw TCP sockets, so `src/transport.rs::build_swarm`
  uses `libp2p::websocket_websys` only, via the vendored `src/wsws/`).
- Dialability is gated at peerlist-ingestion time by
  `src/dnsaddr.rs::is_dialable_multiaddr`: requires `/ip4/` (no DNS resolver,
  no v6) and either `/ws[s]` or plain `/tcp/` on native, `/ws[s]` only on
  wasm. The peers.json store reuses the same predicate via
  `peers.rs::is_dialable_str` in `PeerStore::upsert`.
- DNS is **DoH-only** (`src/doh.rs`, `src/dnsaddr.rs`) — no system resolver.
  `/dnsaddr/mainnet.ethswarm.org` is resolved over HTTPS the same way in CLI,
  daemon and browser.

### Concurrent substream opens (vendored `stream_pool`)

`src/protocols/stream_pool/` is a **vendored, patched copy of
`libp2p_stream`** (upstream `protocols/stream`). The one change: upstream
serialises outbound substream upgrades behind a singular `pending_upgrade:
Option<…>`, so every pushsync chunk's substream open blocks on the previous
one — this dominated per-chunk wall time. Our `Handler` replaces that slot
with a `HashMap<UpgradeId, …>` keyed by a monotonic `u64`, so many upgrades
are in flight at once. Public API (`Behaviour`, `Control`, `IncomingStreams`,
`OpenStreamError`, `AlreadyRegistered`) is identical to upstream. Cap is
`DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES = 64`
(`stream_pool/handler.rs`), tunable via the CLI `--substream-upgrade-cap` /
`TransportConfig::max_concurrent_substream_upgrades`. There is **no external
`libp2p-stream` dependency** anymore.

## Build

- Native: `cargo build` / `cargo build --release`. Release is ~2-5× faster on
  crypto paths but only ~10-15% end-to-end (network dominates).
- Edition **2024**; crate version tracked in `Cargo.toml` (`0.1.x`).
- WASM: **nightly + `build-std` + `--no-default-features`** (the `cli`
  feature pulls non-wasm deps). `.cargo/config.toml` already sets the
  atomics/bulk-memory rustflags. After any lib change:

  ```
  RUSTUP_TOOLCHAIN=nightly cargo check --target wasm32-unknown-unknown --no-default-features
  ```

  **Threaded vs. threadless wasm (`wasm-threads` feature).** `wasm-threads`
  (default-OFF) forwards to `nectar-primitives/wasm-threads`, the only thing
  that pulls `wasm-bindgen-rayon` — whose presence forces wasm-bindgen's threads
  transform and a *shared* (`SharedArrayBuffer`) memory, requiring COOP/COEP
  cross-origin isolation on the hosting page. Two intended builds:
  - **Gateway (threaded):** `--no-default-features --features wasm-threads`,
    with the atomics/`--shared-memory` rustflags from `.cargo/config.toml`.
    Faster BMT hashing; must be served cross-origin-isolated. This is what
    `apps/gateway` builds. It calls `initThreadPool` and polls retrieval
    futures across Web Worker threads (see `idb_chunk_store` threading note).
  - **Upload dApp (threadless, no shared memory):** `--no-default-features`
    (omit `wasm-threads`) **and** override the rustflags with empty `RUSTFLAGS`
    so `--shared-memory`/`+atomics` aren't applied → a plain non-shared linear
    memory, no `SharedArrayBuffer`, no COOP/COEP. Runs on hosts that can't set
    those headers (e.g. the eth.limo ENS gateway). See `apps/upload/build-wasm.sh`.
    Single-threaded: **do not** call `initThreadPool` on this build, and nectar's
    `split` rayon paths run inline with no pool. (The upload path also must never
    hit `std::time::{SystemTime,Instant}::now()` — use `web_time`/`js_sys::Date`
    — and must avoid rayon contention on `parking_lot` locks, which can't park
    a thread on wasm.)

  Nectar crates are pulled from **upstream 0.3.0** (crates.io). The old
  `[patch.crates-io]` omnipin fork and its bespoke `wasm-threads` gate are
  gone — upstream v0.3.0 has `MaybeSend`/`MaybeSync` (Send/Sync relaxed on
  wasm) and `web_time` natively. API notes vs 0.2.0:
  - `sync_split` → `split` (free function, same signature)
  - `SyncChunkGet`/`SyncChunkPut` → removed (use async `ChunkGet`/`ChunkPut`)
  - `ChunkStoreError::Other(String)` → `ChunkStoreError::Other(Box<dyn Error + Send + Sync>)`
  - `MemoryIssuer::from_batch` returns `Result<_, IssuerError>`

  There **is** one active `[patch.crates-io]`: `futures-bounded` →
  `vendor/futures-bounded`, whose `Delay::tokio` falls back to
  `futures-timer` on wasm32 (real tokio's timer panics in the browser). This
  fixes libp2p-identify's (and any other) `futures_bounded::Delay::tokio`
  usage on wasm.

  First-time setup:
  ```
  rustup target add wasm32-unknown-unknown --toolchain nightly
  rustup component add rust-src --toolchain nightly
  ```

- `build.rs` runs `prost-build` over every file in `proto/`. New wire types
  go in `proto/` and are re-exported under `src/lib.rs::proto`. Regenerate the
  committed `src/proto/*.rs` with `scripts/regen-protos.sh` (uses the
  `regen-protos` example + `prost-build` dev-dep; not part of a normal build).

## Binaries

- `hoverfly` (`src/bin/hoverfly.rs`) — the CLI. Subcommands: `discover`,
  `fetch`, `upload`, `bmt` (compute a BMT/collection root offline), `daemon`,
  `save-peers`, `vanity-overlay`, `batch create` (on-chain postage batch),
  `bridge`.
- `sigcheck` (`src/bin/sigcheck.rs`) — signer/handshake reference comparison
  tool, not user-facing.

Both require `--features cli` (default). The `cli` feature gates `clap`,
`tracing-subscriber`, `tar`, and `indicatif`.

The `bridge` feature (default-on) gates the `hoverfly bridge` subcommand and
`src/bridge.rs`. Compile it out with `--no-default-features --features cli`.
It adds no new dependencies (reuses the reqwest + alloy signing stack already
pulled in for `batch.rs`) and is native-only
(`#[cfg(all(not(target_arch = "wasm32"), feature = "bridge"))]`).

## Apps (browser front-ends embedding the wasm)

- `apps/gateway/` — browser-only Swarm **subdomain gateway** (like the IPFS
  service-worker-gateway). One `SharedWorker` runs a single hoverfly node for
  the whole gateway (warm peers + warm session cache); a broker iframe bridges
  the daemon MessagePort to each `<cid>.bzz.*` content origin; a service
  worker resolves paths against the mantaray manifest and returns `Response`s.
  Uses the **threaded** wasm build → requires COOP/COEP (its `serve.js`
  sets them). esbuild + pnpm.
- `apps/upload/` — prototype in-browser **upload dApp**. Connect an EIP-1193
  wallet (Gnosis/chain 100) → buy a postage batch → upload a file via an
  embedded hoverfly wasm node running in a Worker. Foreground-only (no
  SharedWorker). Uses the **threadless** wasm build (`build-wasm.sh`) → no
  `SharedArrayBuffer`, no COOP/COEP, so it can be hosted on the eth.limo /
  eth.link ENS gateway (which only send `Cross-Origin-Resource-Policy`).
  Key design: to avoid ~thousands of per-chunk wallet popups it mints an
  ephemeral in-browser secp256k1 **session key**, sets it as the `createBatch`
  owner, and signs all stamps locally (session-key.ts / wallet.ts isolate the
  signer so AA/7702/7579 can drop in later).

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
  Upstream nectar v0.3.0 uses `MaybeSend`/`MaybeSync` on wasm, relaxing the
  `+ Send` bound on ChunkGet. The old `send_wrapper` workaround for the
  `ChunkGet` impl is therefore removed. However, `send_wrapper` is still
  used by `src/wsws/mod.rs` to make the WebSocket `Connection` struct `Send`
  (libp2p's transport trait requires it).
- Per-target `impl` blocks gated by
  `#[cfg(target_arch = "wasm32")]` / `#[cfg(not(target_arch = "wasm32"))]`.
- `walk_manifest` in client.rs switched from `FuturesUnordered` (Send-bound)
  to sequential iteration on wasm because the ChunkGet future is no longer
  wrapped to be Send.
- `tokio_with_wasm` is missing: `runtime::Handle`, `time::Instant`,
  `time::interval_at`, `Sleep::reset`. For sleep-resets, re-pin a fresh
  `Box::pin(tokio::time::sleep(d))`. On the upload/wasm path never call
  `std::time::{SystemTime,Instant}::now()` — use `web_time::Instant` or
  `js_sys::Date`.
- `Cargo.toml` deliberately pulls three `getrandom` package versions
  (0.2, 0.3, 0.4) on wasm — alloy-primitives 1.6.x pulls 0.4 transitively.
  Do not "clean up" these duplicates without checking the transitive graph.
- `futures-timer` is pulled with the `wasm-bindgen` (gloo-timers) feature so
  libp2p-swarm/ping's `Delay` doesn't panic in-browser.

## Architecture map

- `src/transport.rs` — libp2p transport (dual TCP + WS on native, WS-only on
  wasm), per-peer `PeerSession` with a single swarm-driver task + concurrent
  pushes via `Arc<SessionState>` + cloned stream-pool `Control`.
  Accounting (`reserve_plur`, `balance_plur`, pseudosettle) lives here,
  guarded by `tokio::sync::Mutex`. Client-side ghost-balance mirror retires
  the session at `GHOST_BALANCE_LIMIT_PLUR`; `MAX_PUSHES_PER_SESSION` is the
  defence-in-depth ceiling. Hosts `TransportConfig`
  (incl. `max_concurrent_substream_upgrades`).
- `src/client.rs` — high-level `discover`/`fetch`/`upload`. `NetworkedStore`
  implements nectar's `ChunkGet`; cache is shared via `Clone`. Fetch resolves
  mantaray manifest paths and mutable **feeds** (`resolve_feed_root`,
  delegating to `src/feed.rs`). Upload uses an adaptive session pool with
  pre-warmed rotation, proximity-sorted per-chunk peer ordering, and an
  in-flight buffer capped at 128. Public `SessionPool` lets the daemon reuse a
  warm pool across requests; `*_with_pool` variants of `upload_bytes` /
  `upload_file_with_manifest` call `push_chunks_with_pool` directly.
  Collections still go through the one-shot `upload_collection`.
- `src/feed.rs` — Swarm **feed retrieval** (read-only). Resolves the latest
  update of a sequence-indexed feed (single-owner chunks) via a concurrent
  exponential-probe + k-ary search, then extracts the content reference. Feed
  params come from a feed manifest's root-entry metadata (`swarm-feed-owner`
  / `-topic` / `-type`); this is how feed-backed ENS sites stay updatable.
  Publishing feeds is out of scope. Mirrors bee `pkg/feeds`.
- `src/daemon.rs` — `#[cfg(unix)]` only. Long-running daemon that owns a
  `Transport` + in-memory `PeerStore` + lazy `Arc<SessionPool>` reused across
  requests. Unix-socket IPC, `u32-LE length` + JSON wire protocol. File
  contents pass by absolute path (not inline). **Not a security boundary** —
  anyone with socket access can read/write the daemon's filesystem and sign
  uploads with whatever key they send.
- `src/inbound.rs` — `#[cfg(not(target_arch = "wasm32"))]` only. Optional
  daemon listener for serving retrieval requests from the local upload cache.
- `src/protocols/` — bee wire protocols. Current on-wire ids:
  `handshake` `15.0.0` (+ `14.0.0` fallback), `hive` `2.0.0` (+ `1.1.0`),
  `pricing` `1.0.0`, `pseudosettle` `1.0.0`, `pushsync` `1.3.1`,
  `retrieval` `1.4.0`, `swap` `1.0.0`, `status` `1.1.3`; plus `framing`
  and the vendored `stream_pool`. `handshake` and `hive` support two
  versions concurrently (bee 2.8.0 raised handshake `14→15` and hive
  `1.1→2.0` as a network-wide upgrade, May 2026): outbound tries v15/v2
  first and falls back on `UnsupportedProtocol`; inbound accepts both ids in
  parallel. The `Version` enum on each module disambiguates downstream. The
  `status` responder is inbound-only; bee's `pkg/salud` probes us to decide
  whether to mark us Healthy in its kademlia metrics collector.
- `src/bridge.rs` — `#[cfg(all(not(target_arch = "wasm32"), feature =
  "bridge"))]`. The *second* RPC-touching module (alongside `batch.rs`),
  feature-gated and native-only. Funds the signer's Gnosis address with
  xDAI + BZZ from another chain via the permissionless Relay REST API
  (`POST /quote/v2` → broadcast the returned origin-chain deposit tx(s) →
  poll `/intents/status/v3` until the solver fills on Gnosis). Signs
  **type-2 (EIP-1559)** origin txs (`sign_eip1559_tx`), unlike `batch.rs`'
  legacy type-0 — the L2 origins return 1559 fee fields. `--to both` uses
  the Beeport pattern: conditional xDAI gas top-up (only when the
  recipient is below threshold) followed by a BZZ swap, so it's one origin
  swap in the common case and two when a top-up is needed. No API key
  required (Relay is permissionless). `--from-token` accepts a bare symbol
  (e.g. `USDC`), resolved to the canonical address + decimals via Relay's
  `/chains` token list (`resolve_token`); a raw `0x` address is used
  verbatim with `--from-decimals`; omitted = native gas token. Verified
  end-to-end on Base→Gnosis mainnet (both address and symbol forms).
- `src/batch.rs` — `#[cfg(not(target_arch = "wasm32"))]`. On-chain postage
  batch creation on Gnosis (mirrors bee `postagecontract.CreateBatch`):
  approve BZZ → `createBatch(...)` (legacy EIP-155 type-0 tx via `alloy-rlp`)
  → parse the `BatchCreated` event. Depth/amount math is mirrored by
  `apps/upload`.
- `src/cheques.rs` — `#[cfg(not(target_arch = "wasm32"))]`. JSON-backed
  per-peer cumulative-payout sidecar (`cheques.json`). Required to
  persist across CLI runs because bee rejects non-strictly-increasing
  `CumulativePayout` (`chequestore.go::ErrChequeNotIncreasing`). Loaded
  by the CLI at startup when `--chequebook` is set, mutated under
  `SessionState::settle_lock`, flushed on upload completion.
- `src/peers.rs` — JSON-backed peer store. Each `Peer` carries a
  reachability cache (`last_dial_success_unix`, `last_dial_failure_unix`,
  `consecutive_failures`, `last_dial_rtt_ms`). `RECENT_FAILURE_SECS = 300`
  defines the deprioritization window. `upsert` filters underlays via
  `is_dialable_str` (same predicate as the transport), so non-`/ip4/` and
  non-dialable entries are silently dropped on ingestion.
- `src/stamp.rs` — postage-stamp wire validator (113-byte shape + owner
  recovery). Does NOT verify on-chain batch ownership (no RPC). Currently
  unused for ingestion; ready for a future chunk-ingestion path.
- `src/manifest.rs` — mantaray encode/decode helpers.
- `src/erasure/` — **erasure-coding-aware download** (Reed–Solomon). Since
  ~bee v2.8.1 gateway uploads are RS erasure coded by default, so a fresh
  upload's data chunks can be unretrievable for a forwarding-dependent light
  client while parity chunks let the file be reconstructed (ethersphere/bee
  #5541). `reedsolomon.rs` is a byte-exact port of klauspost's default matrix +
  GF(2^8) reconstruction (golden-vector tested); `mod.rs` has the bee span/level
  decode, per-level erasure tables, and `ReferenceCount`/`ChunkAddresses`
  helpers; `joiner.rs` is a bee-compatible tree-walking joiner that fetches each
  intermediate node's data children and RS-reconstructs any that time out from
  the node's parity siblings. `client::join_target` detects a level-encoded root
  span and routes to it, else falls back to nectar's plain `GenericJoiner`. All
  download entry points (CLI/daemon/wasm) funnel through it.
- `src/signer.rs` — `SwarmSigner`: overlay derivation, handshake signing
  (v14 + cached v15), eth-address recovery. See "Bee 2.8.0 protocol support".
- `src/wasm.rs` — `wasm-bindgen` façade (`HoverflyClient`): `start`/`stop`,
  peer load/merge/export, `prewarmSessions`, `enableChunkStore`,
  `discover`/`fetch`/`fetchManifestPath`/`listManifest`,
  `upload`/`uploadFile`/`uploadCollection`, upload progress/diagnostics, and
  feed-hint import/export. WASM-only.
- `src/idb_chunk_store.rs` — persistent IndexedDB-backed L2 chunk cache
  (browser only). Immutable content-addressed chunks survive reloads on top
  of the per-fetch in-memory cache in `client::NetworkedStore`. **Threading
  gotcha:** the threaded (gateway) build polls futures across rayon Web
  Worker threads, and the `indexed-db` handle (`Rc<Database>`) is `!Send` /
  thread-affine — so only the database *name* is process-global; each thread
  lazily opens + caches its own `Database` handle via `thread_local`. Uses
  the `indexed-db` crate specifically because it's the only binding that
  works under wasm-bindgen's multi-threaded futures executor.
- `src/wsws/` — vendored libp2p-websocket-websys, patched so
  `WebSocket.send()` gets a non-shared buffer (the wasm memory is a
  `SharedArrayBuffer` in the atomics build and Chrome rejects shared views).
- `src/cache.rs`, `src/cid.rs`, `src/doh.rs`, `src/dnsaddr.rs`, `src/mime.rs`
  — support modules.
- `src/lib.rs` — public re-exports; canonical view of what's stable API.

## Repo conventions

- Multiple git remotes: `github` → GitHub (`omnipin/hoverfly.git`),
  `rad` → Radicle (push), `iris` → Radicle (HTTPS mirror via
  `iris.radicle.xyz`), `vps` → SSH push to the VPS that runs the
  long-lived daemon. Push targets are explicit; there's no shared
  default — pick the remote you mean.
- `peers.json` is gitignored (runtime artifact). The CLI writes reachability
  observations back into it on every operation; respect existing fields on
  read (see `apply_log` / `record_dial_{success,failure}`).
- **`peers.seed.json`** (committed, ~800 IPs) and **`peers.ws.json`**
  (committed, WS-dialable subset for the browser builds) are IP-diverse
  cold-start seeds harvested from a long-running daemon. CI copies a seed to
  `peers.json` before starting the daemon so a fresh runner doesn't discover
  from scratch. Regenerate via `hoverfly save-peers --socket <sock>` against a
  daemon that's been running a few hours.
- Hard constants worth knowing before tuning (file:line — verify current
  values, they drift):
  - `transport.rs  MAX_PUSHES_PER_SESSION = 10_000` — defence-in-depth
    safety net; normal rotation is driven by ghost balance, not this.
  - `transport.rs  GHOST_BALANCE_LIMIT_PLUR = 12_000_000` — client-side
    mirror of bee's `ghostBalance` disconnect threshold (~16.875M PLUR on
    bee, with headroom for in-flight pushes). Session retires when crossed.
  - `transport.rs  GHOST_BALANCE_PREWARM_{NUMERATOR,DENOMINATOR} = 1/2`
    — fraction of the limit at which a replacement session is pre-dialed.
  - `transport.rs  REFRESH_RATE_PLUR = 4_500_000`,
    `SAFE_PEER_THRESHOLD_PLUR = REFRESH_RATE_PLUR * 2` — pseudosettle math,
    mirrors bee's `pkg/node/node.go::refreshRate`.
  - `stream_pool/handler.rs  DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES = 64`
    — per-connection concurrent substream-upgrade cap (`--substream-upgrade-cap`).
  - `client.rs  DEFAULT_FETCH_CONCURRENCY = 5`,
    `DEFAULT_DISCOVER_CONCURRENCY = 16`, `DEFAULT_UPLOAD_CONCURRENCY = 8`.
  - `client.rs  CHUNK_PEER_PARALLELISM = 3` — each chunk races up to 3
    proximity-ordered peers (≈2-3× throughput for ≈3× bandwidth).
    `PREEMPT_INTERVAL = 1s` extends/tops-up that race window when the initial
    seed used fewer peers or after an early shallow/error reply — short enough
    to race on per-chunk RTT timescales.
  - `client.rs  DEAD_SKIP_SECS = 15`, `DEAD_STRIKES = 3` — how long to park a
    session entry, and how many rotation-dial failures trigger parking.
  - `client.rs  MAX_CHUNK_RETRIES = 60` with 500ms retry penalty per failed
    dispatch — outer pusher-layer retry budget per chunk (mirrors bee's
    `pusher.DefaultRetryCount` philosophy). Independent of `--max-retries`.
  - `transport.rs  is_connection_dead` deliberately excludes `Timeout` — a
    single slow op shouldn't retire a whole session with many in-flight pushes.
  - `client.rs  SESSION_DIAL_PARALLELISM = 128` — in-flight window while
    filling the session pool (absorbs the high mainnet dial-rejection rate).
- Network IDs: `1` = mainnet (default), `10` = testnet/sepolia. Bootnode:
  `/dnsaddr/mainnet.ethswarm.org`. **EVM chain id** is separate from
  network id (it's the `chainID` in the cheque's EIP-712 domain): 100
  for Gnosis / Swarm mainnet, 11155111 for Sepolia. Set via
  `--chequebook-chain-id`.
- **Bee-citizenship features** (May 2026) for long-term kademlia presence
  growth: stable overlay across runs (persist nonce via `--nonce-file`,
  default `overlay-nonce` in CWD; see `signer::from_bytes_with_nonce`),
  outbound hive self-announce on every session connect
  (`protocols::hive::announce_self`, invoked from `transport::do_hive_announce`
  after the bee handshake), inbound status responder (`protocols::status`).
  Slow-burn lever: bees that learn about us via gossip add us to `knownPeers`
  and may dial us back later, growing our kademlia presence beyond a single
  session. A pullsync inbound responder was tried and dropped (constant probe
  noise, no reciprocal benefit — we store no chunks). See PERFORMANCE.md
  "Bee-citizenship".
- **Bee 2.8.0 protocol support** (May 2026). Handshake v15 + hive v2 carry a
  signed `timestamp` + `chequebook_address` in the `BzzAddress`.
  `SwarmSigner::sign_handshake_v15_cached` caches the `(timestamp, signature)`
  pair per `(underlay, chequebook)` so reconnects to the same peer replay an
  **identical** record. Bee 2.8's gossip path rejects updates within
  `MinimumUpdateInterval = 300 s` of the existing record, so re-issuing a
  fresh signature every reconnect would age our addressbook entry out across
  the network. (Bee itself later adopted the same "sign once, reuse until the
  advertised data changes" approach in v2.8.1 — hoverfly already did this by
  construction; nothing to change.) Also added `libp2p::ping::Behaviour`
  because bee 2.8's reacher uses `/ipfs/ping/1.0.0` to verify reachability;
  failed pings mark us private and the kademlia prune loop kicks us. See
  PERFORMANCE.md "Bee 2.8.0 protocol migration".
- **SWAP / chequebook** is implemented but scoped to *issuance only*: no
  contract deploy, no cashout, no on-chain RPC. Caller supplies an
  already-deployed chequebook via `--chequebook` whose `issuer()` matches
  `--key`'s eth address. Sessions advertise the beneficiary in a one-shot
  `/swarm/swap/1.0.0/swap` handshake at connect; `try_settle_once` then emits
  a cheque for the PLUR remainder after pseudosettle. Exchange-rate fallback
  is abort→pseudosettle-only (no hardcoded rate; trust bee's per-stream
  `exchange`+`deduction` headers). Correct but no measured throughput benefit
  at one-shot upload workloads. See PERFORMANCE.md "SWAP / chequebook".
- **Diagnostics** (May 2026): `diag::CONN_CLOSED_IO_DETAIL` buckets
  `ConnectionClosed.cause` (empirically ~100% ECONNRESET from bee's kademlia
  bin-prune of non-public peers — mitigate with `daemon + --listen +
  --advertise`); per-stream/per-chunk latency histograms
  (`diag::PUSH_LATENCY_*`, `OPEN_STREAM_*`, `PUSH_OUTCOME_*`,
  `CHUNK_LATENCY_*`) printed at upload end, shaped to match bee's Prometheus
  metrics for direct A/B. See PERFORMANCE.md.
- CLI has split timeouts: `--timeout` (per-operation, default 10 s, applies
  to pushsync / retrieval / pseudosettle substreams) ≠ `--dial-timeout`
  (session open: dial + identify + handshake + pricing, default 3 s). Don't
  conflate them. Bee's internal `pushsync.defaultTTL` is 30 s; setting
  `--timeout` below ~10-15 s on slow links causes spurious timeouts that
  bee then logs as ghost-balance overdraw on our overlay.
- CLI `--max-retries` per chunk: see `client.rs`
  `cap = max_retries.max(1).min(order.len())`. `0` is silently promoted to
  `1`; the value is also capped by the live pool size, so on a small/attrited
  pool the user-supplied number is the upper bound, not the guarantee.

## When changing this code

- After any `transport.rs`, `client.rs`, or trait-bound change, run both
  the native build and the wasm check. `Send`-bound regressions on wasm
  are by far the most common breakage (nectar v0.3.0 `MaybeSend` relaxes
  this for ChunkGet, but other paths like `tokio::spawn` still require Send).
- Network behaviour is empirical. If you change defaults or the constants
  above, measure against mainnet with a freshly randomised file (bee
  dedupes by chunk address: identical bytes re-upload in O(stamp) and tell
  you nothing about real throughput).
- The reference Bee implementation lives at
  `~/Coding/forks/bee/pkg/{pushsync,pusher,accounting,node,p2p,bzz,hive,topology,feeds,salud}`.
  When in doubt about protocol semantics — pushsync receipts, accounting,
  pseudosettle wall-second rule, ghostBalance/blocklist windows, the v15
  handshake / v2 hive wire format, feed derivation, kademlia
  saturation/prune behavior — read Bee directly; the upstream docs lag the
  code. Check out the tag running on mainnet
  (`git -C ~/Coding/forks/bee checkout v2.8.1`) when you need exact code.
