# AGENTS.md

hoverfly is a Rust crate: a minimal, WASM-portable Swarm (Ethereum Swarm)
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

  **Threaded vs. threadless wasm (`wasm-threads` feature).** `wasm-threads`
  (default-OFF) forwards to `nectar-primitives/wasm-threads`, the only thing
  that pulls `wasm-bindgen-rayon` — whose presence forces wasm-bindgen's threads
  transform and a *shared* (`SharedArrayBuffer`) memory, requiring COOP/COEP
  cross-origin isolation on the hosting page. Two intended builds:
  - **Gateway (threaded):** `--no-default-features --features wasm-threads`,
    with the atomics/`--shared-memory` rustflags from `.cargo/config.toml`.
    Faster BMT hashing; must be served cross-origin-isolated. This is what
    `apps/gateway` builds.
  - **Upload dApp (threadless, no shared memory):** `--no-default-features`
    (omit `wasm-threads`) **and** override the rustflags with empty `RUSTFLAGS`
    so `--shared-memory`/`+atomics` aren't applied → a plain non-shared linear
    memory, no `SharedArrayBuffer`, no COOP/COEP. Runs on hosts that can't set
    those headers (e.g. the eth.limo ENS gateway). See `apps/upload/build-wasm.sh`.
    Single-threaded (nectar's `split` rayon paths run inline with no pool).

  Nectar crates are pulled from **upstream 0.3.0** (crates.io). The old
  `[patch.crates-io]` omnipin fork and its `wasm-threads` gate are gone —
  upstream v0.3.0 has `MaybeSend`/`MaybeSync` (Send/Sync relaxed on wasm)
  and `web_time` natively. API changes from 0.2.0:
  - `sync_split` → `split` (free function, same signature)
  - `SyncChunkGet`/`SyncChunkPut` → removed (use async `ChunkGet`/`ChunkPut`)
  - `ChunkStoreError::Other(String)` → `ChunkStoreError::Other(Box<dyn Error + Send + Sync>)`
  - `MemoryIssuer::from_batch` returns `Result<_, IssuerError>`

  First-time setup:
  ```
  rustup target add wasm32-unknown-unknown --toolchain nightly
  rustup component add rust-src --toolchain nightly
  ```

- `build.rs` runs `prost-build` over every file in `proto/`. New wire types
  go in `proto/` and are re-exported under `src/lib.rs::proto`.

## Binaries

- `hoverfly` (`src/bin/hoverfly.rs`) — the CLI.
- `sigcheck` (`src/bin/sigcheck.rs`) — signer/handshake reference comparison
  tool, not user-facing.

Both require `--features cli` (default). The `cli` feature gates `clap`,
`tracing-subscriber`, `tar`, and `indicatif`.

The `bridge` feature (default-on) gates the optional `hoverfly bridge`
subcommand and `src/bridge.rs`. Compile it out with `--no-default-features
--features cli`. It adds no new dependencies (reuses the reqwest + alloy
signing stack already pulled in for `batch.rs`) and is native-only
(`#[cfg(all(not(target_arch = "wasm32"), feature = "bridge"))]`).

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
  `retrieval`, `pushsync`, `pseudosettle`, `hive`, `framing`,
  `swap`, `status`. `handshake` and `hive` each support two on-wire
  versions concurrently (bee 2.8.0 raised handshake `14.0.0`→`15.0.0`
  and hive `1.1.0`→`2.0.0` as a network-wide upgrade in May 2026):
  outbound tries v15 first and falls back to v14 on
  `UnsupportedProtocol`; inbound accepts both ids in parallel. The
  `Version` enum on each module disambiguates downstream. The
  `status` responder is inbound-only; bee's `pkg/salud` probes us
  via `/swarm/status/1.1.0/status` to decide whether to mark us
  Healthy in its kademlia metrics collector.
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
- `src/manifest.rs` — mantaray encode/decode helpers.
- `src/wasm.rs` — `wasm-bindgen` façade. WASM-only module.
- `src/cache.rs`, `src/cid.rs`, `src/doh.rs`, `src/dnsaddr.rs`, `src/mime.rs`,
  `src/signer.rs` — support modules.
- `src/lib.rs` — public re-exports; canonical view of what's stable API.

## Repo conventions

- Multiple git remotes: `github` → GitHub (`omnipin/hoverfly.git`),
  `rad` → Radicle (push), `iris` → Radicle (HTTPS mirror via
  `iris.radicle.xyz`), `vps` → SSH push to the VPS that runs the
  long-lived daemon. Push targets are explicit; there's no shared
  default — pick the remote you mean.
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
  - `client.rs  MAX_CHUNK_RETRIES = 60` with 500ms flat backoff
    (≈30 s total in the common case, longer if peers genuinely
    take time) — outer pusher-layer retry budget per chunk. Was
    `10` with linear backoff in older docs; bumped after observing
    that bee mainnet bin-saturation and ghost-overdraw blocklist
    windows occasionally need more retry runway. Independent of
    `--max-retries`.
  - `transport.rs:118  is_connection_dead` deliberately excludes
    `Timeout` — a single slow op shouldn't retire the whole session
    on which dozens of other pushes might still be in flight.
  - `client.rs  SESSION_DIAL_PARALLELISM = 128` — in-flight window
    while filling the session pool. Was `32`; bumped to absorb the
    ~97% dial rejection rate against the post-bee-2.8 mainnet
    where most peers either run 2.8 or have entries in our seed
    that are now-stale.
- Network IDs: `1` = mainnet (default), `10` = testnet/sepolia. Bootnode:
  `/dnsaddr/mainnet.ethswarm.org`. **EVM chain id** is separate from
  network id (it's the `chainID` in the cheque's EIP-712 domain): 100
  for Gnosis / Swarm mainnet, 11155111 for Sepolia. Set via
  `--chequebook-chain-id`.
- **Bee-citizenship features** (May 2026) for long-term kademlia
  presence growth: stable overlay across runs (persist nonce via
  `--nonce-file`, default `overlay-nonce` in CWD; see
  `signer::from_bytes_with_nonce`), outbound hive self-announce on
  every session connect (`protocols::hive::announce_self`,
  invoked from `transport::do_hive_announce` after the bee
  handshake), inbound status responder (`protocols::status`).
  Hive announces fire at ~160 per 5 MiB upload at c=64.
  Single-upload throughput unchanged in benchmarks; the design is
  a slow-burn lever — bees that learn about us via gossip add us
  to their `knownPeers` and may dial us back hours later,
  growing our kademlia presence beyond what any single session
  could. We previously also exposed an inbound pullsync responder
  (empty cursors / empty offers) but dropped it: it was the only
  piece bee actually probed, but each empty response immediately
  triggered another probe, creating constant noise with no
  reciprocal benefit — we don't store chunks, so non-empty offers
  aren't possible. See PERFORMANCE.md "Bee-citizenship".
- **Bee 2.8.0 protocol support** (also May 2026, day-of-release).
  Handshake v15 (`signer::sign_handshake_v15`) and hive v2 carry
  signed `timestamp` + `chequebook_address` fields in the
  `BzzAddress`. `SwarmSigner::sign_handshake_v15_cached` caches
  the `(timestamp, signature)` pair per `(underlay, chequebook)`
  so reconnects to the same peer replay an identical record —
  bee 2.8's gossip path rejects updates within
  `MinimumUpdateInterval = 300 s` of the existing record, so
  re-issuing a fresh signature every minute-scale reconnect
  ages our addressbook entry out across the network. Also added
  `libp2p::ping::Behaviour` because bee 2.8's reacher uses
  `/ipfs/ping/1.0.0` to verify peer reachability; failed pings
  mark us `ReachabilityStatusPrivate` and the kademlia 5-min
  prune loop kicks us. See PERFORMANCE.md "Bee 2.8.0 protocol
  migration" for the full story.
- **`peers.seed.json`** (committed, ~700 IPs as of mid-2026). An
  IP-diverse cold-start seed harvested from a long-running daemon.
  CI workflows copy it to `peers.json` before starting the daemon
  so a fresh runner doesn't have to discover from scratch (which,
  on AWS/Azure egress to a EU-Hetzner-heavy network, is slow).
  Local installs that want fast cold-start can do the same.
  Regenerate via the `hoverfly save-peers --socket <sock>` CLI
  on a daemon that's been running a few hours.
- **Postage stamp signature validator** (`src/stamp.rs`). Validates
  the 113-byte wire-format stamp shape and recovers the batch
  owner's Ethereum address from the signature. Does NOT verify
  on-chain that the recovered address actually owns the claimed
  batch — that would require an RPC call we deliberately don't
  make. Currently unused (we only emit stamps via
  `nectar-postage`, never ingest them); ready for the future
  chunk-ingestion path (pullsync delivery, retrieval-forwarding).
- **SWAP / chequebook** is implemented but scoped to *issuance only*:
  no contract deploy, no cashout, no on-chain RPC. Caller supplies an
  already-deployed chequebook via `--chequebook` whose `issuer()`
  matches `--key`'s Ethereum address. `transport::SwapConfig` carries
  the cheque store and per-peer cap. Sessions advertise the
  beneficiary in a one-shot `/swarm/swap/1.0.0/swap` handshake at
  connect time; `SessionState::try_settle_once` then emits a cheque
  for the PLUR remainder after pseudosettle clears what it can.
  Exchange-rate fallback is `abort and fall through to
  pseudosettle-only` (no hardcoded rate; we trust bee's per-stream
  `exchange`+`deduction` headers, which it derives from its on-chain
  priceoracle poll). Bee's `chequestore.go::ReceiveCheque` does an
  on-chain `chequebook.issuer()` + `balance()` + `paidOut()` triplet
  per accepted cheque, so the SWAP path's marginal cost on the
  receiving side is hundreds of ms — that's why emission is gated to
  the existing settle path, not run on every push.
  **Status (May 2026):** code is correct (`cheques_emitted` > 0,
  `cheques_failed` ≈ 0 on real uploads), but no measurable throughput
  benefit at one-shot upload workloads. The mechanism that *would*
  pay off — bee's `notifyPaymentThresholdUpgrade` at
  `100 × refreshRate` per-peer cumulative — is unreachable when
  sessions die from kademlia bin pruning long before per-peer debt
  accumulates that high. See PERFORMANCE.md "SWAP / chequebook".
- **Connection-close cause diagnostic** (`diag::CONN_CLOSED_IO_DETAIL`,
  added May 2026). Captures `SwarmEvent::ConnectionClosed.cause`
  on every session death and buckets the underlying `io::Error`.
  Empirically on mainnet 100% are `errno 104 (ECONNRESET)`, attributable
  to bee's kademlia bin-prune path
  (`pkg/topology/kademlia/kademlia.go:719`) preferentially disconnecting
  peers with `ReachabilityStatusPublic != Public`. Mitigation:
  run via `daemon + --listen + --advertise` (see PERFORMANCE.md
  "Public reachability"). Default config is Private, gets pruned.
- **Per-stream + per-chunk latency histograms** (May 2026):
  `diag::PUSH_LATENCY_*` (do_pushsync wall-time buckets),
  `diag::OPEN_STREAM_*` (multistream-select + yamux open buckets),
  `diag::PUSH_OUTCOME_*` (ok / shallow / overdraft / error counts),
  `diag::CHUNK_LATENCY_*` (per-chunk total wall-time including
  retries and racing). All printed at upload end. Shape matches
  bee's `bee_pusher_sync_time` / `bee_pushsync_push_peer_time` /
  etc. Prometheus metrics so direct A/B comparisons are possible
  against a co-located bee node. See PERFORMANCE.md
  "Bee-vs-hoverfly end-to-end comparison" for the numbers.
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
  are by far the most common breakage (nectar v0.3.0 `MaybeSend` relaxes
  this for ChunkGet, but other paths like tokio::spawn still require Send).
- Network behaviour is empirical. If you change defaults or the constants
  above, measure against mainnet with a freshly randomised file (bee
  dedupes by chunk address: identical bytes re-upload in O(stamp) and tell
  you nothing about real throughput).
- The reference Bee implementation lives at
  `~/Coding/forks/bee/pkg/{pushsync,pusher,accounting,node,p2p,bzz,hive,topology}`.
  When in doubt about protocol semantics — pushsync receipts,
  accounting, pseudosettle wall-second rule, ghostBalance/blocklist
  windows, the v15 handshake / v2 hive wire format, kademlia
  saturation/prune behavior — read Bee directly; the upstream
  docs lag behind the code. Check out a specific tag
  (`git -C ~/Coding/forks/bee checkout v2.8.0`) when you need
  the actual code that's running on mainnet right now.
