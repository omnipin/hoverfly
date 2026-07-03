# PERFORMANCE.md

Cross-operation summary of every optimisation in this crate's history,
organised by code path. References are by **symbol/const name** (grep-able,
line-number-stable) rather than `file:line`. Empirical numbers are mainnet
observations on a residential Mac + a Fedora VPS + CI runners; treat them
as orders of magnitude, not guarantees — mainnet run-to-run variance at
the workloads we test is roughly 2-3×.

## What hoverfly is (for accounting purposes)

hoverfly is a **light node**: it stores no chunks, keeps no reserve, and
serves no pullsync. Functionally it is a client. But at the bee handshake it
deliberately advertises **`full_node = true`** (see the call-site comment in
`transport.rs`, `run_handshake` / `respond_to_handshake` — every caller
passes `true`). This is a measured throughput decision, not an accident:

- Advertising `light` (`full_node = false`) was tried **twice** and both
  regressed:
  - **Attempt 1** (full-node-sized accounting): pool filled in ~300 ms
    (bee skips the kademlia bin-saturation gate for lights) but collapsed
    within ~30 s — our outstanding per-peer balance overshot bee's **light
    disconnect limit (~1.6875M PLUR = 125% × the light payment threshold
    13.5M/10)** and peers blocklisted us for over-debt.
  - **Attempt 2** (matched light refresh `REFRESH_RATE_PLUR = 450K`,
    `SAFE_PEER_THRESHOLD_PLUR = 900K`): pool fills < 1 s, but throughput
    regressed 5-6× (30-40 KiB/s vs the ~194 KiB/s full-node baseline). The
    light per-peer credit rate (450K PLUR/s) is the binding constraint at
    our concurrency.
- **Trade-off:** light mode bypasses bee's bin-saturation gate (`Kad.Pick`
  short-circuits `true` for `!FullNode`) but accepts a **10× narrower
  accounting budget** (450K/s vs 4.5M/s per-peer refresh). For our
  high-concurrency upload workload the budget is the worse constraint, so we
  stay `full_node = true` and instead fight the bin-saturation gate with a
  vanity overlay (see "Vanity overlay").

We run no on-chain chequebook, so the v15 `chequebook_address` is always the
zero address. Bee's chequebook-verification gate only fires when a peer runs
`--chequebook-verification` **and** we advertise `full_node = true` — which
we do — but no mainnet peer enables that flag by default, so the zero
chequebook is accepted.

## Discovery

Discovery is hive-driven: dial a peer, accept its hive stream, listen for
broadcast peer announcements for `--wait` seconds, recurse for `--rounds`
hops. The slow knob is the per-peer wait, so wins come from parallelism
and peerlist hygiene.

- **Parallel per-round dialing.** `--discover-concurrency`
  (`DEFAULT_DISCOVER_CONCURRENCY = 16`) keeps N hive exchanges in flight at
  once. A 70-peer round at `--wait 30` finishes in `ceil(70/16) × 30 ≈ 150 s`
  instead of `70 × 30 = 2100 s`.
- **Recursive expansion.** `--rounds` (default 1, recommended 3-5 for upload
  workloads): each round seeds the next from the peers learned in the
  previous, broadening address-space coverage. 1 → 3 rounds typically
  triples-to-quadruples the peerstore; 5 rounds reaches several thousand.
- **Healthcheck dial probe** (`--healthcheck` + `--healthcheck-concurrency`,
  default 64). After discovery, dial each peer once and record the result in
  `peers.json`'s reachability cache (`peers.rs::Peer.last_dial_*`). Future
  ops then skip recently-failed peers up-front. The cache is monotonic
  (later successes overwrite older failures, never the reverse).
- **Underlay ingestion filter.** `PeerStore::upsert` drops non-`/ip4/`,
  non-`/tcp|/ws` multiaddrs at write time via `is_dialable_str` (same
  predicate as the transport). No DNS resolver in this crate, and
  residential networks rarely have outbound v6, so those entries would only
  burn dial timeouts later.

## Retrieval (fetch)

Per-chunk pull from the swarm. Each chunk needs a closest-peer dial +
substream open + content delivery. Bottleneck is per-chunk RTT and peerlist
coverage of the chunk's neighborhood.

- **Per-chunk peer racing.** `--concurrency` (`DEFAULT_FETCH_CONCURRENCY = 5`)
  races N peers in parallel per chunk; whoever returns a valid chunk first
  wins. Set to 1 for strict closest-first sequential behaviour.
- **Two-tier chunk cache.** In-memory `cache.rs::ChunkCache` (cloneable,
  shared by fetch and the daemon's inbound responder) plus, on wasm, a
  persistent IndexedDB L2 (`idb_chunk_store.rs`). Immutable content-addressed
  chunks survive reloads/sessions in the browser build.
- **Parallel mantaray walk.** `list_manifest` / `fetch_manifest_path` walk
  the manifest tree concurrently on native (`FuturesUnordered`); on wasm they
  walk sequentially because the ChunkGet future isn't Send-wrapped there.
- **Mutable feed / ENS resolution.** `client::resolve_feed_root` →
  `feed.rs`. If a fetched root is a feed manifest (root entry carries
  `swarm-feed-*` metadata), resolve the latest sequence update via a
  concurrent exponential-probe + k-ary search, then fetch the content it
  points at. This is how feed-backed ENS sites resolve.
- **`--max-retries 0` = uncapped on fetch.** Marches through every peer in
  the proximity-sorted candidate list until one yields the chunk.
- **Reachability-cache priming.** `RECENT_FAILURE_SECS = 300` deprioritises
  peers that failed recently in any prior operation.

## Upload (pushsync)

The most code in the repo: pushsync is RTT-bound, accounting-mediated, and
adversarial (bee blocklists overlays that overdraw their ghost balance).
Wins come from session reuse, parallelism, accounting discipline, and
graceful failure handling.

### Session model

- **Persistent peer sessions** (`transport.rs::PeerSession`). One libp2p
  connection per peer, kept open across many chunks; each pushsync opens a
  fresh yamux substream over the existing connection. Avoids the ~150-300 ms
  per-chunk dial-handshake-pricing setup cost.
- **Concurrent pushes per session.** Each `PeerSession` has a single
  swarm-driver task that pulls commands from a channel and spawns each push
  into a `FuturesUnordered`. Accounting and pseudosettle are serialised via a
  `tokio::sync::Mutex` on `SessionState`; accounting locks are released
  before any network IO.
- **Concurrent substream opens** (`protocols::stream_pool`, vendored+patched
  `libp2p_stream`). Upstream serialises outbound substream upgrades behind a
  singular `pending_upgrade` slot; our `Handler` keys them in a
  `HashMap<UpgradeId, …>` so many upgrades are in flight at once. Cap:
  `DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES = 64` (`--substream-upgrade-cap`).
  This alone moved a 222-peer residential run from ~75 → ~133 KiB/s.
- **Session pool** (`client.rs::SessionPool`). `--concurrency`
  (`DEFAULT_UPLOAD_CONCURRENCY = 8` one-shot; daemon `--pool-size` default 256,
  the throughput operating point) controls target pool size. The per-chunk dispatcher picks
  the closest-by-proximity session for routing.
- **Address-space spread** (`client.rs::spread_across_address_space`). Pool
  candidates are bucketed by leading overlay byte (256 bins) and
  round-robined into the dial queue, so the live pool covers proximity bins
  evenly instead of clustering near `0x00`.
- **Wide dial-fill window.** `SESSION_DIAL_PARALLELISM = 128` keeps that many
  dials in flight while filling the pool, **decoupled** from target pool size
  — mainnet peerlists are ~50% stale and bee RSTs most random-overlay dials,
  so the search must be far wider than the number of sessions we keep.
- **Pre-warmed rotation** (`SessionPool` prewarm path). Two triggers:
  1. **Ghost-balance**: when a session crosses
     `GHOST_BALANCE_LIMIT_PLUR × GHOST_BALANCE_PREWARM_NUMERATOR /
     GHOST_BALANCE_PREWARM_DENOMINATOR` (= **1/2** of the limit), a
     replacement is dialed in the background while the current session still
     serves pushes.
  2. **Dead-session**: when a session's driver task has exited
     (`PeerSession::is_alive() == false`) and the entry has no accumulated
     dial-failure strikes. This catches the empirically dominant retirement
     cause — the connection dying for non-accounting reasons (bee bin-prune
     RST, NAT keepalive expiry, yamux idle timeout) long before ghost balance
     approaches its watermark.
  When the active session retires, the triggering chunk finds a pre-dialed
  replacement at `take_pending()` instead of paying the synchronous dial
  cost. The dispatcher sweeps for prewarm candidates after every chunk
  completion (ok OR err) and on a heartbeat.
- **Session-retirement diagnostic counters** (`transport::diag`). Atomics
  distinguish causes: `dead_low_ghost`, `dead_prewarm_ghost`,
  `dead_high_ghost`, `ghost_threshold`, `max_pushes`, plus `prewarm_on_dead`
  / `prewarm_on_ghost`. Printed at upload end. Empirically at high
  concurrency ~100% of retirements are `dead_low_ghost` — the connection dies
  externally; bee rarely blocklists us at the accounting layer.
- **Split timeouts.** `--timeout` (per-substream, default 10 s) ≠
  `--dial-timeout` (whole session open, default 3 s).
- **Daemon mode** (`daemon.rs`, `#[cfg(unix)]`). Long-running unix-socket
  daemon owns a warm `SessionPool`, reused across uploads. It does an **eager
  pool fill on startup** (background task, not lazy on first upload), so the
  pool-fill cost is paid once and the first request is fast too. A background
  **maintenance loop** (fast 1-s tick, `HOVERFLY_MAINTENANCE_SECS`) prunes
  dead entries and trickles up to `pool_target / 8` fresh dials per tick. The
  fast+spread cadence out-paces bee's continuous RSTs and *desynchronises*
  connection deaths, holding a big pool near target (measured: ~95-105 live at
  target 137 vs 2-6 under the old 5-min tick); the per-peer parking limiter +
  fresh-peer (distinct-node) selection keep it under bee's per-node dial limit.
  Connections stay via a decoupled long `idle_timeout` (600 s) so the swarm
  doesn't self-close warm, substream-idle connections. `--pool-size` default
  is 256 (the throughput operating point).

### Per-chunk dispatch

- **3-bucket storage-radius-aware sort.** Each chunk's dispatcher ranks pool
  entries into three buckets, then by descending PO to the chunk address
  within each bucket:

  | Bucket | Entry classification |
  |---|---|
  | 0 (front) | confirmed in-AOR storer (`storage_radius ≤ chunk_PO`) |
  | 1 (middle) | unknown (no observation yet — fresh session) |
  | 2 (back) | confirmed forwarder for this PO range |

  An earlier version of this 3-bucket design regressed ~1.6× because
  "unknown" was contaminated with slow/NAT'd/dead peers. The current stack
  pre-filters those (`is_dead`, cooldown filter, `inflight_cap`) so "unknown"
  is a clean pool of fresh sessions worth trying ahead of known forwarders.
  Measured +28% median on a pool=256 setup (827 → 1055 KiB/s).
- **In-flight buffer.** `buffer = (128 * mult).min(total).max(pool.len())`,
  where `mult` is `--buffer-multiplier` / `HOVERFLY_BUFFER_MULT` (default 1).
  128 matches bee's pusher `ConcurrentPushes = swarm.Branches = 128`. An
  earlier `pool × 16` buffer was measured to collapse throughput (6 chunks/s
  → 0.1 chunks/s) because mass-concurrent reserves piled up on the
  per-session accounting mutex; 128 is the empirical knee. Scale it *with*
  pool (not alone) via `--buffer-multiplier`.
- **Per-chunk peer racing.** `CHUNK_PEER_PARALLELISM = 3`. Each chunk
  dispatches to the top-3 closest peers concurrently; the first valid receipt
  wins, losers finish their accounting silently. Collapses per-chunk RTT from
  N serial peer-walks to ~one peer's RTT for most chunks. Wire cost is ~3×
  pushes/chunk, but bee credits all 3 hops, so it pays for ~2-3× throughput.
  `PREEMPT_INTERVAL = 1s` tops up the race window when the initial seed used
  fewer than 3 peers (small/attrited pool) or after an early shallow/error
  reply — short enough to race on per-chunk RTT timescales.
- **Shallow-receipt retry.** `pushsync::PushReceipt::is_shallow`: a receipt
  signed by a peer outside the chunk's AOR proves a forwarding hop, not
  durable storage. The dispatcher retries the next-closest peer; after the
  candidate list is exhausted with only shallow + overdraft outcomes, it
  accepts the deepest-PO shallow receipt rather than aborting (bee's pushsync
  takes the same `maxPushErrors`/`errSkip` way out).
- **Overdraft refresh fallback.** If every candidate within `cap` returns
  Overdraft (no real errors), the dispatcher sleeps ~1.1 s (one bee
  `refreshRate` window) and retries the closest-N peers, since pseudosettle
  has had a chance to refresh credit.
- **Per-peer in-flight cap** (`IN_FLIGHT_CAP = 4`, latency-aware). Each pool
  entry tracks live concurrent pushes via an atomic counter; the dispatcher's
  `order` filter excludes entries at cap, forcing fan-out to lower-PO peers.
  The cap is **latency-aware** (`SessionEntry::inflight_cap()`): fast peers
  (push-latency EWMA < 200 ms) get `2 × IN_FLIGHT_CAP = 8`, medium get the
  base 4, slow (EWMA ≥ 2 s) get `IN_FLIGHT_CAP / 2 = 2`, unknown/fresh get
  the base 4. Uniform `cap = 8` regressed (590 vs 665 at cap=4) due to yamux
  substream contention per session; latency-awareness gives fast peers the
  wider cap without inducing that contention on slow ones.

  **Why the cap is the dominant throughput lever** (mid-2026, 64-peer pool,
  residential VPS, multi-target vanity overlay):

  | Config | Median KiB/s | Best |
  |---|---:|---:|
  | No cap (buffer=128, 3-way race) | 194 | 282 |
  | **`IN_FLIGHT_CAP = 4`** | **515** | **557** |

  Mechanism: without the cap, the dispatcher stacks 5-7 concurrent pushes on
  the top-PO peers. At ~6.75K PLUR/chunk and ~60 ms median latency that's
  ~675K PLUR/s of per-peer debt — above even bee's full-node refresh rate
  (4.5M/s is per-peer, so a single saturated peer overdraws). Bee returns
  `ErrOverdraft` → dispatcher rotates → throughput drops to
  refresh-rate-per-saturated-peer. Bee-light avoids this naturally because
  its kademlia routes each chunk to one AOR neighbour; the in-flight cap is a
  cheap approximation that forces wider fan-out. After the change,
  `PUSH_OUTCOME_OVERDRAFT` went from 18% of pushes to ~0.

  **Cap composes with pool+buffer scaling** (each commit-and-measured):

  | Stack | Median KiB/s | Best |
  |---|---:|---:|
  | uniform `cap=4`, pool=64 | 515 | 557 |
  | + `--pool-size 128 --buffer-multiplier 2` | 665 | 954 |
  | + latency-aware `inflight_cap()` + `--pool-size 256` | 827 | 1106 |
  | + 3-bucket AOR sort | **1055** | **1075** |

### Accounting + connection lifecycle

- **Client-side ghost balance mirror** (`GHOST_BALANCE_LIMIT_PLUR =
  12_000_000`). Mirrors bee's per-overlay `ghostBalance` disconnect threshold
  (~16.875M PLUR) with headroom for in-flight pushes. When our mirror crosses
  the limit, the session flips `accept_new = false` and retires gracefully;
  the replacement's `Connect()` resets bee's ghostBalance for us.
- **Pseudosettle wall-second rule.** Bee rejects two pseudosettles within the
  same wall-second; sessions serialise settles via `settle_lock` and gate
  them on `last_settle.elapsed() >= ~1.1 s`. Auto-settle triggers at one
  `REFRESH_RATE_PLUR` (4.5M PLUR) of accrued balance.
- **Narrow `SAFE_PEER_THRESHOLD_PLUR`** (`= REFRESH_RATE_PLUR × 2` = 9M
  PLUR). The peer's announced threshold is larger, but using it directly
  produced thundering-herd contention on the accounting mutex (one
  experiment: overdrafts on 50 MiB shot 1.6k → 51k). The narrower cap forces
  more frequent pseudosettles but keeps the dispatch queue from piling up.
- **Per-session pushes ceiling** (`MAX_PUSHES_PER_SESSION = 10_000`).
  Defence-in-depth; ghost balance retires sessions long before this fires.
- **Dead-peer marking + skip window.** A session whose rotation dial fails
  accumulates strikes; after `DEAD_STRIKES = 3` the entry parks for
  `DEAD_SKIP_SECS = 15` (`SessionEntry::mark_dead`). During parking the
  dispatcher skips it in proximity ordering, so we stop hammering bee with
  redials it will only RST.
- **Outer pusher-layer retry.** `MAX_CHUNK_RETRIES = 60` with a **flat 500 ms
  backoff** (≈30 s total in the common case) — sized to outlast
  `DEAD_SKIP_SECS` and bee's ghost-overdraw blocklist window, so a chunk
  whose whole alive pool transiently collapsed waits for revival instead of
  aborting the upload. Mirrors bee's `pusher.DefaultRetryCount` philosophy.
  Independent of the `--max-retries` CLI flag (which is the per-attempt peer
  candidate cap, silently promoted from 0→1 and capped by live pool size).
- **Timeouts do not retire sessions.** `is_connection_dead` deliberately
  excludes `TransportError::Timeout`. A single slow substream errors that
  chunk back to the dispatcher (which advances to the next peer) but leaves
  the session alive for dozens of other in-flight pushes. Treating timeouts
  as dead-connection signals at high `--concurrency` cascade-retired most of
  the pool. Ghost-balance still increments on timeouts, so a session that
  keeps timing out retires naturally via the threshold.

### Stamping

- **Parallel postage stamp signing on native** (`nectar-postage` + rayon).
  `stamp_chunks_parallel` uses every core. On wasm the upload path stamps
  **sequentially and must not init a rayon pool** — nectar's split hashes
  into a `parking_lot::RwLock`-guarded store, and on wasm `parking_lot`
  cannot park a thread, so a contended write panics; with no pool,
  `into_par_iter` runs inline. The wasm stamp path also uses
  `issuer.prepare_stamp` + `sign_message_sync` + `js_sys::Date` timestamps
  (never `SystemTime::now`, which panics on wasm).

## Shared infrastructure

- **Transport.** Native uses `or_transport` to serve both plain TCP and
  TCP-over-WebSocket from one libp2p stack; libp2p picks the inner transport
  from the multiaddr. WASM is WebSocket-only (via vendored `src/wsws`).
- **Active identify-push during connection setup** (`transport.rs::
  prep_connection`). One of the largest per-session savings in the crate:
  1. libp2p connection established; bee sends its identify.
  2. We extract the `observed_addr` (our externally-visible underlay) and
     `swarm.add_external_address()` it.
  3. We immediately `identify.push([peer_id])` — proactively re-sending our
     identify with the correct external address — instead of waiting for
     libp2p's periodic identify interval.
  4. We wait for the `Pushed` event before letting the handshake run.

  Without step 3, bee sits idle waiting for our liveness signal for the
  default ~7-10 s libp2p identify interval — every session pays that. With
  the push, bee proceeds one RTT (~50-300 ms) after ack. Across a 128-session
  fill this is the difference between "ready in ~5 s" and "ready in ~15 min".
  The code comment there calls this "the magic that makes bee proceed
  immediately" — preserve the mechanism on any connection-setup refactor.
- **Reachability log + writeback** (`peers.rs::ReachabilityLog`, `apply_log`).
  Every operation collects dial outcomes; on completion the CLI writes them
  back to `peers.json`. Next run starts faster.

## Empirical ceilings

Measurements, not theoretical floors. They move with code and peerlist.

| Configuration | Throughput (mainnet, novel random content) |
|---|---|
| Residential Mac, racing-only, ~222-peer peerlist | ~75 KiB/s |
| Residential Mac, racing + stream_pool patch, 222 peers | ~133 KiB/s |
| VPS, racing-only, 340 peers | ~152 KiB/s |
| VPS, racing + stream_pool patch + 3335 peers (5-round discover) | ~335 KiB/s |
| VPS, daemon + vanity overlay, pool=64 | ~194 KiB/s median, 282 best |
| VPS, + **per-peer in-flight cap** (`IN_FLIGHT_CAP=4`), pool=64 | ~515 median, 557 best |
| VPS, + **`--pool-size 128 --buffer-multiplier 2`** | ~665 median, 954 best |
| VPS, + **latency-aware cap** + `--pool-size 256` | ~827 median, 1106 best |
| VPS, + **3-bucket storage-radius sort** | **~1055 median, 1075 best** — last pre-bee-2.8 |

**Post-bee-2.8 (May 2026).** After the network-wide upgrade (see "Bee 2.8.0
protocol migration"), the comparable VPS number dropped to ~324 KiB/s median
(high variance, peak 525) — partly addressbook eviction that happened during
the v14-only window before we shipped v15 + ping + timestamp cache. CI
runners, by contrast, jumped from ~50-200 to ~400-500 KiB/s because the
broken v14 fallback path was the thing holding them back.

| Configuration | Throughput |
|---|---|
| VPS, v15 + cache + dnsaddr bootnode + 794-IP seed | ~324 median, 525 best |
| **GitHub Actions** runner, same config | **~473 median, 527 best** |
| CircleCI runner, same config | ~376 median, 462 best |

Framing: **throughput is gated by (1) peerlist coverage of the chunk address
space, (2) the rate we can negotiate fresh substream upgrades per connection,
and (3) bee mainnet's forwarding latency for chunks bee doesn't have cached**
— roughly that order. The first two are addressable here; the third is
mainnet's reality. The old "~150 KiB/s protocol floor" claim was wrong — it
was one measurement at one configuration.

### Recommended config (mid-2026)

Daemon mode on a VPS with a sustained upload workload:

    hoverfly \
      --nonce-file overlay-nonce \
      --buffer-multiplier 2 \
      daemon \
      --socket /tmp/hoverfly.sock \
      --pool-size 256 \
      --listen /ip4/0.0.0.0/tcp/1635 \
      --identity 0xYOUR_KEY \
      --advertise /ip4/YOUR_PUBLIC_IP/tcp/1635

Key knobs:
- **`--pool-size 256`** widens the candidate pool so the latency-aware cap can
  pick fast peers. The `--pool-size` default of 16 is a minimal
  works-anywhere floor, not a target — with a random overlay and no public
  reachability, bee RSTs most sessions anyway (see "Session-death cause"), so
  a large default would just churn. 256 is worthwhile only *together with* a
  vanity overlay + `--listen`/`--advertise`.
- **`--buffer-multiplier 2`** doubles in-flight chunks (128 → 256), scaled
  with the pool so per-session in-flight stays ~constant.
- **Stable + vanity overlay + `--listen`/`--advertise`** are prerequisites —
  without them bee prunes us as a non-public, bin-saturated peer regardless
  of how many sessions we open.

Cold-start: pool=256 fills in ~80 s vs ~30 s for pool=128 on our VPS, but the
eager fill runs in the background before the first upload, so it's invisible
unless you start the daemon and upload immediately.

### Steady-state connection count

With the daemon at rest, open connections = `pool_target` (one libp2p
connection per peer). At the default `--pool-size 16` that's up to 16; at the
recommended 256, up to 256. The *live* count is usually lower than the target
because bee RSTs non-citizen sessions continuously (below); the maintenance
loop tops the pool back up each tick. For contrast, a bee node maintains ~131
kademlia neighbours as a structural obligation of full participation — a
different quantity from our upload fan-out working set.

## Known limits (not bugs)

- **Bee mainnet forwarding RTT** is the per-chunk floor (~200-800 ms/hop,
  more on long PO chains).
- **Bee dedup masks performance.** Re-uploading the same bytes hits bee's
  "is within AOR" short-circuit without forwarding. Always test with freshly
  randomised content.
- **Peerlist coverage of the address space** is the dominant single lever for
  random-content uploads. A chunk whose address lands in a PO bin with <5
  peers in your store stalls no matter how many sessions you open. `--rounds
  3` → `--rounds 5` (≈340 → ≈3300 peers) more than doubled throughput.
- **Pool + buffer scale together, not separately.** A fixed-pool buffer sweep
  regressed (pool=128, buffer 1→2→4→8 went 20→24→34→65 s) because each doubles
  per-session load. Scaling *both* keeps per-session in-flight ~constant while
  total grows. 50 MiB VPS sweep, `upload --raw`:

  | Configuration | Time | Throughput |
  |---|---:|---:|
  | `--concurrency 128` (mult=1) | 138 s | 380 KiB/s |
  | `--concurrency 256` (mult=1) | 91 s | 576 KiB/s |
  | `--concurrency 256 --buffer-multiplier 2` | 76 s | 690 KiB/s |
  | `--concurrency 512 --buffer-multiplier 4` (run 1) | 65 s | 807 KiB/s |
  | **`--concurrency 512 --buffer-multiplier 4` (run 2)** | **49 s** | **1070 KiB/s** |
  | `--concurrency 768 --buffer-multiplier 6` | 126 s | 416 KiB/s |
  | `--concurrency 1024 --buffer-multiplier 8` | 122 s | 430 KiB/s |

  Sweet spot: `--concurrency 512 --buffer-multiplier 4` (~3 in-flight/session
  at race=3, 1536 total). Beyond it, per-session yamux contention dominates.

## `--substream-upgrade-cap` sweep (single-trial)

3335-peer VPS, fresh 5-MiB tar per iteration, `--concurrency 128
--max-retries 128 --timeout 30`:

| cap | time | throughput |
|---:|---:|---:|
| 8 | 56.7 s | 92 KiB/s |
| 16 | 34.7 s | 151 KiB/s |
| 32 | 20.9 s | 246 KiB/s |
| 64 | 46.0 s | 111 KiB/s |
| 96 | 28.4 s | 180 KiB/s |
| 128 | 25.0 s | 205 KiB/s |

Non-monotonic shape (64 worse than 32 and 96) = single-trial variance (~2×)
is comparable to the effect. Honest reading: cap ≤ 16 is genuinely too low;
cap ≥ 32 all sit in the same plausible range. Default 64 is kept for prior
validation + headroom + the asymmetric cost of being too low (cap=8 → 57 s vs
cap=128 → 25 s). For a real verdict, run 5+ interleaved trials per cap.

## Multi-connection-per-peer sweep (negative result)

`HOVERFLY_CONNECTIONS_PER_PEER` tested whether multiple yamux pipes per peer
relieve per-connection contention. VPS, 5 MiB random tar, 3335 peers,
`--concurrency 128`, `BUFFER_MULT × CONNECTIONS_PER_PEER`:

|              | conn=1 | conn=2 | conn=4 |
|---:|---:|---:|---:|
| **buf=1** | 16 s | 20 s | 30 s |
| **buf=2** | 15 s | 48 s | 21 s |
| **buf=4** | 15 s | 28 s | 28 s |
| **buf=8** | 34 s | 32 s | 17 s |

The mechanism works where yamux contention is induced (buf=8 row improves
34→32→17 with more conns), but no cell beats baseline `buf=1 conn=1` at
15-16 s. Multi-conn helps only the configurations that aren't fast anyway.
Both env knobs stay as investigator tools, not defaults.

## Multi-worker upload (built, validated, removed)

N worker subprocesses, each with its own ephemeral overlay, coordinated over
Unix sockets. Hypothesis: bee's per-overlay accounting was the cap.

50 MiB VPS, before the buffer-multiplier fix:

| Configuration | Time | Throughput |
|---|---:|---:|
| Single-process `--concurrency 128` | 116 s | 450 KiB/s |
| `upload-parallel --workers 2 --concurrency 128` | 257 s | 204 KiB/s |
| `upload-parallel --workers 4 --concurrency 128` | 161 s | 326 KiB/s |
| `upload-parallel --workers 8 --concurrency 128` | 89 s | 590 KiB/s |

After the buffer-multiplier fix:

| Configuration | Time | Throughput |
|---|---:|---:|
| Single-process `--concurrency 512 --buffer-multiplier 4` | 49 s | **1070 KiB/s** |
| `upload-parallel --workers 2 --concurrency 512 --buffer-multiplier 4` | 234 s | 224 KiB/s |

The combination regresses: both knobs target the same parallelism, and 2×512
sessions from one IP competes with itself for ephemeral ports and bee's
per-IP limits. **Lesson:** the wall wasn't bee's per-overlay accounting — it
was how much parallelism one libp2p stack extracts from one source IP. Once
that was unlocked via pool+buffer scaling, multi-worker added nothing. The
~900-LOC coordinator/worker path was removed.

## JIT-AOR sessions (tried, falsified, removed)

Plan address-space-targeted dials for peers that look like in-AOR storers for
specific chunks and run them in parallel with pushes, appending successful
dials to the live pool. Mechanism worked end-to-end but produced a wash:
OFF median 154.5 KiB/s vs ON 148.9 (3.6% regression, within variance) at
`--concurrency 128`. At today's mainnet it adds only ~15-20 sessions (128 →
~145), and the bottleneck is forwarding RTT, not pool coverage — a JIT
session often lands after the chunk already dispatched elsewhere. Planner,
dialer, `--aor-budget`, and `SessionPool::extend_one` removed. The
receipt-driven storage-radius routing (`in_aor()`, bucket sort) stays — only
the JIT *seeding* of `storage_radius` was tied to the removed dialer.

**Peerlist freshness (the real side-lever).** Running `discover --rounds 3`
and immediately uploading recovers ~1 MB/s at `--concurrency 128` on a
snapshot where an hours-old healthchecked peerlist gives ~180 KiB/s. The
`peers.json` reachability cache ages out slower than peers actually recover,
so the dispatcher locks out peers that are alive again. Guidance: for one-shot
uploads, discover-then-upload in the same shell minute. Daemon mode is
unaffected (it keeps live sessions). Open follow-ups: shorten
`RECENT_FAILURE_SECS` (currently 300; <60 might already help) or halve the
lockout for peers that only ever failed once; probe-on-fill retry of
recently-failed peers.

## Single-swarm collapse (built, measured, regressed ~100%, removed)

Hypothesis (from `nxm-rs/vertex`, which runs one swarm): collapse the
per-peer `Swarm` (`PeerSession` = one whole swarm + one `SessionDriver` each,
N swarms at pool 256-512) into **one shared swarm** owned by a single driver
task, with `PeerConn` handles opening substreams to any peer via the vendored
`stream_pool::Control`. Motivation: FD count ≈ `connections + 1` instead of
per-swarm overhead ×N, one identify/ping/transport stack, one task.

Built it end-to-end (`SwarmDriver` owns the swarm; does dial + the identify
"magic" + close detection + inbound-stream routing by `PeerId`; work runs in
caller tasks via a cloned `Control`). Connect + pool-fill work great: **64/64
sessions fill in <1 s**. Fixed a chain of real bugs found along the way —
dial-cancellation on the shared swarm (`PeerCondition::Always` + per-peer
dedup), a 131%-CPU driver busy-loop (removing `biased` from the `select!`),
phantom-redial hangs (disabling `stream_pool`'s internal auto-dial), and stale
liveness (`is_alive` reading the swarm's real connection set instead of a
driver-set flag that lags under mass churn).

**It still regresses to zero throughput.** Back-to-back A/B on the *same*
peers / overlay / churn, non-daemon `upload peers.seed.json` (357 chunks,
`--concurrency 64`):

| Path | Result |
|---|---|
| Per-session (old) | **357/357 ok, upload completes** (1.4 MB, ok=357 error=0) |
| Single-swarm (new) | **0/357** — pool collapses to ~2 live TCP; every `open_stream` fails `Canceled` / `Disconnected` |

**Root cause (architectural, not a bug):** one shared swarm + one driver +
the `stream_pool::Control` indirection cannot service ~64 connections ×
hundreds of concurrent substream opens (buffer 128 × race 3 ≈ 384 in flight).
Each `open_stream` hops a channel to the behaviour and only advances when the
single driver next polls the swarm; under load, opens get cancelled and
connections tear down. The per-session model gives every connection a
**dedicated poller** — that turns out to be essential parallelism for this
high-concurrency push workload, not just overhead. The FD/memory win is real
but nets a throughput loss to zero.

**Lesson:** don't bolt one swarm under the rotation pool. If single-swarm is
ever revisited, it needs vertex's hand-rolled multiplexing `ClientHandler`
(the behaviour *is* the poll loop, no `Control` channel indirection), which is
a much larger rewrite — not the assumed "`stream_pool` already gets the win".
The parking dial limiter developed alongside this (`src/ratelimit.rs`,
`DialRateLimiter`) is independent and stays.

## SWAP / chequebook (built, tested, no measured benefit)

Hypothesis (from Swarm infra): uploads 3× faster when paying peers in BZZ via
SWAP cheques vs relying on time-based pseudosettle. Scope: **issuance only**
(no deploy, no cashout, no on-chain RPC). Activation via `--chequebook`
(whose `issuer()` must equal `--key`'s eth address),
`--chequebook-per-peer-cap-bzz`, `--cheques-file`.

Interleaved A/B (May 2026 mainnet, VPS, 771-peer pool, c=64, 5 MiB, 4 trials):

| Mode | Throughputs (KiB/s) | Median |
|---|---|---:|
| Unpaid | 97, 290, 195, 290 | 195 |
| Paid | 282, 82, 80, 238 | 160 |

Distributions overlap completely (~3× trial variance). **Verdict: not
confirmed at this workload.** Cheques are correct (`cheques_emitted` > 0,
`cheques_failed` ≈ 0). Reading bee's accounting (`notifyPaymentThresholdUpgrade`),
paying a peer only raises its threshold once cumulative settled debt crosses a
**checkpoint**, and the first checkpoint is `refreshRate × linearCheckpointNumber
= 4.5M × 1800 ≈ 8.1B PLUR` per peer (then linear `+refreshRate` per
`refreshRate × 100` step, going exponential past the linear limit). Our per-peer
cumulative reaches only a few million before the session dies (external causes)
— roughly **three orders of magnitude short of even the first checkpoint**. The
3× claim is consistent with **long-lived daemon sessions** accumulating 100s of
MiB per peer over hours, not one-shot uploads. Code stays: it's correct, and the
retrieval path debits us via the same accounting, so a long-running fetch worker
would benefit identically.
Re-measure in the daemon + public-reachability + multi-GB regime.

## Session-death cause (RST analysis)

`CONN_CLOSED_IO_DETAIL` classifies every `SwarmEvent::ConnectionClosed` by
`cause`. On mainnet at c=64-128: **~100% of session deaths are TCP
`ECONNRESET` (errno 104)** — zero clean closes, zero keepalive timeouts.

**Important:** this does NOT by itself prove bee is "abusively" terminating
us. bee's libp2p-go TCP listener sets `SO_LINGER=0`, so **every** libp2p-go
close becomes a TCP RST regardless of reason — clean close, accounting
blocklist, kademlia bin prune, NAT keepalive all surface as ECONNRESET. What
we *can* attribute: `keepalive=0` rules out idle timeout, `clean=0` rules out
us closing first, `yamux:closed` minority rules out yamux fatals. The
dominant path is therefore a bee-side `Disconnect`. bee's kademlia prune
(`pkg/topology/kademlia`) selects targets in priority: (1) **Unhealthy**
(failed `pkg/salud` probe), (2) **Non-Public** reachability, (3) random. So
non-public and unhealthy peers are disproportionately dropped. We mitigate
(2) with `daemon --listen --advertise` (below, 1.74×); we can't reliably fix
(1) (kademlia-membership chicken-and-egg, below).

## Public reachability (`--listen` + `--advertise`)

Be `Public`-reachable to bee. Requires daemon mode: bee's reacher pings us
back on the advertised underlay and only marks us Public if the ping
succeeds. There's no advertise-without-listen mode that works.

5 trials each, c=64, 5 MiB, 771-peer peerlist:

| Configuration | Throughputs (KiB/s) | Median |
|---|---|---:|
| One-shot baseline | 96, 118, 115, 93, 118 | 115 |
| Daemon, no `--listen` | 170, 54, 79, 42, 88 | 79 |
| Daemon + `--listen` + `--advertise` | 158, 208, 188, 220, 200 | **200** |

**Daemon with public reachability is 1.74× the one-shot median with the
tightest distribution.** Daemon *without* listen is *worse* than one-shot
(79 vs 115): between uploads the persistent pool sits idle and sessions die
to the same RST; the next upload pays dead-session detection + re-dial that a
fresh one-shot skips. The background maintenance loop (now implemented,
`daemon.rs::maintain_pool`) is the countermeasure.

## Status protocol responder (`/swarm/status/1.1.3/status`)

Hypothesis: bee's `pkg/salud` probes connected peers; non-responders are
marked Unhealthy → prune-target #1. Serving it should reduce our prune rate.
Implementation: `proto/status.proto`, `protocols/status.rs`, inbound-only,
plumbed through both outbound sessions and the daemon `--listen` listener.

Result: **no measurable improvement.** Reason (the chicken-and-egg): bee's
inbound path calls `notifier.Connected(..., forceConnection=false)`; if our
PO bin is at `OverSaturationPeers = 18`, kademlia returns `ErrOversaturated`
and we're **not added to `connectedPeers`** — still libp2p-connected but
kademlia-invisible. `pkg/salud` only iterates `EachConnectedPeer`, so it
**never probes a kademlia-invisible peer**. Confirmed by zero "status
responded" log entries across ~1000 connected peers. The code stays as
long-term defense for if we ever become kademlia-visible (overlay rotation,
or a bee whose bin has room), but it can't fix present-day throughput.

## Where hoverfly loses time vs a bee node

We're slower end-to-end than a co-located bee (measured multiples vary with
the mainnet snapshot; the *shape* of the gap is stable). Note that bee's HTTP
API reports a **deferred-upload** "uploaded" time that returns ~22× before
chunks are actually retrievable — always compare on a durable-receipt basis
(e.g. cross-verify via `bzz.limo`), which is what hoverfly waits for anyway.
Where the gap lives (instrumented, same histogram buckets as bee's Prometheus
metrics):

Per-stream RTT: our **median is faster** than bee's, but our tail is ~30×
worse (~22% of pushes take 2-5 s vs bee's 0.05%). Per-chunk mean ~6 s vs
bee's ~452 ms (racing helps median — 54% land <500 ms — but the tail
dominates the mean). Push outcomes pre-`IN_FLIGHT_CAP`: our overdraft 18% vs
bee 11%; **the cap dropped our overdraft to ~0**, confirming the diagnosis
(per-peer accounting throttling) was right but the fix was on our side
(limit concurrent debits per peer), not bee's.

**The one-sentence why:** even a *light* bee (no storage, pushes everything
like us) wins — so the gap isn't local storage. It's that bee holds **~131
stable kademlia neighbours** that treat it as a Public full-citizen with warm
accounting state and connections that survive hours, while our pool churns
(peers RST us within seconds as a non-citizen). bee also spreads load across
those neighbours naturally (kademlia AOR routing ≈ 1.5 pushes/s each); our
top-K-closest dispatch stacked pushes on top peers until `IN_FLIGHT_CAP=4`
forced the wider fan-out that closes most of the gap without implementing
kademlia.

## Bee-citizenship: stable overlay + hive self-announce (built, tested)

Get into bee's kademlia over time by mimicking a real participant:

- **Stable overlay across restarts.** Overlay is `keccak256(eth_addr ||
  network_id || nonce)`; the nonce is persisted via `--nonce-file` (default
  `overlay-nonce`; `signer::from_bytes_with_nonce`), so restarts look like the
  same peer instead of a new one.
- **Outbound hive self-announce on every session connect.** Send a `Peers`
  envelope with our own BzzAddress; bee's `peersHandler` reads, dial-probes
  (reacher ping to our `--advertise` underlay), and adds us to `knownPeers`.
  bee's manage loop may then dial us OUTBOUND with `forceConnection=true`,
  bypassing the bin-saturation gate (`protocols::hive::announce_self`,
  `transport::do_hive_announce`).

**Single-upload throughput is unchanged** (~160 announces/upload fire
successfully, ~0 failures, but 0 inbound connections from new peers over
5-min idle windows). It's a slow-burn lever — bees learn about us and may
dial back hours later; the code stays because it's correct, doesn't regress,
and is the prerequisite for long-term kademlia growth. Validate it by leaving
a daemon running 6+ hours before measuring.

We previously also served an inbound **pullsync** responder (empty cursors /
offers) — the earliest reciprocal signal bee ever gave us (6 cursor probes in
15 min idle). Dropped it: every empty offer immediately triggered another
probe (bee's puller has no backpressure on empty offers), a tight poll loop
with no benefit since we store no chunks. Honest `UnsupportedProtocol` is
better. Would return with a real reserve if chunk storage is ever added.

## Bee 2.8.0 protocol migration (May 2026)

Bee 2.8.0 was a hard network-wide cutover — v2.7 and v2.8 nodes can't
handshake or gossip with each other. Three changes hit us:

1. **`/swarm/handshake/14.0.0` → `15.0.0`.** `BzzAddress` gains two signed
   fields: `timestamp` (int64 s since epoch) and `chequebook_address` (20
   bytes, zero for us). Sign payload becomes `"bee-handshake-" || underlay ||
   overlay || network_id_BE_8 || nonce || timestamp_BE_8 ||
   chequebook_address`. bee's `CheckTimestamp` rejects records >
   `MaxClockSkew = 60 s` in the future, and (gossip path) records whose
   timestamp doesn't advance by ≥ `MinimumUpdateInterval = 300 s`.
2. **`/swarm/hive/1.1.0` → `2.0.0`.** Same `BzzAddress` shape; receivers
   validate the signature on each gossiped record.
3. **`/ipfs/ping/1.0.0` is now load-bearing.** bee's reacher pings our
   advertised underlay; a failed ping marks us `Private`, making us the top
   target for the 5-minute prune sweep.

What we shipped: proto fields on `BzzAddress` (proto3 zero-defaults keep v14
wire compat); `signer::generate_sign_data_v15` (byte-for-byte bee payload),
`sign_handshake_v15`, and `sign_handshake_v15_cached`; `Version` enums on
handshake/hive with outbound v15/v2-first-then-fallback and inbound both-in-
parallel; `libp2p::ping::Behaviour`; a fresh IP-diverse `peers.seed.json`.

**The cached-signature detail matters.** `sign_handshake_v15_cached` returns
the same `(timestamp, signature)` per `(underlay, chequebook)` so reconnects
replay an **identical** record. Without it, our minute-scale session rotation
re-handshakes the same peer with a fresh timestamp every time; bee's gossip
path rejects updates within `MinimumUpdateInterval`, so the network never
refreshes its view of us and our overlay ages out of addressbooks. The cache
is per-process (daemon restart re-signs once with a newer `now_unix()`, which
is correct). Bee itself adopted the same "sign once, reuse until advertised
data changes" behaviour in **v2.8.1** — hoverfly already did this by
construction, so that upstream change requires no action here.

Symptoms before we understood the migration: "pool fills 256 then `pruned 256
dead, 0 live` every 5 min" (reacher couldn't ping → marked Private → pruned);
"connection reset 1 ms after handshake" (gossip churn made our addressbook
entry stale → presented as unknown peer in a saturated bin); "discover from
bootnode returns 0 peers" (bootnodes ran 2.8 ahead of the network and stopped
registering /14.0.0 — protocol-rejecting, not bin-saturating).

## Vanity overlay (built, measured, 2.2× over random baseline)

**Validated hypothesis:** bee mainnet kademlia bins saturate at
`defaultOverSaturationPeers = 18`. Most random-overlay dials land in bee's
already-full bin 0 and are dropped right after the handshake
(`Kad.Connected` → `ErrOversaturated`). A nonce giving our overlay high PO to
specific stable peers puts us in their *deeper*, undersaturated bins (PO=8+
typically has 0-3 peers), so those peers accept and keep our connection. This
is why `README.md` step 2 tells you to pre-mine a nonce with
`hoverfly vanity-overlay`.

Wire-level diagnosis (default random nonce, one bench run): 2.2 push attempts
per chunk (3.5× overhead); of 3163 errors, **3065 were `dial too soon`**
(libp2p's per-peer cooldown rejecting redials of peers bee already RST'd);
median push 60 ms (sub-bee when it works); p95 4.9 s concentrated on ~10 slow
peers; **only 70 of 256 sessions delivered receipts** — ~73% were
dead-on-arrival or RST'd before we pushed anything. Peer-diversity sanity
check ruled that out as the cause: 100/100 random chunk addresses had a pool
peer at PO ≥ 9. The bottleneck was always the bins peers put *us* into.

`vanity-overlay` brute-forces a nonce maximizing PO to targets. Two modes:
**anchored** (`--target-overlay`, maximize min-PO across targets — cheap when
targets share a prefix) and **coverage** (no targets, uses `peers.json`,
maximize count of peers at PO ≥ `--target-po`). Anchoring against 5 top-pusher
peers sharing a `d9` prefix found PO ≥ 8 to all 5 in 442 attempts (instant).

Measured (5 MiB via daemon, 10 warm runs each):

| Config | Median KiB/s | Best | vs bee-light (822 deferred) |
|--------|-------------:|-----:|---------------------:|
| Random overlay | 120 | 159 | 14-19% |
| Single-target vanity (PO=13 to 1 peer) | 215 | 336 | 26-41% |
| **Multi-target vanity (PO≥8 to 5 peers)** | **269** | **349** | **33-42%** |

Single-target is 2.0× over random; multi-target 2.2× over random + 25% over
single-target with lower variance. Caveats: anchors must be **stable** peers
(if an anchor goes offline the advantage evaporates — multi-target trades
per-anchor PO for redundancy) and **empirically chosen** (best from a prior
run's top-pusher list; arbitrary discovery-dump peers give no benefit). The
advantage is **per-peer, not network-wide** — we're PO≈0 to most random
peers, so they still drop us; the win is that our top performers stay
connected.

Workflow: run a normal-overlay upload to populate `peers.json` + surface
top-pushers → `vanity-overlay --target-overlay <each> --target-po N --output
overlay-nonce` → restart with `--nonce-file overlay-nonce`. Higher
`--target-po` = stronger anchor but exponentially more search cost (>~12-14
takes minutes; >~20 needs distributed search).

## Further work (ordered by expected impact)

1. **Daemon + public reachability** is the recommended config and the only
   consistently measurable win (1.74×). Keep `--listen`/`--advertise`
   prominent in CLI help.
2. **Peerlist freshness fixes** (shorter `RECENT_FAILURE_SECS` or
   halved single-failure lockout; probe-on-fill retry). A ~6× swing was seen
   just from removing stale lockouts. Composes with public reachability.
3. **Re-measure SWAP in the daemon + public + multi-GB regime** — the one
   configuration where per-peer cumulative might reach bee's threshold-upgrade
   gate before the session dies.
4. **Distributed workers on different IPs.** If the per-IP wall is real, N
   machines each pool+buffer-scaled give linear scaling. Coordinator/worker
   over TCP (NAT/auth/latency make this harder than the deleted local variant).
5. **`IN_FLIGHT_CAP` tuning for full-node peers.** The base 4 is sized for
   bee-light's 450K/s worst case; full-node peers (4.5M/s) could take 8-16.
6. **Chunk storage + real pullsync.** The item that would close the remaining
   gap vs bee: a reserve lets us serve non-empty pullsync offers and become a
   real kademlia citizen, unlocking the stable-neighbour dynamics that make
   bee-light fast.
