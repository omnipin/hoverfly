# PERFORMANCE.md

Cross-operation summary of every optimisation in this crate's history,
organised by code path. Cites file:line where the constant or mechanism
lives so it's grep-able. Empirical numbers are mainnet observations on
a residential Mac + a Fedora VPS; treat them as orders of magnitude, not
guarantees.

## Discovery

Discovery is hive-driven: dial a peer, accept its hive stream, listen for
broadcast peer announcements for `--wait` seconds, recurse for `--rounds`
hops. The slow knob is the per-peer wait, so wins come from parallelism
and peerlist hygiene.

- **Parallel per-round dialing.** `--discover-concurrency` (default 16,
  see `client.rs:84 DEFAULT_DISCOVER_CONCURRENCY`) keeps N hive
  exchanges in flight at once. A 70-peer round at `--wait 30` finishes
  in `ceil(70/16) × 30 ≈ 150 s` instead of `70 × 30 = 2100 s`.
- **Recursive expansion.** `--rounds` (default 1, recommended 3 for
  upload workloads): each round seeds the next from the peers learned
  in the previous, broadening address-space coverage from the
  bootnode's neighborhood out to the wider mainnet. 1 → 3 rounds
  typically triples-quadruples the peerstore.
- **Healthcheck dial probe** (`--healthcheck` + `--healthcheck-concurrency`,
  default 64). After discovery, dial each peer once and record the
  result in `peers.json`'s reachability cache (`peers.rs::Peer.last_dial_*`).
  Future ops then skip recently-failed peers up-front, saving
  `--dial-timeout`-seconds per known-dead peer. The cache is monotonic
  (later successes overwrite older failures, never the reverse).
- **Underlay ingestion filter.** `PeerStore::upsert` (`peers.rs:198`)
  drops `/dns4/`, `/dns6/`, `/dnsaddr/`, `/ip6/`, and non-`/tcp/`
  multiaddrs at write time via `is_dialable_str`. This keeps
  unreachable entries (no DNS resolver in this crate, residential
  networks rarely have outbound v6) from burning dial timeouts
  later.

## Retrieval (fetch)

Per-chunk pull from the swarm. Each chunk needs a closest-peer dial +
substream open + content delivery. Bottleneck is per-chunk RTT and
peerlist coverage of the chunk's neighborhood.

- **Per-chunk peer racing.** `--concurrency` (default 5, see
  `client.rs:79 DEFAULT_FETCH_CONCURRENCY`) races N peers in parallel
  per chunk; whoever returns a valid chunk first wins. Slow/dead peers
  no longer head-of-line block a fast one. Set to 1 for strict
  closest-first sequential behaviour.
- **Chunk cache** (`cache.rs::ChunkCache`, cloneable). Both fetch and
  the daemon's inbound responder share a single cache via `Clone`, so
  repeated retrievals (e.g. mantaray walks that revisit fork chunks)
  hit memory instead of the network. Cache survives for the lifetime
  of the process / daemon.
- **Parallel mantaray walk.** `list_manifest` / `fetch_manifest_path`
  walk the manifest tree concurrently rather than depth-first
  sequentially. With `--concurrency 5` this is roughly `branches × 5`
  parallelism at each level.
- **`--max-retries 0` = uncapped on fetch.** Marches through every peer
  in the proximity-sorted candidate list until one yields the chunk.
  Useful when the chunk's neighborhood is sparsely represented in
  `peers.json` (most non-AOR bees still forward via their own kademlia).
- **Reachability-cache priming.** Same `RECENT_FAILURE_SECS = 300` window
  (`peers.rs:55`) deprioritises peers that failed recently in any prior
  operation, so a fetch run doesn't burn dial timeouts on peers a
  preceding discover already proved unreachable.

## Upload (pushsync)

The most code in the repo, because pushsync is RTT-bound, accounting-
mediated, and adversarial (bee blocklists overlays that overdraw their
ghost balance). Wins come from session reuse, parallelism, accounting
discipline, and graceful failure handling.

### Session model

- **Persistent peer sessions** (`transport.rs::PeerSession`, see
  `transport.rs:368`). One libp2p connection per peer, kept open
  across many chunks; each pushsync opens a fresh yamux substream
  over the existing connection. Avoids the ~150-300 ms per-chunk
  setup cost of the dial-handshake-pricing dance.
- **Concurrent pushes per session.** Each `PeerSession` has a single
  swarm-driver task that pulls commands from a channel and spawns each
  push as its own task into a `FuturesUnordered`. Many in-flight pushes
  per session multiplex over yamux without blocking each other.
  Accounting and pseudosettle are serialised via `tokio::sync::Mutex`
  on `SessionState` (`transport.rs:598`); accounting locks are released
  before any network IO.
- **Session pool** (`client.rs::SessionPool`, see `client.rs:1223`).
  `--concurrency` (default 8, `client.rs:678 DEFAULT_UPLOAD_CONCURRENCY`)
  controls target pool size. Each session can carry many in-flight
  pushes; the per-chunk dispatcher picks the closest-by-proximity
  session for routing.
- **Address-space spread** (`client.rs::spread_across_address_space`,
  `client.rs:1927`). Pool candidates are bucketed by leading
  overlay byte (256 bins) and round-robined into the dial queue, so
  the live pool covers proximity bins evenly instead of clustering
  near `0x00`. Random-chunk uploads benefit directly: every PO bin
  ends up with at least one nearby session.
- **Wider dial-fill window.** `SESSION_DIAL_PARALLELISM = 32`
  (`client.rs:1920`) keeps that many dials in flight while filling the
  pool, regardless of target pool size. Mainnet peerlists are
  ~50% stale; a wide dial window finds the target N reachable peers
  in O(N + stale-cluster) wall time instead of O(N × per-peer-timeout).
- **Pre-warmed rotation** (`client.rs:1689 maybe_prewarm`). When a
  session crosses 2/3 of `GHOST_BALANCE_LIMIT_PLUR`
  (`transport.rs:131,137-138`), a replacement is dialed in the
  background while the current session is still serving pushes. When
  the active session retires, the chunk that triggered rotation finds
  a pre-dialed replacement waiting at `entry.take_pending()` instead
  of paying the synchronous dial cost.
- **Split timeouts.** `--timeout` (per-substream, default 10 s) ≠
  `--dial-timeout` (whole session open, default 3 s). Originally one
  knob; splitting them lets dial-timeouts fail dead peers in seconds
  while leaving healthy peers a full timeout budget for slow forwarding
  round trips on real pushes.
- **Daemon mode** (`daemon.rs`, `#[cfg(unix)]` only). Long-running
  unix-socket daemon owns a warm `SessionPool`, reused across many
  upload requests. Per-request pool-fill cost (~5-10 s for a
  128-session pool) is paid once at daemon startup, not per CLI
  invocation. Inbound listener (`inbound.rs`) optionally serves
  retrieval requests from the local upload cache.

### Per-chunk dispatch

- **Proximity-sorted candidate list.** Each chunk's dispatcher walks
  sessions in descending PO to the chunk address (`client.rs:1366`),
  so the closest peer (often inside the chunk's AOR) is tried first;
  bee stores directly without forwarding.
- **In-flight buffer.** `buffer = 128.min(total).max(pool.len())`
  (`client.rs:1315`). Sized to match bee's pusher
  `ConcurrentPushes = swarm.Branches = 128`. Earlier `pool × 16` was
  measured to collapse throughput from ~6 chunks/s to ~0.1 chunks/s
  because mass-concurrent reserves piled up on the per-session
  accounting mutex; 128 is the empirical knee.
- **Shallow-receipt retry.** `pushsync::is_shallow` (`pushsync.rs:54`):
  a receipt signed by a peer outside the chunk's AOR proves a
  forwarding hop, not durable storage. The dispatcher treats it as
  retry-worthy and tries the next-closest peer
  (`client.rs:1463-1471`). After the candidate list is exhausted with
  only shallow + overdraft outcomes, the dispatcher accepts the
  deepest-PO shallow receipt (`client.rs:1621`) rather than aborting:
  bee's pushsync takes the same way out via `maxPushErrors`/`errSkip`.
- **Overdraft refresh fallback.** If every candidate within `cap`
  returns Overdraft (no real errors), the dispatcher sleeps 1.1 s
  (one bee `refreshRate` window, mirrors `pkg/node/node.go`) and
  retries the closest-N peers, since pseudosettle has had a chance to
  refresh credit on each. Distinguished from error-fallback so we
  don't burn the sleep when the issue is actually network failure.
- **Single-attempt per peer per chunk.** `CHUNK_PEER_PARALLELISM = 1`
  (`client.rs:1324`). An earlier per-chunk 3-way peer race tripled
  session-mutex pressure and slowed the tail; shallow-retry already
  handles the case where the chosen peer isn't a real AOR storer.

### Accounting + connection lifecycle

- **Client-side ghost balance mirror** (`transport.rs:131
  GHOST_BALANCE_LIMIT_PLUR = 12_000_000`). Mirrors bee's per-overlay
  `ghostBalance` disconnect threshold (~16.875 M PLUR) with a 4.8M
  margin for in-flight pushes. When our mirror crosses the limit, the
  session driver flips `accept_new = false` and the session retires
  gracefully; the upload loop dials a replacement which resets bee's
  ghostBalance on the new `Connect()`.
- **Pseudosettle wall-second rule.** Bee rejects two pseudosettles
  within the same wall-second; sessions serialise settles via
  `settle_lock` and gate them on `last_settle.elapsed() >= 1.1 s`
  (`transport.rs:728-738`). The auto-settle trigger is one
  `REFRESH_RATE_PLUR` (4.5 M PLUR) of accrued balance
  (`transport.rs:657`), mirroring bee's `apply_credit` channel.
- **Narrow `SAFE_PEER_THRESHOLD_PLUR`** (`transport.rs:47`, =
  `REFRESH_RATE_PLUR × 2` = 9 M PLUR). The peer's announced threshold
  is much larger, but using it directly produces thundering-herd
  contention on the accounting mutex (one experiment: overdrafts on
  50 MiB shot from 1.6 k → 51 k). The narrower cap forces more
  frequent pseudosettles but keeps the dispatch queue from piling up.
- **Per-session pushes ceiling** (`transport.rs:120
  MAX_PUSHES_PER_SESSION = 10_000`). Defence-in-depth ceiling; in
  normal operation ghost balance retires sessions long before this
  fires. Raised from an earlier conservative 25.
- **Dead-peer marking + skip window.** A session whose rotation dial
  fails accumulates strikes; after `DEAD_STRIKES = 3`
  (`client.rs:1213`) the entry parks for `DEAD_SKIP_SECS = 60`
  (`client.rs:1206`). Sized to outlast bee's ghost-overdraw
  blocklist (~22-60 s) and a rotation-dial cluster that hits when
  many sessions retire near-simultaneously at high `--concurrency`.
- **Outer pusher-layer retry.** `MAX_CHUNK_RETRIES = 10`
  (`client.rs:1340`) with linear backoff `1000 × (1+n)` capped at
  10 s (~55 s total). Sized to outlast `DEAD_SKIP_SECS`, so a chunk
  whose entire alive pool transiently collapsed waits for revival
  instead of aborting the upload. Mirrors bee's
  `pusher.DefaultRetryCount`. Independent of the `--max-retries`
  CLI flag (which is the per-attempt peer candidate cap).
- **Timeouts do not retire sessions.** `is_connection_dead`
  (`transport.rs:102`) deliberately excludes `TransportError::Timeout`.
  A single slow pushsync substream errors that chunk back to the
  dispatcher (which advances to the next peer) but leaves the session
  alive for dozens of other in-flight pushes. Treating timeouts as
  dead-connection signals at high `--concurrency` was empirically
  shown to cascade-retire most of the pool and abort the upload.
  Ghost-balance accounting still increments on timeouts, so a session
  that keeps timing out retires naturally via the threshold.
- **Pipeline buffer doesn't block exit on stuck pre-warm dials**
  (`client.rs:1726`). When all chunks are dispatched and dispatch
  resolved, the upload returns immediately even if pre-warm dials are
  still pending. Previously a mid-Multistream-negotiation hang on a
  pre-warm would block the entire upload's return forever.

### Stamping

- **Parallel postage stamp signing on native** (`Cargo.toml` →
  `nectar-postage` with rayon). The `stamp_chunks_parallel` step
  before push uses every available core. WASM stamps sequentially
  (single-threaded runtime). For a 13 MB upload this is ~100 ms vs.
  ~1 s.

## Shared infrastructure

- **Transport (`transport.rs:1180+`).** Native uses `or_transport` to
  serve both plain TCP and TCP-over-WebSocket from one libp2p stack;
  libp2p picks the right inner transport from the multiaddr's
  protocol stack. WASM is WebSocket-only (browsers can't open raw
  TCP). Avoids the false choice between "WS-only" (excludes mainnet
  bees that publish plain `/tcp/`) and "TCP-only" (breaks the
  browser).
- **Active identify-push during connection setup**
  (`transport.rs::prep_connection`, lines 970-1014). One of the
  largest per-session savings in the crate. The interaction with bee:
  1. libp2p connection established; bee sends its identify message.
  2. We receive bee's identify, extract the `observed_addr` (our
     externally-visible underlay as bee sees us), and
     `swarm.add_external_address()` it.
  3. We immediately call `identify.push([peer_id])` — proactively
     re-sending our identify with the now-correct external address —
     instead of waiting for libp2p's periodic identify interval.
  4. We wait for the `Pushed` event before letting `do_handshake` run.

  Without step 3, bee sits idle waiting for our liveness signal
  (it expects our externally-routable underlay before it will engage
  the bee-protocol handshake) for the default ~7-10 s libp2p identify
  interval. Every single session pays that cost. With the active push,
  bee proceeds the moment it ack's our updated identify, which is
  one RTT (~50-300 ms on mainnet). Multiplied across a 128-session
  pool fill, this is the difference between "ready in ~5 s" and
  "ready in ~15 min".

  Code comment at `transport.rs:972-974` calls this verbatim "the
  magic that makes bee proceed immediately instead of waiting ~10s
  for our liveness signal" — preserve that mechanism on any
  refactor of the connection-setup state machine.
- **Reachability log + writeback** (`peers.rs::ReachabilityLog`,
  `apply_log`). Every operation collects dial outcomes into a
  thread-safe log; on completion (success or error) the CLI writes
  observations back to `peers.json`. Next run starts faster.
- **Address-space sketch in upsert.** Cheap leading-byte bucketing
  used both at pool-fill time and for diagnostic histograms.
  Detecting an uneven peerstore (a few bins with <5 peers) is the
  fastest signal that a random-content upload will stall: that's
  the slowest-chunk neighborhood.

## Empirical ceilings

These numbers are *measurements*, not theoretical floors. They move
as the code and the peerlist change; treat them as the working floor
at a given configuration.

| Configuration | Throughput (mainnet, novel random content) |
|---|---|
| Residential Mac, racing-only, ~222-peer peerlist (commit `5741cf9`) | ~75 KiB/s |
| Residential Mac, racing + stream_pool patch, 222 peers (commit `1769220`) | ~133 KiB/s |
| VPS, racing-only, 340 peers (commit `5741cf9`) | ~152 KiB/s |
| VPS, racing + stream_pool patch + 3335 peers (5-round discover) | ~335 KiB/s |
| VPS, **`--concurrency 512 --buffer-multiplier 4`** + 3335 peers | **~1.05 MB/s** |

The earliest version of this doc claimed "~150 KiB/s is the protocol
floor". That claim was wrong; it was a measurement at one
configuration, not a ceiling. The right framing is: **throughput is
gated by peerlist coverage of the chunk address space, by the rate
at which we can negotiate fresh substream upgrades per connection,
and by bee mainnet's forwarding latency for chunks bee doesn't have
cached** — in roughly that order of impact based on what we've
measured. Of those three, the first two are addressable in this
client; the third is mainnet's reality.

## Known limits (not bugs)

- **Bee mainnet forwarding RTT** is the per-chunk floor. ~200-800 ms
  per chunk per hop is normal, more on long PO chains. The theoretical
  ceiling with pool size 128, buffer 128, and per-chunk RTT around
  500 ms is ~1 MB/s; we now reach ~1/3 of that with the right peerlist.
  The rest is reachable but needs the levers in the "Further work"
  section below.
- **Bee dedup masks performance.** Re-uploading the same bytes hits
  bee's "is within AOR" short-circuit (`pushsync.go:295`) without
  forwarding. Always test with freshly-randomised content; a re-run
  of an earlier upload tells you nothing about real throughput.
- **Peerlist coverage of the address space** is the dominant single
  lever for random-content uploads. A chunk whose address lands in a
  PO bin with <5 peers in your store will stall, no matter how many
  sessions you open. Going from `--rounds 3` (~340 peers on the VPS)
  to `--rounds 5` (~3300 peers) more than doubled throughput in
  measurement; this is the biggest single-knob improvement available.
- **`--max-retries` is silently capped.** `client.rs:1372`:
  `cap = max_retries.max(1).min(order.len())`. `0` is promoted to
  `1`; the value is capped by the live (non-parked) pool size, so on
  a small or attrited pool the user-supplied number is an upper
  bound, not a guarantee.
- **Pool + buffer scaling together unlocks ~1 MB/s on a VPS.**
  The earlier `ISHEIKA_BUFFER_MULT` sweep showed pure buffer scaling
  regressed (at fixed pool=128, doubling buffer 1→2→4→8 went
  20→24→34→65 s). That was at *fixed* pool. The actual lever is
  **scale pool and buffer together** so per-session in-flight stays
  constant while total in-flight grows. Now exposed as
  `--buffer-multiplier` (CLI) / `ISHEIKA_BUFFER_MULT` (env). VPS
  sweep, 50 MiB random, single-process `upload --raw`:

  | Configuration | Time | Throughput |
  |---|---:|---:|
  | baseline `--concurrency 128` (mult=1) | 138 s | 380 KiB/s |
  | `--concurrency 256` (mult=1) | 91 s | 576 KiB/s |
  | `--concurrency 256 --buffer-multiplier 2` | 76 s | 690 KiB/s |
  | `--concurrency 512 --buffer-multiplier 4` (run 1) | 65 s | 807 KiB/s |
  | **`--concurrency 512 --buffer-multiplier 4` (run 2)** | **49 s** | **1070 KiB/s** |
  | `--concurrency 768 --buffer-multiplier 6` | 126 s | 416 KiB/s |
  | `--concurrency 1024 --buffer-multiplier 8` | 122 s | 430 KiB/s |

  Sweet spot is `--concurrency 512 --buffer-multiplier 4`: ~3
  in-flight per session at race=3, 1536 total in-flight chunks across
  512 sessions. Beyond it, per-session yamux contention dominates and
  throughput collapses.

  Why the earlier sweep missed it: that sweep varied `BUFFER_MULT`
  at `--concurrency 128`. Doubling buffer there doubles per-session
  load (3 → 6 → 12 → 24) and the connections saturate. The new sweep
  varies BOTH knobs together, keeping per-session ~3 across the
  range, so total in-flight grows without congesting any one
  connection.

  This is the configuration that hits **1.05 MB/s on a VPS**, beating
  the previous single-process baseline (450 KiB/s) by 2.4× and the
  best multi-worker configuration (~600 KiB/s) by 1.8×. Multi-worker
  is no longer the most-impactful lever; pool + buffer scaling is.

## `--substream-upgrade-cap` sweep (single-trial)

One-trial sweep on the 3335-peer VPS workload, fresh random 5-MiB
tar per iteration, `--concurrency 128 --max-retries 128 --timeout 30`:

| cap | time | throughput |
|---:|---:|---:|
| 8 | 56.7 s | 92 KiB/s |
| 16 | 34.7 s | 151 KiB/s |
| 32 | 20.9 s | 246 KiB/s |
| 64 | 46.0 s | 111 KiB/s |
| 96 | 28.4 s | 180 KiB/s |
| 128 | 25.0 s | 205 KiB/s |

The non-monotonic shape (64 worse than both 32 and 96) is the
giveaway that single-trial run-to-run variance on mainnet is
~2×, comparable to the choice itself. Honest reading:

- cap ≤ 16 is genuinely too low — the bottleneck shifts back toward
  near-upstream serialization.
- cap ≥ 32 all sit in the same plausible range; we can't tell them
  apart at one trial each.
- Default of 64 is kept because (a) we have prior validation against
  it (the 15-second VPS baseline + Mac 31% improvement measurement
  both used 64), (b) it has comfortable headroom for the lower end
  of `--concurrency` values, and (c) the asymmetric cost of being
  too low (cap=8 → 57 s) is worse than being too high (cap=128 →
  25 s).

For a real verdict on a specific deployment, run 5+ trials per cap
interleaved (not all-cap-A-then-all-cap-B) on a representative file
size, take the median per cap, ignore the deltas if the IQR of any
single cap overlaps the median of another.

## Multi-connection-per-peer sweep (also a negative result)

Commit `f6ad6ff` added `ISHEIKA_CONNECTIONS_PER_PEER` to test the
buffer-scaling negative's followup hypothesis: that opening multiple
independent yamux pipes per peer would relieve the per-connection
contention. VPS workload (5 MiB random tar, 3335-peer pool,
`--concurrency 128`), 2D sweep of `BUFFER_MULT × CONNECTIONS_PER_PEER`:

|              | conn=1 | conn=2 | conn=4 |
|---:|---:|---:|---:|
| **buf=1** | 16 s | 20 s | 30 s |
| **buf=2** | 15 s | 48 s | 21 s |
| **buf=4** | 15 s | 28 s | 28 s |
| **buf=8** | 34 s | 32 s | 17 s |

The mechanism works as designed — the `buf=8` row (where yamux
contention is induced) shows monotonic improvement with more
connections (34 → 32 → 17 s). But no cell in the grid beats the
baseline `buf=1 conn=1` at 15-16 s. Multi-conn is a real lever for
configurations that engage yamux contention, but those configurations
are *also* the ones that aren't fast in the first place. At the
workload sizes we test, single-connection with default buffer is
already at or near the per-overlay ceiling.

Both env knobs (`ISHEIKA_BUFFER_MULT`, `ISHEIKA_CONNECTIONS_PER_PEER`)
stay in the code as investigator tools but should not be the default
recommendation.

## Multi-worker upload (shipped, modest gain)

The 5-phase multi-worker plan landed (commits `b4897c8` Phase 1
through `02926ea` Phase 5). `isheika upload-parallel` spawns N worker
subprocesses, each with its own ephemeral overlay key. The
coordinator stamps every chunk upfront with the batch-owner key and
distributes batches to workers over Unix sockets; workers push under
their own libp2p identities. Bee sees N independent source overlays
with N independent ghost-balance counters.

**The architecture works. The throughput multiplier is much smaller
than the plan predicted.**

VPS sweep, 50 MiB random content, 3335-peer peerlist, `--concurrency 128`
per worker, single-trial per cell:

| Configuration | Time | Throughput |
|---|---:|---:|
| Single-process `upload --concurrency 128` (baseline) | 116s | 450 KiB/s |
| `upload-parallel --workers 2 --concurrency 128` | 257s | 204 KiB/s |
| `upload-parallel --workers 4 --concurrency 128` | 161s | 326 KiB/s |
| `upload-parallel --workers 8 --concurrency 128` | 89s / 88s | **590 KiB/s** |

Honest read:

- Only `workers=8` clearly beats single-process, and only by **~1.3×**
  — far below the "4-8× linear scaling" the plan predicted.
- `workers ∈ {2, 4}` either regress or match the baseline.
  Coordinator IPC overhead + correlated pool-fill dial storms +
  reduced per-chunk peer coverage (each chunk only sees its assigned
  worker's pool) plausibly explain it; we didn't pin down which
  dominates.
- The plan's assumption was that bee's per-overlay accounting
  (4.5 M PLUR/sec refresh rate per overlay) was the cap. Empirically
  that wasn't the bottleneck at our workload — we're well under the
  per-overlay credit ceiling. The real wall is some combination of
  mainnet's aggregate forwarding capacity for this one source IP,
  variance in close-peer availability across workers' independent
  pool subsamples, and IPC + coordination overhead.

The multi-worker pipeline is shipped and correct: stamps are reused
across workers, failed chunks re-route, dying workers re-queue their
in-flight batches. It's just not the throughput unlock the plan
hoped for. **For real multiplicative gains, distinct source IPs
(workers on different machines) is the next architectural step**,
since per-IP rather than per-overlay seems to be where mainnet's
implicit ceiling sits.

## Further work (unblocked, ordered by expected impact)

1. **Distributed workers on different machines.** Workers communicate
   with a single remote coordinator over TCP instead of Unix sockets.
   N different IPs hit mainnet from N different routes; bee's
   per-IP rate-limit + per-IP connection cap + (likely) aggregate
   forwarding capacity become N-fold higher. Same protocol
   (`src/multiwork/protocol.rs`), different transport (replace
   `UnixStream` with `TcpStream`). Real expected multiplicative
   scaling — but coordination is harder (auth, NAT, latency).
2. **Larger-workload re-measurement of single-worker knobs.** All
   sweeps to date use 5-50 MiB workloads where pool-fill amortises
   loosely. A 500 MiB-5 GiB workload might surface different
   bottlenecks (push-phase dominates entirely, ghost-balance
   rotation fires repeatedly). The multi-conn / buffer / cap knobs
   we closed as negative at small scale might prove load-bearing at
   that scale.
3. **`--substream-upgrade-cap` interleaved-trial sweep.** Single-trial
   sweep couldn't separate signal from noise. A 5-trial
   interleaved-order sweep + median per cap would settle whether 32
   actually beats 64. Available headroom is small per the data;
   probably not worth the time compared to (1).
