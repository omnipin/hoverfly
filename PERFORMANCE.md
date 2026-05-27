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
- **Pre-warmed rotation** (`client.rs::maybe_prewarm`). Two triggers:
  1. **Ghost-balance**: when a session crosses 2/3 of
     `GHOST_BALANCE_LIMIT_PLUR` (`transport.rs:131,137-138`), a
     replacement is dialed in the background while the current session
     is still serving pushes.
  2. **Dead-session**: when a session's driver task has exited
     (`PeerSession::is_alive() == false`) and the entry has no
     accumulated dial-failure strikes. This catches the empirically
     dominant retirement cause — `dead_low_ghost` (libp2p connection
     died for non-accounting reasons: NAT keepalive expiry, bee
     restart, yamux idle timeout) — that the ghost-balance trigger
     never sees because the connection dies long before ghost balance
     approaches the watermark. The strike gate prevents repeated
     re-dials to peers that already refused us once (which would burn
     bee's per-IP libp2p rate limit at ~10 RPS / burst 40 per /32).
  Either way, when the active session retires, the chunk that
  triggered rotation finds a pre-dialed replacement waiting at
  `entry.take_pending()` instead of paying the synchronous dial cost.
  The dispatcher sweeps for prewarm candidates after every chunk
  completion (ok OR err) and on every 5 s heartbeat, so dead sessions
  get a replacement queued promptly even during a stall.
- **Session-retirement diagnostic counters** (`transport::diag`).
  Process-global atomics distinguish retirement causes: `dead_low_ghost`
  (driver exited at ghost < prewarm watermark — candidates for
  per-peer reconnect), `dead_prewarm_ghost`, `dead_high_ghost`,
  `ghost_threshold`, `max_pushes`, plus `prewarm_on_dead` /
  `prewarm_on_ghost` to attribute prewarm dials. Printed to stderr at
  upload end. Empirical finding: at `--concurrency 512` on mainnet,
  100% of retirements are `dead_low_ghost`; bee never blocklists us
  at the accounting layer. The ghost-balance retirement path is
  effectively dead code at this concurrency.
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

- **3-bucket storage-radius-aware sort.** Each chunk's dispatcher
  ranks pool entries into three buckets, then by descending PO
  to the chunk address within each bucket:

  | Bucket | Entry classification |
  |---|---|
  | 0 (front) | confirmed in-AOR storer (`storage_radius ≤ chunk_PO`) |
  | 1 (middle) | unknown (no observation yet — fresh session) |
  | 2 (back) | confirmed forwarder for this PO range |

  Earlier the same 3-bucket design regressed ~1.6× because
  "unknown" was contaminated with slow/NAT'd/dead peers. The mid-
  2026 stack pre-filters those out (`is_dead`, cooldown filter,
  `inflight_cap`) so "unknown" is now a clean pool of fresh
  sessions — worth trying ahead of known forwarders. Empirically
  bumps median +28% on a pool=256 setup (827 → 1055 KiB/s).
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
- **Per-chunk 3-way peer race.** `CHUNK_PEER_PARALLELISM = 3`
  (`client.rs:1813`). Each chunk dispatches to the top-3 closest
  peers concurrently; the first valid receipt wins, the losers
  finish their accounting silently in the background. Mid-2026
  reversal of an earlier "single-attempt" finding: with the
  pipeline buffer now hard-capped at 128 (was `pool × 16` = 1.5k+
  in flight), session-mutex pressure no longer dominates the tail,
  and racing collapses the per-chunk RTT from N peers' worth of
  serial walks to roughly one peer's RTT for most chunks. Wire-cost
  trade is ~3× pushes per chunk, but bee credits all 3 hops, so it
  pays for ~2-3× throughput. See "Per-peer in-flight cap" below for
  the load-distribution fix that makes this work without
  saturating top-PO peers.
- **Per-peer in-flight cap** (`client.rs:1505 IN_FLIGHT_CAP = 4`).
  Each pool entry tracks live concurrent pushes via an atomic
  counter (`SessionEntry.inflight_pushes`); the dispatcher's
  `order` filter excludes entries whose count has reached the cap,
  forcing fan-out to lower-PO peers in the same dispatch. RAII
  guard around each push attempt keeps the counter honest under
  cancellation. The cap value 4 is sized so per-peer push rate
  (`4 / 60ms × 6.75K` = ~450K PLUR/s, bee's *light*-node refresh
  rate) stays at or under bee's per-peer accounting budget even
  during fast-RTT bursts; for full nodes it's ~10× under.

  **Why this is the dominant throughput lever** (mid-2026
  measurement on a 64-peer pool, residential VPS, multi-target
  vanity overlay):

  | Config | Median KiB/s | Best | Notes |
  |---|---:|---:|---|
  | No cap (buffer=128, 3-way race) | 194 | 282 | Pre-2026 baseline |
  | **`IN_FLIGHT_CAP = 4`** | **515** | **557** | **2.66× median** |

  Mechanism: without the cap, the dispatcher stacks 5-7 concurrent
  pushes on the top-PO peers per upload — at 6.75K PLUR/chunk and
  60ms median push latency, that's ~675K PLUR/s of debt per
  saturated peer, well above bee's full-node refresh rate
  (4.5M/s) for *each* peer (since accounting is per-peer, not
  global). The peer's accounting goes into overdraft → bee returns
  `ErrOverdraft` on subsequent pushes → dispatcher rotates →
  effective throughput drops to refresh-rate-per-saturated-peer.

  Bee-light avoids this naturally because its kademlia routes each
  chunk to ONE neighbor per the AOR rule, spreading load across
  all 131 connected peers. We don't have a kademlia table, but the
  in-flight cap is a cheap approximation: forces wider fan-out
  when top candidates are busy. After the change, the
  `PUSH_OUTCOME_OVERDRAFT` counter goes from 18% of pushes to
  zero. The 1398 "dispatch failed" events that remain are all
  `(0 overdraft, 0 shallow, 0 err)` — meaning "all candidate peers
  temporarily at cap, chunk waits 500ms and retries". Acceptable
  trade: the upload still finishes in 9-12 s for 5 MiB instead of
  22-35 s.

  **Cap composes with two extensions** (each commit-and-measured):

  | Stack | Median KiB/s | Best |
  |---|---:|---:|
  | uniform `cap=4`, pool=64 | 515 | 557 |
  | + `--pool-size 128 --buffer-multiplier 2` | 665 | 954 |
  | + latency-aware `inflight_cap()` + `--pool-size 256` | 827 | 1106 |
  | + 3-bucket AOR sort | **1055** | **1075** |

  Latency-aware cap: fast peers (EWMA < 200ms) get cap=8, medium
  cap=4, slow (EWMA ≥ 2s) cap=2. Slow peers self-throttle without
  the dispatcher having to model them explicitly.

  3-bucket sort: confirmed in-AOR storers first, unknown next,
  confirmed forwarders last. Sends chunks to the most likely
  storer in fewer hops; only fall back to known forwarders when
  the front-of-line candidates are saturated or dead.

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
| VPS, daemon + vanity overlay (PO=10 single-target), pool=64 | ~194 KiB/s median, 282 best |
| VPS, daemon + vanity overlay + **per-peer in-flight cap** (`IN_FLIGHT_CAP=4`), pool=64 | ~515 KiB/s median, 557 best |
| VPS, all of the above + **`--pool-size 128 --buffer-multiplier 2`** | ~665 KiB/s median, 954 best |
| VPS, all of the above + **latency-aware cap** + `--pool-size 256` | ~827 KiB/s median, 1106 best (1.08 MiB/s) |
| VPS, all of the above + **3-bucket storage-radius sort** | **~1055 KiB/s median (1.03 MiB/s), 1075 best** — last pre-bee-2.8 measurement |

**Bee 2.8.0 network rollout (May 2026)** — bee 2.8 raised handshake to
`/swarm/handshake/15.0.0` and hive to `/swarm/hive/2.0.0` (network-wide
upgrade required). Old `/14.0.0` and `/1.1.0` substreams stop being
registered on upgraded nodes, so any v14-only client gets
`UnsupportedProtocol` rejections that look exactly like kademlia
bin-saturation. Once we shipped v15 + cached `(timestamp, signature)`
+ libp2p ping responder + the post-bee-2.8 IP-diverse `peers.seed.json`:

| Configuration | Throughput (mainnet, novel random content) |
|---|---|
| VPS, v15 + cache + dnsaddr bootnode + 794-IP seed | ~324 KiB/s median, 525 best (high variance) |
| **GitHub Actions** runner, same config | **~473 KiB/s median, 527 best** |
| CircleCI runner, same config | ~376 KiB/s median, 462 best |

The VPS regression vs the pre-bee-2.8 number is partly explained: bee
2.8 nodes prune unreachable peers more aggressively (the reacher uses
libp2p ping, which we now respond to, but the addressbook eviction
that started before we added ping is still affecting us across the
network). CI numbers, by contrast, jumped from ~50-200 KiB/s to
~400-500 KiB/s because the v14 fallback path was effectively broken
against the bee 2.8 majority of peers — v15 unlocked the full
addressable peerset.

For context, bee-light reports ~822 KiB/s on the same 5 MiB random
upload — but that's its `deferred-upload` time which returns ~22×
faster than chunks are actually retrievable (see "Bee-vs-isheika
end-to-end comparison" below). On a fully-durable-receipt basis
(we wait for every receipt before returning) our **1.03 MiB/s
median beats bee-light's own reported deferred-upload number by
~28%**, and our 1.05 MiB/s best by ~33%. Against bee-light's real
~320 KiB/s durable rate, our median is ~3.3×.

### Recommended config (mid-2026)

For daemon mode on a VPS with a reasonable upload workload:

    isheika \
      --nonce-file overlay-nonce \
      --buffer-multiplier 2 \
      daemon \
      --socket /tmp/isheika.sock \
      --pool-size 256 \
      --listen /ip4/0.0.0.0/tcp/1635 \
      --identity 0xYOUR_KEY \
      --advertise /ip4/YOUR_PUBLIC_IP/tcp/1635

Key knobs:
- `--pool-size 256` quadruples the candidate peer pool vs the
  pre-cap default of 64. More candidates → fan-out is wider →
  latency-aware cap can pick fast peers more easily.
- `--buffer-multiplier 2` doubles in-flight chunks (128 → 256), so
  the dispatcher can keep more sessions saturated within their
  per-peer in-flight cap.
- **Latency-aware `inflight_cap()`** (hardcoded in `SessionEntry`):
  cap = 8 for fast peers (EWMA < 200ms), 4 for medium, 2 for slow
  (EWMA ≥ 2s). Concentrates load on proven-fast peers without
  inflating yamux contention.
- Stable overlay + vanity nonce + `--listen --advertise` are
  prerequisites for the citizenship-adjacent behavior bee expects.

Cold-start cost: pool=256 takes ~80 s to fill vs ~30 s for pool=128
on the residential VPS we test on. For daemon mode this only
matters once per daemon-lifetime — the eager pool fill runs in
the background before the first upload arrives, so it's invisible
to the first request unless you start the daemon and upload
immediately.

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

## Multi-worker upload (built, validated, removed)

A 5-phase plan to scale upload throughput via N worker subprocesses,
each with its own ephemeral overlay key, coordinated over Unix
sockets. Hypothesis: bee's per-overlay accounting was the cap, so
N overlays would give N× throughput. Landed Phases 1-5 (commits
`b4897c8` through `02926ea`), validated, found strictly dominated
by single-process with the pool+buffer scaling fix above, **removed**.

50 MiB random content sweep, VPS, before the buffer-multiplier fix:

| Configuration | Time | Throughput |
|---|---:|---:|
| Single-process `upload --concurrency 128` (then-baseline) | 116s | 450 KiB/s |
| `upload-parallel --workers 2 --concurrency 128` | 257s | 204 KiB/s |
| `upload-parallel --workers 4 --concurrency 128` | 161s | 326 KiB/s |
| `upload-parallel --workers 8 --concurrency 128` | 89s | 590 KiB/s |

Combination test after the buffer-multiplier fix:

| Configuration | Time | Throughput |
|---|---:|---:|
| Single-process `--concurrency 512 --buffer-multiplier 4` | 49s | **1070 KiB/s** |
| `upload-parallel --workers 2 --concurrency 512 --buffer-multiplier 4` | 234s | 224 KiB/s |

The combination regresses because both knobs target the same
underlying parallelism. Multi-worker × 2 with 512 sessions each = 1024
total connections from one IP, which competes with itself for local
ephemeral ports and bee's per-IP / per-bee limits. Single-process
with the same total parallelism reaches the throughput unencumbered.

**Lesson:** the throughput wall wasn't bee's per-overlay accounting.
It was simply how much parallelism a single libp2p stack could
extract from one source IP. Once that was unlocked via
pool+buffer scaling, multi-worker added nothing.

The full multi-worker code path (coordinator + worker subcommands,
IPC protocol, ~900 LOC) was removed after this finding. Architecture
sound, hypothesis falsified.

## JIT-AOR sessions (tried, falsified, removed)

The hypothesis: at the start of an upload, plan address-space-
targeted dials for peers that look like in-AOR storers for specific
chunks but aren't in the initial pool. Run those dials in parallel
with the chunk pushes; each successful dial appends a session to
the live pool, seeded with a `storage_radius` hint so the
dispatcher promotes it for chunks it's a good fit for.

Mechanism worked end-to-end: `plan_jit_aor_candidates` walked
`work`, picked per-chunk top-1 unused peers, deduped + budget-
truncated; a background `FuturesUnordered` arm in the dispatcher's
select loop ran dials with `AOR_DIAL_PARALLELISM=32`; successful
dials appended to a now-`RwLock<Vec<Arc<SessionEntry>>>`-backed
pool; the dispatcher's per-chunk snapshot picked up the new entries
on the very next dispatch.

### Measurement results (May 2026)

The `--concurrency 512 --buffer-multiplier 4` baseline (1.05 MB/s)
was **not reproducible** on the mainnet snapshot we tested against:
even on a freshly discovered peerlist (3126 peers, 589 reachable
after healthcheck), the pool filled to ~340/512 then collapsed to
<10 alive sessions under load (multistream-select failures, bee-
side connection-init races). The 1.05 MB/s measurement was taken
when mainnet peer liveness was substantially higher, and was later
shown to also depend on running the upload **immediately** after
discover (stale reachability cache silently locks out most live
peers — see "Peerlist freshness" below).

A/B sweep at the highest currently-viable configuration
(`--concurrency 128`, default buffer, 50×1 MiB random tar
collection, 3 trials interleaved, May 2026):

| Trial | Arm | JIT sessions landed | Pool size | Time | Throughput |
|-------|-----|--------------------:|----------:|-----:|-----------:|
| 1 | OFF | — | 128 | 200.6s | 255.4 KiB/s |
| 1 | ON | 16 | 144 | 256.1s | 200.0 KiB/s |
| 2 | OFF | — | 128 | 416.1s | 123.1 KiB/s |
| 2 | ON | 22 | 150 | 344.1s | 148.9 KiB/s |
| 3 | OFF | — | 128 | 331.6s | 154.5 KiB/s |
| 3 | ON | 12 | 140 | 408.4s | 125.4 KiB/s |

**Medians: OFF = 154.5 KiB/s, ON = 148.9 KiB/s** — a wash (3.6%
regression, well within ~2× run-to-run mainnet variance).

### Why it didn't help

The 2-4× speedup prediction assumed the `--concurrency 512` regime
where JIT-AOR would add ~512 sessions (doubling the pool from ~512
to ~1000). At `--concurrency 128` on today's mainnet:
- JIT-AOR adds only ~15-20 sessions (128 → ~145), a ~13% pool
  increase — not enough to materially change per-chunk routing.
- The bottleneck is **mainnet forwarding RTT** (~200-1500 ms per
  hop), not pool coverage. Even when a JIT session lands for a
  specific chunk, the chunk's dispatch is often already in flight
  to another peer by the time the JIT dial completes.
- The initial pool of 128 already covers enough of the address
  space on a 589-peer reachable set that the marginal JIT-AOR
  sessions don't change the closest-peer distribution much.

The planner, dialer, CLI flag (`--aor-budget`), env knob
(`ISHEIKA_AOR_BUDGET`), and `SessionPool::extend_one` were removed.
The receipt-driven storage-radius routing (`in_aor()`, two-bucket
sort) **stays** — it's independent of JIT-AOR, only the *seeding*
of `storage_radius` from JIT plan data was tied to the dialer.

### Peerlist freshness (the actual lever)

Side observation that shipped to no code change but should be:
running `discover --rounds 3` and immediately uploading against
that fresh peerlist recovers ~1 MB/s at `--concurrency 128` on the
same VPS / same mainnet snapshot where a healthchecked peerlist
from hours earlier gives ~180 KiB/s. The reachability cache in
`peers.json` ages out faster than its default `RECENT_FAILURE_SECS`
window in practice, and the dispatcher then locks out peers that
are alive again.

Practical guidance: for one-shot uploads, discover-then-upload in
the same shell minute. Daemon-mode is unaffected (it keeps live
sessions; the cache only matters at pool-fill time).

Open follow-ups around this finding:
- Shorten `RECENT_FAILURE_SECS` (currently 300s; experiments
  suggest <60s would already help) **or** halve the lockout for
  peers that only ever failed once.
- Probe-on-fill: when filling the pool, if `closest()` returns
  enough recently-failed entries to dent target size, retry them
  inline instead of skipping straight to the next-closest.

## SWAP / chequebook (built, tested, no measured benefit)

Hypothesis (from Swarm infra team): uploads are 3× faster when the
client pays peers in BZZ via SWAP cheques instead of relying on
time-based pseudosettle refresh.

Implementation scope: **issuance only**. No contract deploy, no
cashout, no on-chain RPC from this client. See `AGENTS.md` SWAP
section for the wire pieces. Activation:

```
# Caller is responsible for the chequebook contract existing and
# being funded with BZZ. Its issuer() must equal --key's eth address.
isheika upload \
  --batch <BATCH> --key <HEX_KEY> \
  --chequebook 0xYOUR_CHEQUEBOOK_ADDR \
  --chequebook-per-peer-cap-bzz 100000000000000000 \
  --cheques-file ./cheques.json \
  ./file.bin
```

Diagnostic output at upload end:

```
swap: cheques_emitted=N cheques_failed=M
```

### Measurement (May 2026 mainnet, VPS, 771-peer pool, c=64, 5 MiB random files)

Interleaved A/B, 4 trials each:

| Mode | Throughputs (KiB/s) | Median |
|---|---|---:|
| Unpaid | 97, 290, 195, 290 | 195 |
| Paid (cheques fire) | 282, 82, 80, 238 | 160 |

The two distributions overlap completely. Mainnet variance is ~3×
trial-to-trial at this workload size, so we cannot distinguish a
real signal from noise without dozens of trials. The first trial
showed paid 2.9× faster than unpaid; the second pair inverted with
unpaid 2.2× faster. The "fast" run in any pair was the second one,
suggesting an **ordering artifact** — peer caches warm up between
back-to-back runs regardless of payment.

**Verdict: hypothesis not confirmed on this client at this workload.**
SWAP code is correct (cheques sign, are accepted, persist correctly
across runs — see `cheques_emitted=117` / `cheques_failed=0-2`
typical), but produces no measurable throughput improvement.

The code stays. Two reasons:

1. **Retrieval path uses the same accounting.** bee's
   `pkg/retrieval/retrieval.go:497-500` debits OUR balance for each
   chunk we fetch via the same `debitAction.Apply()` as pushsync.
   Threshold-upgrade via cumulative cheques is independent of
   direction, so a long-running fetch worker (archive sync, video
   hosting) would benefit by the same mechanism as a long-running
   upload worker.

2. **The accounting headroom may matter at much higher pool/buffer
   scales.** Mid-2026's `IN_FLIGHT_CAP=4` + pool=128 + buffer=256
   regime keeps per-peer concurrent debt at ~500K PLUR (well under
   bee's 11.25M full-node disconnect limit), so the upgrade path is
   moot. If we push further (cap=8 trial at 590 KiB/s median was a
   yamux contention regression, not an accounting one — suggests
   the next ceiling is per-session substream throughput, addressable
   by spreading across more peers with even more pool, which would
   eventually let SWAP matter again).

### Why it doesn't help here

Reading bee's accounting (`pkg/accounting/accounting.go`), payment
actually has effect via `notifyPaymentThresholdUpgrade` —
cumulative cheque value of `100 × refreshRate = 450M PLUR` per peer
triggers a +`refreshRate` upgrade to that peer's
`paymentThresholdForPeer` (and consequently their `disconnectLimit`).

Our cheques to any single peer total a few million PLUR before the
session dies — three orders of magnitude short of triggering even
one threshold upgrade. Sessions die from external causes (see next
section) long before per-peer cumulative reaches the threshold-
growth gate. The 3× claim is consistent with **long-lived sessions
on a daemon** where one peer accumulates 100s of MiB of paid
traffic and ratchets up the threshold over hours.

### What to look for if re-measuring

- **`cheques_emitted = 0`** on a substantial upload: settle path
  never reached the cheque branch. Pseudosettle covers everything.
- **`cheques_failed >> cheques_emitted`**: signer key mismatch
  (`--key`'s eth address ≠ chequebook's `issuer()`). Verify with
  `cast call <chequebook> "issuer()" --rpc-url https://rpc.gnosischain.com`.
- **`cheques_failed = 0` but throughput unchanged**: expected at
  one-shot upload scales. Try daemon mode with persistent peer
  set + much larger total upload volume.

## Session-death cause (RST analysis)

`conn-closed-io-detail` diagnostic classifies every
`SwarmEvent::ConnectionClosed` by its `cause` field. On mainnet at
c=64-128:

```
conn-closed: io=207 keepalive=0 clean=0
conn-closed-io-detail: yamux-io:connectionreset:104=166 yamux-decode-mid-frame:connectionreset:104=38 yamux:closed=3
```

**100% of session deaths are TCP `ECONNRESET` (errno 104)**. Zero
clean closes, zero keepalive timeouts.

**Important correction:** the ECONNRESET rate does NOT, by itself,
prove bee is "abusively" terminating us. Reading
`go-libp2p/p2p/transport/tcp/tcp.go::tryLinger`, bee's libp2p TCP
listener sets `SO_LINGER=0` on every accepted connection. This
means **every libp2p-go connection close becomes a TCP RST**,
regardless of reason — clean close, accounting blocklist, kademlia
bin prune, NAT keepalive expiry all surface to us as ECONNRESET.
The cause classifier can distinguish IO vs. keepalive vs. clean
internal-libp2p closes, but it can't distinguish *which* bee
subsystem chose to close us, because they all use the same TCP
path.

What we *can* attribute from the data:

- `keepalive=0` rules out libp2p connection-level idle timeout.
- `clean=0` rules out us closing first.
- `yamux:closed` minority rules out yamux protocol fatal errors
  in most cases.

The dominant disconnect path is therefore some bee-side decision to
call `Disconnect → host.Network().ClosePeer()`, surfaced through
`SO_LINGER=0` as RST. Candidate paths in bee
(`grep -rE 'Disconnect\(' pkg/`):

- `kademlia.go:719` "pruned from oversaturated bin"
- `kademlia.go:1194` "kicking out random peer to accommodate node" (bootnode-only)
- `libp2p.go:702` "unable to find peer slot for light node" (not us; we're full-node)
- `libp2p.go:743` "unknown inbound peer"
- Accounting blocklist on overdraft (ruled out by our `ghost_balance` counters)

`pkg/topology/kademlia/kademlia.go:700-704` selects prune targets
in this priority:

1. **Unhealthy** peers (failed bee's `pkg/salud` health probe)
2. **Non-Public** reachability (no public underlay)
3. Random fallback

So both unhealthy and non-public-reachable peers are
disproportionately disconnected. We can mitigate (2) by running
`daemon --listen --advertise` (measured below, 1.74× speedup). We
cannot reliably mitigate (1) because of the kademlia-membership
chicken-and-egg described in the next section.

## Public reachability (`--listen` + `--advertise`)

The fix for the bin-prune effect: be `Public`-reachable to bee.
This requires daemon mode because we need to actually accept
inbound connections — bee's reacher pings us back on the advertised
underlay, and only marks us Public if the ping succeeds. There's no
"advertise-without-listen" mode that works.

### Measurement (May 2026, same mainnet conditions as above)

5 trials each at c=64, 5 MiB random files, same 771-peer peerlist:

| Configuration | Throughputs (KiB/s) | Median | Range |
|---|---|---:|---|
| One-shot baseline | 96, 118, 115, 93, 118 | 115 | wide (1.27×) |
| Daemon, no `--listen` | 170, 54, 79, 42, 88 | 79 | very wide (4×) |
| Daemon + `--listen` + `--advertise` | 158, 208, 188, 220, 200 | **200** | tight (1.4×) |

**Daemon with public reachability is 1.74× the one-shot baseline
median and has the tightest distribution.** Activation:

```
isheika daemon \
  --socket /tmp/isheika.sock \
  --peerlist peers.json \
  --pool-size 64 \
  --listen /ip4/0.0.0.0/tcp/1634 \
  --identity 0x<HEX_KEY> \
  --advertise /ip4/<PUBLIC_IP>/tcp/1634 &

isheika upload \
  --batch <BATCH> --key 0x<HEX_KEY> \
  --concurrency 64 \
  --daemon /tmp/isheika.sock \
  ./file.bin
```

The bee reacher pings our advertised address shortly after
connection; once the ping round-trips, bee marks us
`ReachabilityStatusPublic` and stops preferentially picking us for
bin-prune disconnects.

### Daemon without listen is *worse* than one-shot — why?

Counterintuitive but reproducible: median 79 KiB/s vs 115 for
one-shot. Hypothesis: between uploads, the daemon's persistent pool
sits idle. Sessions in the pool die from RST during idle time (the
same kademlia pruning that kills us during uploads). On the next
upload, many "warm pool" entries are actually dead. The dispatcher
then pays both the dead-session detection cost AND the re-dial
cost, which the one-shot path didn't incur (it just dials fresh).

This suggests an opportunity: a background heartbeat-prober in the
daemon that detects and replaces dead sessions during idle. Not
implemented; tracked in Further Work.

## Status protocol responder (`/swarm/status/1.1.0/status`)

Hypothesis: bee's `pkg/salud` periodically probes connected peers
via `/swarm/status/1.1.0/status`. Peers that don't respond (or
respond with bad values) are marked `Counters.Healthy = false`,
which makes them prune-target #1 in `kademlia.go:700-704`. Serving
the protocol — defaulting `BeeMode="full"` and plausible percentile
values — should reduce our prune rate.

Implementation: `proto/status.proto`, `src/protocols/status.rs`,
inbound-only responder plumbed through both the `Transport`
(outbound sessions accept on the same connection bee opened to us)
and the daemon `--listen` listener. See `src/protocols/status.rs`
module docs for the percentile-passing default values.

### Measurement (May 2026)

Same A/B layout as the daemon+listen test. 5 trials × 3 configs,
fresh 5 MiB random files, c=64, fresh peerlist:

| Configuration | Median throughput | Trials |
|---|---:|---|
| One-shot (no status responder) | 115 KiB/s | 96, 118, 115, 93, 118 |
| One-shot + status responder | 121 KiB/s | 89, 159, 121 |
| Daemon + listen (no status responder) | 200 KiB/s | 158, 208, 188, 220, 200 |
| Daemon + listen + status responder | 134 KiB/s | 113, 152, 134, 137, 156 |

Result: **status responder produces no measurable improvement.**

### Why it doesn't help (the kademlia-membership chicken-and-egg)

Reading bee's inbound-connection path
(`pkg/p2p/libp2p/libp2p.go:712`), bee calls
`notifier.Connected(ctx, peer, forceConnection=false)`. In kademlia
(`pkg/topology/kademlia/kademlia.go:1188`), if the bin for our PO
is at `OverSaturationPeers = 18`, bee returns `ErrOversaturated`
and **we are silently NOT added to `connectedPeers`**. We're still
libp2p-connected, but kademlia-invisible.

`pkg/salud/salud.go:145` iterates `s.topology.EachConnectedPeer`,
which reads from `connectedPeers`. **A kademlia-invisible peer is
never probed.** Our status responder is correctly implemented
(verified via local log: protocol advertised in identify push,
accept call returns successfully), but bee never opens the stream
because we're not in its kademlia. Confirmed by zero
`status responded to {peer_id}` log entries across multiple test
runs against ~1000 connected bee peers.

To become kademlia-visible, we'd have to either:
1. Be in a bin that has room (< 18 peers). Our PO depends on our
   overlay; rotating to a less-saturated bin is possible but the
   client doesn't currently do this.
2. Be discovered by bee's outbound `connect()` path (called from
   bee's manage loop based on hive announcements). This adds us
   with `forceConnection=true`, bypassing the saturation check.
   Slow and out of our control.

The status responder code stays as a long-term defense — if we
ever do become kademlia-visible (e.g. via a future overlay-rotation
feature, or a bee whose bin has room), serving status correctly
prevents the secondary Unhealthy mark. But it cannot fix
present-day mainnet throughput.

## Bee-vs-isheika end-to-end comparison (May 2026)

Apples-to-apples: bee 2.7.1 vs isheika, **same VPS, same identity,
same batch, 5 MiB random files, end-to-end retrievability via
`bzz.limo` (NOT local bee API "uploaded" — that's a deferred-upload
lie which returns in ~0.7s before chunks actually propagate)**.

| Client | Trials (KiB/s end-to-end) | Median |
|---|---|---:|
| Bee 2.7.1 HTTP `/bytes` (deferred=true) | 540, 375, 333, 320 | **354** |
| Bee 2.7.1 HTTP `/bytes` (deferred=false, sync) | wire=720, e2e=510 | **510** |
| isheika one-shot c=64 | 137, 114, 137 | **137** |
| isheika c=256 mult=2 timeout=3 | 111, 80, 100 | **100** |

**Bee is 2.6-7× faster end-to-end depending on configuration.**

### Where the gap lives — instrumented (May 2026)

We added end-of-upload histograms that mirror bee's
`bee_pusher_sync_time`, `bee_pushsync_push_peer_time`,
`bee_pushsync_total_send_attempts`, etc. metrics at
`http://<bee>:1633/metrics`. Same shape, directly comparable.

**Per-stream RTT (wall-clock for one pushsync substream):**

| Bucket | Bee (4114 pushes) | isheika (2069 pushes, c=256 mult=2) |
|---|---:|---:|
| <100 ms | 71% | 77% |
| 100-500 ms | 28% | 4% |
| 500ms-2s | 1% | 3% |
| 2-5s | 0.05% | **22%** |
| 5-10s | 0% | 1.5% |
| Mean | 86 ms | ~640 ms |

Our median per-stream is actually faster than bee's, but our **tail
is 30× worse**: ~22% of our pushes take 2-5 seconds vs bee's 0.05%.

**Per-chunk wall-clock (entry to dispatcher → receipt):**

| Bucket | isheika (1354 chunks, c=256 mult=2) |
|---|---:|
| <500ms | 54% (chunks that landed first-racer fast) |
| 500ms-2s | 6% |
| 2-5s | 12% |
| 5-15s | **23%** |
| >15s | 5% |
| Mean | ~6 sec/chunk |

vs. bee's mean ~452 ms/chunk. **13× worse mean chunk latency** —
our racing helps with the median (54% land <500ms via the
first racer), but our tail dominates the mean.

**Push outcomes (1978 attempts for a 5 MiB upload):**
- ok: 65%
- shallow: 5%
- overdraft: 18%
- error: <1%

vs. bee: ok 99%, shallow 6%, overdraft 11%. Our **overdraft rate is
1.6× bee's**, indicating per-peer accounting is throttling us more.

**Obsoleted by `IN_FLIGHT_CAP=4` (mid-2026)**: the 18% overdraft
rate fell to zero after capping concurrent pushes per peer. See
"Per-peer in-flight cap" in the Upload (pushsync) section. The
diagnosis "per-peer accounting is throttling us more" was correct
but the fix turned out to be on OUR side (limiting concurrent
debits per peer), not bee's (negotiating higher thresholds).

### Why bee wins, in one sentence

The bee node we benchmarked against is `beeMode: "light"` (per
`~/.bee.yaml` `full-node: false`, confirmed via `/status` showing
`reserveSize: 0, storageRadius: 0, pullsyncRate: 0`). **Light bee
does no chunk storage** — every upload travels via pushsync to
other peers, same as us. The 7× throughput gap is therefore NOT
about local storage. The actual difference is:

- Bee-light maintains **131 stable kademlia peers**, all of which
  treat bee-light as a `Public`-reachable kademlia neighbor with
  full-citizen accounting state. Connections survive hours+.
- Our **256-peer pool churns rapidly**: peers RST our connection
  within seconds (kademlia bin-prune of non-citizen peers).
- Bee-light's per-stream RTT is consistent (median ~50ms, tail
  ≤250ms in 99% of streams). Our per-stream RTT has the same
  median but **a 22% tail at 2-5 seconds** — those slow streams
  are presumably peers that don't have accounting state for us
  ready, so they refresh credit before responding, or forward
  through more hops because they don't trust our chunk's stamp
  validation pipeline as much.

In other words: bee-light got its 131 kademlia memberships by being
"adopted" by other bees over time — `forceConnection=true` outbound
dials from the manage loops of other bees that learned about it
via hive. We're trying to short-circuit that via outbound hive
announce, but bees admit us into kademlia slowly (6 pullsync probes
observed in 15 min of idle daemon = roughly 24/hr admission rate).

The fix isn't more tuning — it's **time**. Run the daemon
continuously and let kademlia memberships accumulate. See
"Further work #2: long-duration bee-citizenship measurement".

**OBSOLETED by `IN_FLIGHT_CAP=4` (mid-2026)**: the diagnosis above
was incorrect. Bee-light's 131 stable peers vs our churning pool
*is* a real difference, but the throughput gap wasn't dominated by
connection lifetime — it was dominated by per-peer accounting
saturation during bursts. Bee-light's 131 peers naturally
distributed load such that each peer saw maybe 1.5 pushes/sec; our
64-peer pool with top-K-closest dispatching stacked 5-7 pushes
concurrently on the top peers, exceeding bee's per-peer refresh
rate (4.5M PLUR/s full-node, 450K/s light-node) and triggering
overdrafts. A per-peer in-flight cap of 4 forces our dispatcher to
fan out wider — same effect as kademlia routing without
implementing kademlia. Result: 194 → 515 KiB/s median, on par
with bee-light's *durable* throughput. The "wait hours for bee
citizenship" plan was therefore unnecessary; the load-balance fix
is enough.

### Tuning experiments (no fix, just calibration)

5-MiB random files, freshly-discovered 1407-peer peerlist, May 2026:

| Config | Median (KiB/s wire) |
|---|---:|
| baseline `c=64 --raw` | 137 |
| `c=256 mult=2` | 100 |
| `c=256 mult=2 --timeout=3` | 100 |
| `c=512 mult=4` | 63 (degrades, overdraft surges) |
| `c=1024 mult=4` | 45 (much worse, pool churn) |
| `c-peer=2` (racing 2 instead of 3) | 70 |
| `c-peer=4` | very variable, 17-125 |

The empirical sweet spot today is `c=256 mult=2`, racing 3 peers
per chunk, default `--timeout=10`. `--timeout=3` shaves the tail
slightly but introduces error retries that wash out the gain.

The historic 1 MB/s number from earlier PERFORMANCE.md is not
reproducing on today's mainnet. Either mainnet has degraded or
the configuration we measured then included peer-set conditions
we haven't reproduced.

Push-vs-retrievable gap (what fraction of "upload time" is just
waiting for propagation after the API/client returns):

- **Bee**: 0.74s POST → 16s retrievable. Bee returns 22× faster
  than chunks are actually durable. The HTTP API hard-lies about
  completion via deferred-upload semantics (default ON).
- **isheika**: 32-41s push → 37-45s retrievable. We're only
  9-15% slower than fully durable — we don't lie, we wait for
  receipts before returning.

Reasons bee outperforms us at the end-to-end measurement, in
descending order of probable impact:

1. **133 stable kademlia neighbors** (queried via bee
   `/topology`). Every pushsync routes through bee's persistent
   long-lived neighbor connections — bees that treat bee as a
   permanent member, never bin-prune-disconnect it. Our pool
   has transient sessions: ~64 active at any time, each dies
   within seconds to RST (see "Session-death cause"). The
   network treats bee as a citizen; isheika as a tourist.
2. **Local AOR storage at depth ~9.** ~1/512 of chunks bee
   uploads land in its own AOR (`pkg/pusher/pusher.go:266`,
   `ErrWantSelf` short-circuit) and are stored locally with no
   network push. Small but real.
3. **Pull-sync between neighbors** propagates pushed chunks in
   the background. When bee pushes a chunk to one AOR neighbor,
   that neighbor's pull-sync replicates to others; bee doesn't
   wait. We only get receipts from peers we directly push to.
4. **Mature pusher implementation.** Years of empirical tuning
   on retry, parallelism, shallow-receipt handling, backoff. We
   mirror most of it but likely have rougher edges.

## Bee-citizenship: stable overlay + hive self-announce (built, tested)

After confirming the status responder doesn't fire because we're
not in bee's kademlia (`pkg/topology/kademlia/kademlia.go:1188`
rejects oversaturated-bin inbound peers), the next attempt was to
get into bee's kademlia *over time* by mimicking a real bee
participant:

- **Stable overlay across daemon restarts.** Overlay is
  `keccak256(eth_addr || network_id || nonce)`; previously the
  nonce randomized each process, so every restart looked like a
  new peer to bee. Now persisted via `--nonce-file` (default
  `overlay-nonce` in CWD). See `signer::from_bytes_with_nonce`.
- **Outbound hive announce on every session connect.** Send a
  `Peers` envelope containing our own BzzAddress to every bee we
  connect to. Bee's `peersHandler` reads, dial-probes (via reacher
  ping to our `--advertise` underlay), and adds us to `knownPeers`
  on success. Bee's manage loop may then dial us OUTBOUND, which
  admits us to kademlia with `forceConnection=true` — bypassing the
  bin-saturation gate. See `protocols::hive::announce_self` and
  `transport::do_hive_announce`.

### Measurement (May 2026, VPS, 2941-peer pool from 5-round discover)

5 trials × 4 configs, fresh 5 MiB random files, hive announce
enabled and firing at ~160 announces per upload (verified via
`hive-announce: ok=N fail=M` diag):

| Configuration | Trials (KiB/s) | Median |
|---|---|---:|
| One-shot c=64 | 48, 118, 158, 114, 110 | 114 |
| Daemon+listen+advertise pool=64 c=64 | 69, 160, 69, 164, 120 | 120 |
| Daemon+listen+advertise pool=256 c=256 mult=2 | 91, 131, 266, 83, 91 | 91 |
| Daemon+listen+advertise pool=512 c=512 mult=4 | 92, 190, 189, 178, 113 | 178 |

**Single-upload throughput is unchanged.** Compare to historical
1070 KiB/s at pool=512 mult=4 on 3335 peers (`PERFORMANCE.md`
empirical-ceiling table) — we're 6× below that despite having a
similar-sized peerlist, suggesting **mainnet itself is in a worse
state** than during the historical measurement, or the historical
number depended on conditions we haven't reproduced.

### Why hive announce didn't visibly help (yet)

Hive announces fire successfully (~160 per 5 MiB upload, ~0
failures). Bee's `peersHandler` accepts the envelope. But over 5
minutes of idle daemon (just listening, not pushing), **0 inbound
connections from new peers** were observed.

Candidate explanations:

1. **Bee's manage loop is slow.** Bees that learn about us via
   hive add us to `knownPeers`, but their manage-loop iteration
   that picks new peers to dial may run on a multi-minute cadence,
   or prefer peers learned through other paths. We'd need to leave
   the daemon running for hours and re-measure.
2. **Bee's reacher dial-probe fails.** Bee may attempt to ping our
   advertised address and fail — though we verified our port is
   open externally and bees do reach us via outbound-initiated
   connections. Possibly an issue specific to the reacher's dial
   path.
3. **The compounding effect is real but takes longer than our
   test window.** Bee-citizenship is a slow-burn intervention by
   design; the network has to learn about us, and that learning
   is rate-limited per peer.

The code stays — it's correct, doesn't measurably regress single
uploads, and is the prerequisite for any long-term kademlia-
membership growth. If you want to validate it for real, leave a
daemon running for ~6+ hours, then run a measurement batch.

## Pullsync inbound responder (built, then dropped)

We briefly served bee's `/swarm/pullsync/1.4.0/cursors` and
`/swarm/pullsync/1.4.0/pullsync` substreams to look more like a
real bee citizen — bee's `pkg/salud` (status protocol) didn't
probe us because we weren't in bee's `connectedPeers`, but pullsync
gets opened earlier in the connection lifecycle, so accepting it
was the first reciprocal signal we ever got from bee.

Idle-period measurement showed bee did probe pullsync:

| Measurement window | Cursor probes received | Chunk probes |
|---|---:|---:|
| Daemon idle (~15 min) | 6 | 0 |

Cursor probes mean bee opened a substream specifically to ask for
our reserve cursors, which it only does for peers it treats as
kademlia neighbors. Chunk probes stayed at 0 because we always
returned empty `Offer{Topmost: Get.Start, Chunks: []}`.

**Why we dropped it (commit `00e85c1`):** every empty cursor
response triggered another probe almost immediately. Bee's
puller has no rate-limit backpressure when the offer is
empty (`WaitN(ctx, 0)` is instant), so we ended up in a tight
poll loop with no reciprocal benefit — we'd need to actually
store chunks for the offers to be useful, and chunk storage
is explicitly out of scope. Honest rejection
(`UnsupportedProtocol`) is better for both sides than constantly
saying "I have nothing."

The protocol versions on bee mainnet also moved on: bee 2.8.0
ships `/swarm/pullsync/1.4.0` still on the wire but the substream
ids around it have evolved. Restoring pullsync would mean
re-tracking that without the matching chunk-storage backend, so
the responder code (`src/protocols/pullsync.rs`, `proto/pullsync.proto`,
`PULLSYNC_*_PROTO` consts) was removed entirely.

If we ever do add chunk storage (the "Further work" item that
would close the remaining throughput gap vs bee), pullsync would
come back — but with non-empty offers and a real reserve to back
them.

## Bee 2.8.0 protocol migration (May 2026)

Bee 2.8.0 shipped a network-wide upgrade with three changes that
affect us directly. Bee's release notes label this a hard cutover —
v2.7 and v2.8 nodes can't handshake or gossip with each other.

1. **`/swarm/handshake/14.0.0` → `/swarm/handshake/15.0.0`**.
   `BzzAddress` gains two signed fields: `timestamp` (int64
   seconds since epoch) and `chequebook_address` (20 bytes, zero
   for clients without an on-chain chequebook). The sign payload
   becomes
   `"bee-handshake-" || underlay || overlay || network_id_BE_8
    || nonce || timestamp_BE_8 || chequebook_address`.
   Bee's `pkg/bzz/timestamp.go::CheckTimestamp` rejects records
   whose timestamp is more than `MaxClockSkew = 60 s` in the
   future, and (for the gossip path) whose timestamp doesn't
   advance by at least `MinimumUpdateInterval = 300 s` past the
   existing record.

2. **`/swarm/hive/1.1.0` → `/swarm/hive/2.0.0`**. Same
   `BzzAddress` shape as the v15 handshake. Bee 2.8 receivers
   validate the signature on each gossiped record and drop ones
   that don't verify; this is how the timestamp/chequebook check
   propagates beyond the immediate handshake.

3. **`/ipfs/ping/1.0.0` is now load-bearing.** Bee's reacher
   (`pkg/p2p/libp2p/internal/reacher`) calls `pinger.Ping(addr)`
   to verify our advertised underlay is dial-able. A failed ping
   marks us `ReachabilityStatusPrivate`, which makes us the top
   target for kademlia's 5-minute `pruneOversaturatedBins`
   sweep. Previously the reacher used a swarm-protocol-level
   probe and we got away without registering ping. Bee 2.8's
   reacher path is stricter.

What we shipped to interoperate:

* `proto/handshake.proto` and `proto/hive.proto` gained `nonce`,
  `timestamp`, and `chequebook_address` fields on `BzzAddress`.
  Proto3 zero-defaults keep v14 wire compat — we just emit zeros
  for the new fields when we negotiate `/14.0.0`.

* `signer.rs::generate_sign_data_v15` mirrors bee's v15 payload
  byte-for-byte. `sign_handshake_v15` is the explicit-timestamp
  signer.

* `signer.rs::sign_handshake_v15_cached` is the per-`(underlay,
  chequebook)` cached path. Returns the same `(timestamp,
  signature)` on every call for a given key, so reconnects to a
  peer replay an identical signed record. Without the cache,
  bee 2.8's gossip path rejects every record we issue at every
  receiver — `MinimumUpdateInterval` is 5 minutes but our session
  rotation can re-handshake the same peer minute-scale, so the
  network never updates its addressbook view of us and our
  overlay's kademlia membership ages out. Cache lives in
  `Arc<Mutex<HashMap<...>>>` on `SwarmSigner`; daemon restart
  clears it, which is fine because the next run's `now_unix()`
  is unconditionally newer than any cached value from the
  previous run.

* `protocols/handshake.rs` and `protocols/hive.rs` carry a
  `Version` enum (`V14`/`V15` and `V1`/`V2`). Outbound: try v15
  first via `transport.rs::do_handshake`, fall back to v14 on
  `UnsupportedProtocol`. Inbound: `inbound.rs::run` accepts both
  substream ids in parallel; `respond_to_handshake` is passed
  the version that won negotiation. Hive uses the same family
  the handshake just used — v14 handshake ⇒ v1 hive, v15 ⇒ v2.

* `transport.rs::Behaviour` adds `ping: libp2p::ping::Behaviour`
  with the default config (responds to inbound pings and pings
  every connected peer periodically). Crate dep:
  `libp2p = { features = ["ping", ...] }`.

* `peers.seed.json` (committed) is regenerated from a v15 daemon
  bootstrapping off `/dnsaddr/mainnet.ethswarm.org`: 794 peers
  across 794 unique /32 IPs, IP-diverse so cold-start dials a
  wide net of independent bee operators rather than concentrating
  on a few /32s that bee's `connLimiter` (10 RPS / burst 40 per
  /32) would throttle.

* CI workflows (CircleCI + GitHub Actions) use
  `--bootnode /dnsaddr/mainnet.ethswarm.org`. The hand-picked
  direct-peer bootnodes we used during the v14-only transition
  are no longer needed.

Symptoms we saw BEFORE understanding the migration:

* "Pool fill: 256 sessions, then `pruned 256 dead, 0 live` every
  5 minutes." We attributed this to bee being meaner. Actual
  cause: the reacher couldn't ping us, marked us Private, and
  kademlia pruned us on the next sweep.

* "Connection reset by peer 1ms after handshake completes." The
  handshake substream succeeded but bee's `notifier.Connected`
  immediately disconnected because gossip churn from re-issued
  records made our addressbook entry stale, so we presented as
  an unknown peer landing in a saturated bin.

* "Discover from bootnode returns 0 peers." The bootnodes ran
  bee 2.8 ahead of the network and stopped registering /14.0.0
  for new dialers. They were never bin-saturating us — they were
  protocol-rejecting us.

The pre-bee-2.8 throughput numbers in the empirical-ceilings
table above were measured against a network where most peers
still ran 2.7. After the rollout, the comparable VPS number
dropped from ~1055 KiB/s median to ~324 KiB/s median (high
variance, peak 525). The gap is recoverable but requires
addressing the addressbook eviction that happened during the
v14-only window before we shipped ping/timestamp-cache. A
fresh-identity daemon that comes up clean should land closer
to the pre-2.8 number again; the existing identity is paying
for its eviction history.

## Further work (unblocked, ordered by expected impact)

1. **Daemon + public reachability is the recommended config.** This
   was the only measurable improvement (1.74× at c=64 in earlier
   testing on a different peerlist iteration). Make `--listen` /
   `--advertise` more prominent in CLI help and recommend them for
   any sustained-upload workload.

2. **~~Long-duration bee-citizenship measurement.~~** OBSOLETED by
   `IN_FLIGHT_CAP=4` (mid-2026). The hypothesis was that bee's
   kademlia memberships had to accumulate over hours of hive
   announcing for us to reach bee-light throughput. Turns out the
   real bottleneck was load distribution (we concentrated pushes on
   top-PO peers, hitting per-peer accounting saturation), and
   capping concurrent pushes per peer closes the gap in a single
   benchmark session. Kept here for historical context.

3. **~~Background pool maintenance in daemon mode.~~** DONE. The
   daemon now runs a 5-min tick that prunes dead entries and
   refills from peers.json (`src/daemon.rs::maintain_pool`).

4. **Peerlist freshness fixes.** See "Peerlist freshness" above.
   Shorter cache TTL or inline retry of recently-failed peers
   during pool fill. Independent of public reachability and would
   compose with it: a 6× swing was observed just by removing
   stale lockouts.

5. **Re-measure SWAP impact in the daemon+public regime.** SWAP at
   one-shot didn't help, but the failure mode (sessions die before
   per-peer cumulative reaches the threshold-growth gate) is
   exactly what daemon mode amortizes. A multi-GB upload workload
   running through `daemon + --listen + --advertise + --chequebook`
   is the configuration where SWAP *might* finally show measurable
   benefit. Not yet tested.

6. **Distributed workers on different IPs.** If the per-IP wall is
   real (we haven't tested by going past pool=512 enough to
   measure), N separate machines each running their own
   pool+buffer-scaled upload would give real linear scaling.
   Coordinator + worker over TCP instead of Unix sockets; harder
   than the deleted local multiwork because of NAT, auth, latency.

7. **Per-peer in-flight cap tuning.** The current `IN_FLIGHT_CAP=4`
   is sized for bee-light's 450K-PLUR/s refresh rate as a worst
   case. For full-node peers (4.5M/s refresh) the cap could be 8-16
   without saturating; benchmarking that on a workload large
   enough to amortize the wider concurrency might lift the ceiling
   further. Likewise: making the cap a function of recently-observed
   per-peer push latency (faster peer → higher cap) would let
   genuinely fast peers carry more load without rebalancing onto
   slow ones.

## Vanity overlay (built, measured, 2.2× over random baseline)

**Hypothesis (validated):** Bee mainnet kademlia bins are
saturated at `defaultOverSaturationPeers = 18`
(`pkg/topology/kademlia/kademlia.go:55`). Most of our random-overlay
dials hit `topology.ErrOversaturated` because we land in the
already-full bin 0 of every peer we dial — they accept the
TCP+handshake then immediately disconnect us in
`Kad.Connected()` at line 1192.

A nonce that gives our overlay high PO to specific stable peers
puts us in their *deeper*, undersaturated bins (PO=8+ typically
has 0-3 peers in the bin), so those peers accept and keep our
connection.

### Wire-level diagnosis (the data that drove this)

Running the daemon with default random nonce against the same
batch + key + bench file we use everywhere, with
`RUST_LOG=isheika::profile=trace`:

- **2841 `pushsync_phases` events for 1293 chunks** = 2.2 push
  attempts per chunk on average (3.5× overhead).
- **Per-attempt outcomes:** 1238 ok, 87 shallow, 34 overdraft,
  3163 errors. Of the errors, **3065 were `dial too soon`** —
  libp2p's per-peer 1 s cooldown rejecting redials of peers
  whose connection bee already RST'd.
- **Median push latency: 60 ms.** When a push succeeds, it's
  sub-bee speed. The bottleneck isn't the protocol.
- **p95 push latency: 4.9 s.** 5 % of pushes take ~5 s because
  the receiving peer's accounting takes forever or its forwarder
  chain stalls. Concentrated on ~10 specific peers (30-48 % slow
  rate each).
- **70 of 256 sessions delivered receipts.** ~73 % of dialed
  peers were either dead-on-arrival or RST'd before we could
  push anything through them.

The smoking gun: the 73 % of sessions that don't deliver are bee
peers whose bin 0 is full. Bee accepts our TCP+handshake then
calls `notifier.Connected → Kad.Connected → SaturationFunc`,
which returns true (bin full), and bee disconnects with
`ErrOversaturated`.

### Peer-diversity sanity check (not the bottleneck)

We checked whether the gap could be peer diversity — 100 random
chunk addresses → 100 / 100 of them have a peer in our pool at
PO ≥ 9 (inside bee's storage radius of 9). So we're not short on
"close enough to the chunk" peers. The bottleneck was always
the bins our peers put us into.

### Multi-target search

The `vanity-overlay` subcommand brute-forces a nonce that
maximizes PO to one or more known target overlays. Two modes:

- **anchored** (one or more `--target-overlay`): maximize the
  *minimum* PO across the listed targets. Cheap when targets
  are themselves close (~`2^k` tries for PO≥k against
  prefix-sharing targets).
- **coverage** (no targets, uses `peers.json`): maximize the
  count of peers at PO ≥ `--target-po`. Bounded by uniform
  expectation `N × 2^-target_po`; can outperform by ~2-3× before
  diminishing returns.

For our identity key, anchoring against 5 peers from a previous
upload's top-pushers list (sharing the `d9` prefix in the
trace data) found a nonce that gives PO ≥ 8 to all 5 of them
in **442 attempts (instant)**:

    POs = [8, 8, 8, 8, 9]    min = 8
    overlay = d9dd0fcc640528946cb6225517b6f72ef11616149ac0db3fc8fdf46bd78f3c8f

### Measured impact

5 MiB random upload via daemon, 10 warm runs each:

| Config | Median KiB/s | Best | vs bee-light (822) |
|--------|-------------:|-----:|-------------------:|
| Random overlay (today) | 120 | 159 | 14-19 % |
| Single-target vanity (PO=13 to 1 peer) | 215 | 336 | 26-41 % |
| **Multi-target vanity (PO≥8 to 5 peers)** | **269** | **349** | **33-42 %** |

Single-target alone is **2.0× over random**. Multi-target is
**2.2× over random + 25 % over single-target**, with lower
variance (worst run 153 vs single-target's 103).

### Caveats

- Anchors must be **stable peers**. If the anchor goes offline,
  the vanity advantage evaporates. Multi-target trades
  per-anchor PO for redundancy.
- Anchors must be **chosen empirically.** The best ones come from
  the top-pusher list of a previous upload run on the same key.
  Picking arbitrary peers from a discovery dump gives no benefit
  — they may be dead/NATted/light-mode.
- The advantage is **per-peer, not network-wide.** Our vanity
  overlay is at PO=8 to the 5 anchors, but at PO≈0 to most
  random peers. So bee peers not in our anchor list still drop us
  on dial. The win is that our top performers stay connected.

### Workflow

    # 1. Run a normal-overlay upload to populate peers.json and
    # generate a trace with top-pusher info.
    isheika daemon --identity 0xXXX --advertise ... -v ...

    # 2. Identify the top 5-10 peers from the daemon log
    # (`pushed N/M chunks (latest via <overlay> po=K)` lines,
    # or the `do_pushsync_outer` traces if RUST_LOG=isheika::profile=trace).

    # 3. Run vanity-overlay search against those targets:
    isheika vanity-overlay --key 0xXXX \
      --target-overlay <peer1_overlay> \
      --target-overlay <peer2_overlay> \
      --target-overlay <peer3_overlay> \
      --target-po 6 \
      --output overlay-nonce

    # 4. Restart daemon with the new nonce file:
    isheika --nonce-file overlay-nonce daemon ...

Higher target_po = stronger anchor but exponentially more search
cost. Above ~12-14 the search starts taking minutes; above ~20 it
becomes infeasible without distributed search.
