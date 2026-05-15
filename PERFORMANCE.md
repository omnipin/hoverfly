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
- **Identify push during connection setup** (`transport.rs::prep_connection`).
  Send our identify info before bee waits the default ~10 s for it,
  cutting handshake-to-first-push latency from ~10 s → ~200-500 ms.
- **Reachability log + writeback** (`peers.rs::ReachabilityLog`,
  `apply_log`). Every operation collects dial outcomes into a
  thread-safe log; on completion (success or error) the CLI writes
  observations back to `peers.json`. Next run starts faster.
- **Address-space sketch in upsert.** Cheap leading-byte bucketing
  used both at pool-fill time and for diagnostic histograms.
  Detecting an uneven peerstore (a few bins with <5 peers) is the
  fastest signal that a random-content upload will stall: that's
  the slowest-chunk neighborhood.

## Known limits (not bugs)

- **Bee mainnet forwarding RTT** is the floor on novel-chunk upload
  speed. ~200-800 ms per chunk per hop is normal, more on long PO
  chains. At pool size 128 with buffer 128 that gives a theoretical
  ceiling of ~1 MB/s; empirically we see 70-120 KB/s on residential,
  ~120 KB/s on a Fedora VPS to mainnet. The protocol is the
  bottleneck, not the client.
- **Bee dedup masks performance.** Re-uploading the same bytes hits
  bee's "is within AOR" short-circuit (`pushsync.go:295`) without
  forwarding. Always test with freshly-randomised content; a re-run
  of an earlier upload tells you nothing about real throughput.
- **Peerlist coverage of the address space** is the floor on
  random-content upload speed. A chunk whose address lands in a PO
  bin with <5 peers in your store will stall, no matter how many
  sessions you open. Fix is `discover --rounds 3+` to broaden the
  store, not code changes.
- **`--max-retries` is silently capped.** `client.rs:1372`:
  `cap = max_retries.max(1).min(order.len())`. `0` is promoted to
  `1`; the value is capped by the live (non-parked) pool size, so on
  a small or attrited pool the user-supplied number is an upper
  bound, not a guarantee.
- **The 128-buffer cap doesn't scale with pool size.** Raising
  `--concurrency` past ~128 doesn't increase in-flight throughput on
  its own, because every chunk fans out through the same
  global buffer. Scaling the buffer with pool size is a potential
  future win (rough estimate: ~2× throughput on a 128-session pool),
  blocked on re-measuring whether the per-session accounting
  contention from the earlier `pool × 16` experiment still
  applies after the timeout-doesn't-retire fix.
