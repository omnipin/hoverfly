# Pusher: chunk-push relays on free compute

Status: **stages A+B implemented and deployed** (`src/pusher.rs`,
`src/pushframe.rs`, `client::push_via_pusher(s)`, the dApp relay path in
`apps/upload/`); stage C is partially there (client-side address-sharding with
failover exists; weighted rendezvous + budget accounting do not). This doc is
the single source of truth for the pusher subsystem; it captures decisions made
during the browser-performance investigation (mid-2026).

## 1. Why

The browser build of hoverfly is structurally capped on push throughput and no
amount of client-side tuning fixes it:

- Browsers can only dial bees exposing **wss AutoTLS** underlays — a sliver of
  mainnet (~7 usable hosts of the 48 that front all ~2,800 bees). Many of those
  underlays are stale or refuse connections.
- Browsers serialize/limit same-IP WebSocket opens (Firefox admission manager
  per resolved IP, global `network.websocket.max-connections=200`; Chrome
  per-IP:port endpoint lock), and mainnet bees cluster behind few IPs.
- Result: pools of 5–25 live sessions, 1–3 chunks/s, retry storms and tail
  stalls — versus 500 KB–1.1 MB/s for the same code natively.

A **pusher** is a small relay that accepts pre-signed chunks over HTTPS and
pushes them into the swarm over real TCP libp2p, restoring native reach to
browser (and constrained-network) clients. Pushers are deliberately shaped to
run on **free serverless/container tiers**, so anyone can host lanes at $0.

What a pusher is *not*:

- **Not a gateway.** It serves no retrieval over HTTP and opens no IPC socket.
  It pushes chunks; it never serves content to the web. (Serving cached chunks
  to *peer bees* over libp2p retrieval — `src/inbound.rs` — is normal protocol
  citizenship and stays as-is.)
- **Not a signer.** Keys never cross the wire. The client does all chunking,
  BMT hashing, and stamp signing locally (dApp session key or native wallet);
  the pusher only ever sees pre-signed material. There is nothing secret on a
  pusher to steal.
- **Not a store.** Its `ChunkCache` is a local dedupe/serving cache, same as
  the daemon's.

## 2. CLI surface

```
hoverfly pusher --listen 0.0.0.0:8550 [flags]        # new subcommand
hoverfly upload --peerlist peers.json …              # one-shot (existing)
hoverfly upload --daemon /tmp/hoverfly.sock …        # via daemon IPC (existing)
hoverfly upload --pusher https://a.example --pusher https://b.example …
                                                     # via pusher lanes (new, repeatable)
```

`pusher` flags (all optional; defaults = open mode, generous):

| Flag | Default | Meaning |
|---|---|---|
| `--push-allow 0xA,0xB` | off | Allowlist mode: recovered stamp signer must be listed. No RPC needed. |
| `--rpc-url` | required unless `--push-allow` | Gnosis RPC for batch-alive checks (cached per batchID). |
| `--push-quota` | off | Per-batch capacity quota: ≤ batch effective volume per TTL (see §6). |
| `--push-challenge` | off | Require a signed nonce per upload session (replay hardening, §6). |
| `--push-max-mbps` | off | Global egress cap — the "worst day" bound on free tiers. |
| `--push-verify-sample N` | 1 (verify all) | Verify 1-in-N stamp signatures (CPU lever for Workers, §9). |
| `--pool-size` | profile default | Sessions toward bees (16 persistent, 5 workerd). |

## 3. Wire protocol: streamable HTTP

One transport, three endpoints. Chosen over WebSocket and WebTransport after
dedicated research — rationale recorded in §4.

### Frame format

A push body is a concatenation of frames:

```
addr(32) | stamp(113) | wire_len(u16 LE) | wire(≤ 4104)
```

- `addr` — chunk address (BMT root of the wire content).
- `stamp` — bee wire stamp, `[batchID:32][index:8][timestamp:8][sig:65]`
  (see `src/stamp.rs`).
- `wire` — span(8) + data(≤4096), exactly what goes into pushsync.

The frame format is transport-agnostic on purpose: WS or WT could later carry
the same frames as additional bindings if a need materializes.

### Endpoints

**`POST /v1/push[?receipts=1]`** — body = frames, `Content-Type:
application/x-hoverfly-frames`. Response is **streamed NDJSON**, one line per
chunk as its push resolves (not end-of-batch):

```json
{"a":"<hex addr>","s":"ok"}
{"a":"<hex addr>","s":"dup"}                          // cache hit, not re-pushed
{"a":"<hex addr>","s":"ok","r":"<hex receipt sig>"}   // with ?receipts=1
{"a":"<hex addr>","s":"err","e":"overdraft"}
```

Client sends ~`batch_max` chunks per POST with 2–4 POSTs in flight per lane.
Retry = re-POST unacked frames; the protocol is connectionless, nothing to
resume.

**`GET /v1/status`** — JSON advertisement the client scheduler consumes:

```json
{
  "version": "…",
  "profile": "persistent | request-scoped | workerd",
  "batch_max": 256,
  "inflight_max": 4,
  "budget_remaining_gb": 61.4,    // monthly egress budget left (null = unmetered)
  "load": 0.3,
  "modes": {"allowlist": false, "quota": false, "challenge": false}
}
```

**`GET /v1/challenge`** — only when `--push-challenge`: returns a nonce; the
client signs `(nonce ‖ batchID)` with the same session key that signs stamps
and presents it as a header on subsequent POSTs.

### HTTP hygiene (table stakes)

Body-size cap (`batch_max × 4249` + slack), per-request timeout, connection
limits, `429` with `Retry-After` when saturated or quota-drained.

## 4. Transport decision record

**Streamable HTTP (batched POST up, incrementally-flushed NDJSON down) is the
sole v1 transport.** The same pattern MCP standardized. Why the alternatives
lost (research as of mid-2026):

**WebTransport — ruled out for v1:**
- Baseline "newly available" only since Safari 26.4 (March 2026); installed
  base ≈ 75%, "widely available" not until ~late 2028.
- Firefox `serverCertificateHashes` broken (bug 1873263) → no certless WT.
- QUIC is UDP/443; corporate/hotel networks silently drop it → an HTTP
  fallback is mandatory anyway, so WT-only was never on the table.
- Cloudflare can neither run WT in workerd (workerd#6451: no QUIC stack, "not
  on the roadmap") nor even proxy WT to origins — the very edge that was
  supposed to guarantee modern transport can't carry it.

**WebSocket — examined, rejected:**
- Duplex is wasted: downstream is only acks, and a streamed POST response
  delivers those live per chunk.
- Browsers speak WS over HTTP/1.1 in practice (RFC 8441/9220 support patchy) →
  forfeits the H3 edge leg; CF proxy idle timeout (~100 s) forces
  ping/keepalive/reconnect state machines the protocol doesn't need.
- Stateless per-frame stamp auth means there is no session worth keeping
  alive. AWS API Gateway WS (128 KB messages, per-message billing) is the
  wrong shape for MB/s bulk push.

**The HTTP/3 story:** put any pusher behind plain orange-cloud Cloudflare DNS
and the browser→edge leg of every POST rides H3/QUIC automatically — 0-RTT, no
head-of-line blocking — with zero protocol work. Parallel POSTs sidestep
single-connection HoL on the fallback path too. And the whole thing is
curl-able (`curl --no-buffer`) and survives every proxy that mangles WS
upgrades.

## 5. Auth: the stamps are the credential

No bearer tokens. Every stamp is a secp256k1 signature over
`keccak(addr ‖ batchID ‖ index ‖ ts)` by the **batch owner key**, and the
pusher parses stamps anyway — `ecrecover` yields the pusher's notion of
identity for free (~80 µs/chunk native).

**Default (open mode): "the batch is alive" is the auth.** Stamp signature
must recover to the on-chain owner of its `batchID` (that *is* stamp validity
— a stamp can't be detached and reused on a different chunk, the address is
under the signature), and the batch must be alive (`remainingBalance > 0`).
One cached Gnosis RPC call per new batchID.

**Optional hardening, all mechanism-ready but off by default:**

- `--push-allow` — allowlist of signer addresses; zero-RPC private pusher.
- `--push-quota` — per-batch capacity quota (§6).
- `--push-challenge` — proof-of-liveness nonce (§6).

Multi-pusher benefit: N pushers authenticate the same client statelessly from
the same signatures — no token distribution, no shared config.

## 6. Threat model (open mode)

| Threat | Reality | Mitigation |
|---|---|---|
| **Quota drain via dust batch** | A funded batch does **not** bound bytes: a ~$0.02 mutable batch signs unlimited stamps, so batch-alive-only prices *identity*, not *traffic*. | Accepted for free tiers: worst case = the platform's free egress for the month (~70–100 GB) burned, $0 lost. The escalation knob exists: `--push-quota` caps each batch at its **own effective volume per TTL** (depth 18 ≈ 6.5 MB, depth 20 ≈ 670 MB) — the pusher's pricing becomes Swarm's pricing. **Do not run batch-alive-only on metered/paid deployments.** |
| **Stamp replay (quota-griefing)** | Stamps are public; anyone who saw your chunks holds valid `(addr, stamp, wire)` triples. | Re-pushing known chunks is idempotent; `ChunkCache` dedupe answers `dup` for free. If quotas are on, `--push-challenge`: replayers can't sign a fresh nonce with the session key. |
| **Garbage (invalid stamps/wires)** | Costs pusher CPU + egress; bees reject invalid stamps, so push credit is never burned. | Validate before push: BMT-recompute addr from wire + stamp sig + batch alive (~20 ms/MB native). `--push-verify-sample` trades CPU for egress waste where CPU is the binding constraint (Workers). |
| **Content liability** | The pusher originates chunk bytes toward the swarm from its IP — Tor-exit-class residual risk; no cryptography removes it. | Attribution converts "anonymous abuse from your IP" into "abuse attributable to a chain address with a BZZ funding trail": log `(owner, addr, time)` (JSONL), keep an owner blocklist. Push ≠ serve (mere-conduit posture); run on cloud IPs, not home IPs. |
| **Bee-credit exhaustion / blocklist** | Attacker saturating the pusher could overdraw its peers. | Already handled by the shared push path (payment-threshold mirroring, ghost-balance session retirement — same code as native uploads). Bound the blast radius with `--push-max-mbps`. |
| **Targeted amplification** | Chunk addresses are minable → aim traffic at one operator's neighborhood. | Amplification is only ~1–2× at race=1 and capped by the egress budget/quotas. |

Recommended postures: **free-tier public pusher** = defaults (batch-alive
only) + `--push-max-mbps`; **paid/metered pusher** = `--push-quota
--push-challenge`; **private pusher** = `--push-allow`, no RPC at all.

## 7. Client scheduler: lanes + weighted rendezvous

The client pushes to **multiple pushers in parallel** and splits chunks by
address. Racing lives client-side; **pushers always push each chunk once**
(race=1 internally).

**Assignment — weighted rendezvous hashing.** For each chunk, score every
healthy lane with `hash(addr ‖ lane_id) × weight`, send to the max. This buys:

- **Sticky by address** — retries can't double-spend quota across platforms;
  lane-side `ChunkCache` dedupes across retries and repeat uploads.
- **Weight-proportional load** — chunk addresses are hash-uniform, so load
  follows weights exactly. Weights = f(advertised `budget_remaining_gb`,
  `batch_max`, EWMA of observed ack throughput). A drained/erroring lane
  decays toward 0 without remapping everyone else's chunks.
- **Deterministic fallback order** — rendezvous score rank #2 is a chunk's
  designated straggler lane.

**Shard-first, race-on-straggle.** Default: each chunk goes to exactly one
lane. A chunk unacked past its deadline is re-dispatched to its #2 lane
(different platform, different egress /32, different session pool) — the same
escalation the in-client dispatcher does today across peer sessions, one level
up, and a structural fix for the tail-stall pattern (stragglers currently
retry against the same decayed pool). Blanket k>1 racing burns aggregate quota
k× and is not the default.

Mechanics: one queue per lane, flush at the lane's `batch_max` or a short
timeout, 2–4 POSTs in flight per lane, NDJSON acks feed the existing per-chunk
state machine. Upload completes when every chunk is acked by some lane.
Cross-platform egress ≈ payload × ~1.0–1.2 (only stragglers transit twice) vs
~4× under today's in-client 3-way racing.

The dispatcher, inflight caps, deadline and retry accounting all exist in
`src/client.rs` — lanes slot in as high-capacity sessions; rendezvous is ~20
lines.

**Pusher-side adaptive pool bias (zero protocol).** Push debt per chunk scales
with the distance from the pusher's closest pooled bee to the chunk address,
and per-peer debt is what retires sessions (overdraft/ghost-balance). Under
rendezvous each pusher consistently receives the *same* pseudo-random ~1/N of
the address space — so it can observe arriving addresses and bias pool top-ups
toward bees whose overlays match that distribution: less debt per chunk →
longer session lives → higher sustained throughput. Contiguous keyspace arcs
(consistent-hashing style) would specialize deeper; deferred until receipts
show forwarding depth actually costs us (§11).

## 8. Runtime profiles

The insight that makes serverless viable: **the warm pool was never
load-bearing — the peer cache is.** A cold one-shot upload hits ~1.06 MB/s
with a ~2–5 s pool fill from the cache, and the cache is already externalized
state (CI-refreshed `peers.seed.json`, fetchable from GitHub raw/CDN). So a
pusher can be: frames in → fill small pool from CDN cache → push → stream acks
→ die.

| Profile | Where | Shape | Notes |
|---|---|---|---|
| **P0 persistent** | VPS, Render container | `hoverfly pusher` daemon-style: warm pool, maintenance tick | Reference deployment; zero new code beyond the subcommand |
| **P1 request-scoped** | AWS Lambda | Same native binary + thin `lambda_http` streaming adapter (custom runtime); pool per invocation; container reuse gives incidental warm pools between batches | 15-min cap vs ~70 s per big batch — fine |
| **P2 workerd** | CF Workers (later Deno Deploy) | wasm + a **JS-socket transport backend**: one Rust module against an abstract JS TCP-duplex (template: `src/wsws/`), ~50-line shims per platform (`connect()` / `Deno.connect` / `net.Socket`) | 6 sockets/invocation → DO sharding; 3 MB-gzip script limit is tight but passes |

Per-profile tunables (advertised via `/v1/status`, not hardcoded client-side):
batch 256 / pool 16 on P0–P1; batch ~32 / pool ~5 on P2-free.

## 9. Free-tier capacity (planning figures, mid-2026)

Assumptions: egress ≈ payload × amplification (~1.4× at race=1 incl. protocol
overhead/retries; in-pusher racing is off by design); push CPU ≈ 1 core-sec/MB
(measured, VPS); ingress free everywhere.

| Platform | Binding constraint | Payload/mo | Speed | Port effort |
|---|---|---|---|---|
| **Render free** | 100 GB/mo egress | **~70 GB** | 0.1 vCPU → ~0.1–0.2 MB/s lane; 30–60 s cold wake | **zero** (Dockerfile) |
| **AWS Lambda free** | 100 GB/mo egress (compute ≈ 226 GB-worth, never binds) | **~70 GB** | ~1 MB/s **per invocation**; concurrent POSTs = horizontal scale (default 1000-concurrency cap) | low (~a day) |
| **CF Workers free** | 10 ms CPU/req + 100k req/day; egress **unmetered** | **~100 GB-class** (ecrecover ≈ 0.7–1 ms wasm → ~35–40 KB/req; `--push-verify-sample` ≈ 2×) | per-isolate: client concurrency = horizontal scale | medium (the P2 port) |
| **CF Workers $5** | 30M CPU-ms/mo included | **effectively unbounded** (egress stays free) | same | same |
| Deno Deploy | 100 GB/mo egress | ~70 GB | ~50 ms CPU/req class | shares ~90% of P2 |
| Vercel Hobby | 100 GB/mo; **non-commercial ToS** | ~70 GB | Lambda-like | personal only |
| Netlify / Fly.io / Cloud Run | — | — | — | cut (wrapped Lambda w/ worse limits; no free tier; 1 GB/mo egress) |

Aggregate: Render + Lambda + free Worker ≈ **~240 GB/mo of payload for $0**,
summed by the lane scheduler with near-zero duplication.

Numbers to re-verify at implementation time: Render's 0.1-CPU figure, Workers
request math, Deno CPU/req — free tiers drift.

## 10. The standing gate: shared cloud egress IPs

Bees rate-limit inbound per source-/32, and every platform above dials from
shared cloud ranges. **If bees throttle AWS/CF egress ranges, serverless
pushers die regardless of runtime.** Nothing but an experiment answers it:
deploy the current binary to Render (zero code — Dockerfile + `hoverfly
pusher`… or even the existing one-shot upload), push a blob, read the
push-outcome counters. This is the **first task** of the pusher work, not the
last; a bad result reshapes §8–9 before any adapter is written.

## 11. PoC plan

### Platform picks (final)

| Lane | Platform / region | Why |
|---|---|---|
| 1 | **Render free web service** — Docker runtime, **Frankfurt** | P0 persistent reference on free compute; Frankfurt because mainnet bees are Hetzner-dominated (FSN/HEL) → single-digit-ms RTT to most of the swarm |
| 2 | **AWS Lambda** — **eu-central-1**, arm64, Function URL with response streaming, deployed via cargo-lambda | P1 request-scoped; the lane that matters long-term (per-invocation concurrency = horizontal scale) |
| — | CF Workers / Deno Deploy | **not in the PoC** — P2 is a transport port, only worth it after the protocol is proven |
| — | Vercel | cut (non-commercial ToS, nothing over Lambda) |

Lambda memory setting: **1769 MB = exactly 1 vCPU**. Push costs ~1 core-sec/MB,
so smaller settings throttle throughput proportionally (512 MB ≈ 0.29 vCPU ≈
0.3 MB/s). At 1769 MB a 10 MB push ≈ 18 GB-s → the 400k GB-s/mo free compute ≈
226 GB-worth — egress (100 GB) still binds first.

### Reality check that shapes stage A

The crate has **no HTTP server today** (reqwest is client-only; the daemon is
unix-socket IPC), Render free web services must bind `$PORT` and pass health
checks, and Lambda needs an adapter regardless — so a truly zero-code gate
experiment does not exist. The honest minimum is a small HTTP skeleton;
therefore the PoC makes that skeleton the pusher's actual first commit.

### Stage A — pusher skeleton + the gate experiment (~1 day of code)

New `pusher` cargo feature (hyper 1 + hyper-util + http-body-util; native-only,
wasm builds untouched) and the `hoverfly pusher` subcommand serving just:

- `GET /v1/status` — doubles as the platform health check.
- `POST /v1/probe?size=10485760` — **flag-gated (`--probe`), the experiment
  endpoint**: generate a random blob of `size`, stamp it with an env-provided
  throwaway key/batch (`HOVERFLY_PROBE_KEY`, `HOVERFLY_PROBE_BATCH`), run the
  standard push path (`push_chunks_with_pool`, pool from the bundled
  CI-refreshed peer cache), and stream back a JSON report: MiB/s, attempts,
  error histogram (overdraft/shallow/timeout/refused), **per-/32 error
  clustering**, session lifetime distribution.

Probe mode is the one deliberate exception to "not a signer" — it signs with
its *own* env key against a dust batch, exists only for self-testing, and is
off by default. It stays on the finished pusher as a diagnostics endpoint.

Deploy artifacts (in-repo): multi-stage `Dockerfile` (rust builder →
debian-slim + `peers.seed.json`) + `render.yaml` (free plan, Frankfurt, health
check `/v1/status`); `cargo lambda build --arm64` + Function URL (streaming,
auth NONE) for lane 2.

**Experiment protocol:** paired-alternating runs, same hour — 6 × 10 MiB
probes on Render, 6 on Lambda, 6 on the VPS as baseline (same methodology as
the top-up A/B). **Pass:** cloud lane ≥ 0.5 MiB/s AND attempt-error rate ≤ 2×
VPS AND no per-/32 hard-refusal signature (a farm refusing cloud IPs while
serving the VPS). **Partial throttling:** identify which farms, haircut the §9
capacity math. **Hard fail:** serverless lanes are dead → pivot to $2–3/mo
micro-VPS lanes (Fly/Hetzner) and P0-only; §8–9 rewritten.

### Stage A results — measured 2026-07-05 (Render free, Frankfurt, vs VPS)

Ran the probe on Render's free tier against the VPS baseline. Verdict:
**cloud pushing works and is correct; the free-tier throughput ceiling is a
per-/32 dial-rate limit, not a block.** Three findings, in the order they were
untangled:

1. **Raw TCP egress is clean — not Render's firewall, not network-layer IP
   blocking.** A `/v1/tcpcheck` sweep (raw `TcpStream::connect`, no libp2p)
   was byte-identical from Render and the VPS: 20/20 to every live bee port
   and to a VPS-owned listener; the only failures were dead ports that fail
   for everyone. Whatever throttles us is at bee's application layer.

2. **Overlay oversaturation was the dominant throughput killer — premining
   fixes it.** The first Render runs used a *random* overlay (no persisted
   nonce on the ephemeral FS) and cratered at 0.018 MiB/s with `push_shallow`
   ~1524 — bee dropping us from its full bin 0 (`ErrOversaturated`, §"Vanity
   overlay" in PERFORMANCE.md). With an identical premined overlay to the VPS,
   Render jumped to **0.065 MiB/s (VPS-parity) and shallow fell to 24**. This
   is why cloud pushers **must** premine both overlay nonce
   (`HOVERFLY_OVERLAY_NONCE`) and libp2p identity (`HOVERFLY_PUSHER_IDENTITY`,
   separate from the stamp key) — a random overlay per boot is a config bug,
   not a platform limit.

3. **A residual per-/32 dial-rate limit caps single-shared-IP pool size.**
   With overlay controlled, Render still logged thousands of failed dials vs
   the VPS's ~0 to the same hosts — bee's `SubnetRateLimiter` (`libp2p.go`:
   RPS 10, burst 40 per /32; also a 200-conn/​/32 cap) rejecting our burst
   pool-fill. Effect: Render's pool starves at **~8–35 live sessions** vs the
   VPS's **76+**, ceiling-ing high-load throughput at **~0.05 MiB/s (~7×
   below VPS)**. It never breaks correctness — every chunk still pushes — and
   at low load a handful of good-bin sessions carry the work at parity.
   Confirmed as *source-IP*, not our own code: same binary/overlay/peers, VPS
   unaffected. A dedicated IP (VPS) has the full per-/32 budget; a shared
   cloud IP does not.

**Aggregation (two free pushers, distinct node identities + vanity overlays):**
solo 0.063 + 0.075; concurrent combined **0.094** (vs ~0.07 for one alone).
Aggregation is **real and net-positive (~1.35×) but sublinear** on the same
provider — Render-1 held its full rate while Render-2 halved, consistent with
partial shared-egress-/32 contention (exact IPs uncapturable behind the VPS's
firewalld). The clean *linear* case is already proven by VPS-vs-Render: fully
independent budgets across different IP ranges. **Design consequence:**
multi-pusher scaling is best across **different providers / IP ranges**, and
the client scheduler (§7) should prefer IP-diverse lanes; stacking on one
provider still helps but doesn't scale linearly.

**Net:** Render grade = works, correct, volume-capable (~70 GB/mo egress-bound),
single-IP-throughput-limited (~0.05 MiB/s). Throughput is a client-side
multi-lane concern (§7, stage C), not a single-host property. AWS Lambda's
per-invocation egress-IP diversity (the natural fix) is untested — no account.

### Stage B — the protocol end-to-end (~2–3 days) — **implemented**

- `POST /v1/push`: frame decode → validation (BMT recompute, stamp sig,
  cached batch-alive RPC) → push → streamed NDJSON acks. Open mode only; no
  quota/challenge/allowlist yet.
- `upload --pusher <url>` **single-lane** (no rendezvous yet): frame encoder,
  batched POSTs, ack-driven completion, straggler re-POST to the same lane.
- dApp: pusher URL config + fetch/ReadableStream ack parsing.
- Batch of 256 frames ≈ 1.1 MB — comfortably under Lambda's 6 MB request cap.

**Success metric:** the 71 MB browser video upload sustains **≥ 0.5 MB/s
through the Lambda lane** (vs 1–3 chunks/s direct today) with the key never
leaving the browser.

### Stage C — multi-lane (~1–2 days)

Weighted rendezvous scheduler (§7), status-weighted lanes, straggler
re-dispatch to rank-#2 lane, `budget_remaining_gb` accounting. Ship as the
default browser push path: Render + Lambda lanes.

### Deferred / watchlist

- PR-sized follow-ups: `--push-quota`/`--push-challenge` hardening, P2
  workerd port (unlocks Deno Deploy), attribution-log tooling.
- Contiguous-arc lane assignment + deep pool specialization — only if receipt
  data shows forwarding depth is a real cost (§7).
- WS/WT bindings of the frame format — only on demonstrated need (§4).
- Edge control plane (Worker doing ecrecover/quota/routing in front of dumb
  push origins) — optional sugar if a public pusher federation ever forms.
- Upstream watch: if bee ever ships WebTransport listeners, browsers could
  dial storage nodes directly and the pusher's raison d'être shrinks to
  constrained networks.

Deferred/watchlist:
- Contiguous-arc lane assignment + deep pool specialization — only if receipt
  data shows forwarding depth is a real cost (§7).
- WS/WT bindings of the frame format — only on demonstrated need (§4).
- Edge control plane (Worker doing ecrecover/quota/routing in front of dumb
  push origins) — optional sugar if a public pusher federation ever forms.
- Upstream watch: if bee ever ships WebTransport listeners, browsers could
  dial storage nodes directly and the pusher's raison d'être shrinks to
  constrained networks.
