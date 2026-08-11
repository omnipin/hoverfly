---
marp: true
theme: default
paginate: true
header: 'Paying for relay — SWAP cheques for hoverfly pushers'
---

# Paying for relay

## SWAP cheques for hoverfly pushers

Theory, and what happened when we ran it

<br>

*Companion to `docs/pusher-incentives.md`. Stages 0–1 shipped; one metered lane in production.*

---

# The problem: the relay eats a cost it did not incur

In a **native** upload, your own machine opens the pushsync streams. Bee debits *you*:

```
price(po) = (32 − po) × 10 000    accounting units
```

Put a relay in the middle and that debt moves **wholesale to the relay** — it is the peer bee sees, so it is the peer bee charges.

The browser client that caused the traffic pays **nothing but postage**.

> Today this is booked as accepted risk: *"worst case = the platform's free egress for the month burned, $0 lost."*

---

# What changes, precisely

| | open (today) | metered |
|---|---|---|
| client → relay | nothing | `4.8e8` PLUR per KiB of body sent |
| relay → bee | free pseudosettle | **unchanged** — free pseudosettle |
| relay's egress | unrecovered | recovered above the cashout threshold |

Relay→bee settlement stays free because the relay is **session- and RTT-bound, not credit-bound**: bee grants 4.5e6 accounting units/s per peer, ≈ 2 400 chunks/s across a 128-session pool, against **~150 chunks/s** actually measured. Buying credit buys nothing.

> The recovered amount is small. A relay pushing 100 GB of egress a month is moving ~27 GiB of payload, which at $0.02/GiB is **$0.54**.

---

# Part I — Theory

---

# Everything follows from one asymmetry

> **The client chose its relay. The relay did not choose its client.**

A relay is a plain HTTP service. Anyone can run one; there is no registry, no discovery, no allowlist to be admitted to. What creates the asymmetry is **pinning, not curation**: before it sends a byte, a client verifies the signed quote and pins `(url, node_eth_address, beneficiary)`. The relay has no equivalent — a client is whoever POSTs.

**So every defence the relay has points at the client**, and the relay's goal is: *a client cannot obtain service without paying, and cannot lie about what it owes.*

The client's protections are different in kind — arithmetic and exposure limits, not authentication. Next slide.

---

# What protects the client, then

Not cryptography. Four bounds, none of which need the relay to be trustworthy:

- **It works out the bill itself.** The client adds up the bytes it sent. If the relay reports a bigger number, the client sees it immediately — and knows the relay is the one that is wrong (§8.4).
- **The price is fixed in advance.** It arrives in a quote the relay signs, and the same signed quote comes back with every refusal — so the price cannot move mid-upload.
- **The most it can lose is one credit limit** — about $0.0024 at the maximum, and far less on a small batch, which gets a thousandth of whatever it is still worth.
- **It watches whether chunks actually arrive.** A relay that takes bytes and delivers badly gets sent less work, using the running average that already picks between relays today.

> The curated-set premise was hiding a gap in the fourth: on an unpayable 402 the client **adopts the relay's own `owed`**, and that used to be bounded only by the chequebook balance. It is now bounded by `max_outstanding_plur` from the quote the relay signed — the credit it granted is the most it can claim.

---

# What the asymmetry corrected

Three rounds of adversarial review. The most important finding was structural, not local:

> The design was building **two-sided cryptographic verification** for a **one-sided trust relationship.**

Roughly half of it defended the client against the relay — against lanes we run ourselves. Three concrete costs:

- **The bill was built on someone else's signature** — a receipt from a bee node. Those are easy to fake, so every one had to be checked against the on-chain list of staked nodes.
- **The bill was a list, not a number**, and nothing limited how long that list could get.
- **One unmeasured number could have killed the project**: how many receipt signers are actually staked. Nobody had checked.

---

# What we borrow from SWAP

| Borrowed | Why |
|---|---|
| `ERC20SimpleSwap` + canonical factory | Audited, deployed, in production. Nothing to write. |
| EIP-712 cheque | `Cheque(chequebook, beneficiary, cumulativePayout)` |
| Cumulative-payout monotonicity | Loss-tolerant *and* replay-proof |
| Funding check | …but `liquidBalanceFor(us)`, **not** `balance()` — bee's version is unsound |
| Reservation against concurrent issuance | Needed on **both** sides |
| Payee-only role | The beneficiary is a plain **EOA**. A payee needs no contract. |

Gnosis factory: `0xc2d5a532cf69aa9a1378737d8ccdef884b6e7420`

---

# What we drop

| Dropped | Reason |
|---|---|
| swap libp2p stream, `Handshake`, `EmitCheque` | We're on HTTP already |
| priceoracle, `exchange`, `deduction` | Relay quotes PLUR directly |
| accounting-unit indirection | One unit. No conversion. |
| ghost balances, tolerance, trust ramp | No analogue in request/response |
| `StakeRegistry` snapshot | Nothing left to anchor — see next section |

> **Both counterparties are hoverfly.** Bee is not a party to the payment. We borrow SWAP's *contracts and cheque format*, not its protocol.

---

# Identity: the account is the batch owner

The relay's existing auth already establishes on-chain identity for every push: the stamp signature must recover to the address `PostageStamp` reports as the batch's owner.

> **Account = the batch-owner EOA.**
> A cheque is valid for that account **iff** its chequebook's on-chain `issuer()` equals the same EOA.
> **Credit is keyed one level finer — on the *batch*.**

No session tokens. No registration. No extra protocol message.

In a browser: the session key already owns the batch, so cheques sign with **zero wallet prompts**.

---

# The central decision: bill bytes admitted

> **`owed = (kib_admitted − kib_dedup) × price_plur_per_kib`**

That is the whole billing rule. One property matters more than everything else in the design:

> **The client cannot lie about it — the client produced the bytes and the relay counted them.**

- Nothing to forge — no outside signature is part of the bill
- Nothing to replay — the relay counts arrivals, not tokens
- Nothing to look up on chain
- Nothing the client has to take on the relay's word

Both numbers are known to both parties *before* any push work happens.

---

# What that replaced

The earlier draft billed per **verified pushsync receipt**. To make a *third party's* signature into a billing input, it needed:

- check every receipt signer against the on-chain staking list, or faking one is trivial
- make the bill a list instead of a number, or the same receipt gets submitted twice
- spot-check by fetching chunks back, to catch receipts for work never done
- sweep the staking logs on both sides, and keep them in step
- share one piece of code that decides whether a receipt counts — and **if the two copies ever disagree, nobody can settle it**

Changing the unit **deleted the entire apparatus and every attack against it.**

> Receipts are still forwarded — as *telemetry*, feeding lane weighting. They do not enter the invoice.

---

# Bytes, not successful pushes

Bytes are what the relay **spends money on**.

Egress is incurred on *attempts*: the 3-way peer race and the shallow retries happen whether or not a chunk lands.

Billing successes would mean the relay eats the cost of every failure.

Instead — two mechanisms, each doing what it is good at:

| Concern | Mechanism |
|---|---|
| Relay recovers what it spends | Bill attempts |
| Client protects itself from a lane that spends without succeeding | **Deweight it in the scheduler** (already exists, already works) |

---

# Dedup hits are billed at zero

A frame served from the recent-ack cache does no push work, so its bytes are subtracted.

This is the **one** place a relay assertion enters the bill — the relay claims *"this was a dedup hit"*.

It is safe for a structural reason:

> The claim only ever **lowers** the amount owed.

A relay has no incentive to make it falsely, and a client that disagrees is disagreeing in its own favour.

---

# Cumulative cheques

A cheque is a running total for one `(chequebook, beneficiary)` pair. Three properties fall out:

- **Losing one costs nothing.** Every cheque is a running total, so if a payment fails the next one covers it anyway. No retry logic needed.
- **Old cheques are worthless.** Each must be larger than the last, so sending an old one again pays nothing.
- **Gas is paid once per customer, not once per cheque.** The relay only ever cashes the newest total, so every earlier cheque costs nothing to collect.

**Per-chunk cheques are rejected:** ~137 B per 4 KiB chunk, an EIP-712 signature on the hot path, and cumulative payouts are *serial* per `(issuer, beneficiary)` pair — which forces a total order on chunks within a lane, removing the concurrent multi-POST pipelining the scheduler depends on.

---

# One subtlety that bricks a real deployment

A cumulative is per `(chequebook, beneficiary)`. A **lane** is a URL.

One operator running four lane URLs behind one beneficiary EOA is the obvious deployment.

If the client tracks cumulatives **per lane**, that configuration bricks:

```
lane 1 issues cumulative 10
lane 2, counting from its own zero, issues 8
relay applies ErrChequeNotIncreasing → rejected, forever
```

> **Key the client's cumulative store on `(chequebook, beneficiary)`** — not on lane, not on overlay.

Detecting the sharing is free: the beneficiary is in the signed quote.

---

# Wire protocol

Push frames are **unchanged**. Payment is out-of-band — it must not sit on the hot path, and a payment failure must not fail a push.

| Endpoint | Shape |
|---|---|
| `GET /v1/status` | signed `payment` block; `mode: open \| metered` |
| `GET /v1/challenge` | `{nonce, expires_ms, max_outstanding_plur}` — stateless MAC |
| `GET /v1/account` | authenticated. `owed`, `reserved`, `outstanding`, `kib_admitted`, … |
| `POST /v1/pay` | body = `SignedCheque` JSON |
| `POST /v1/push` | `402 Payment Required` when over cap |

`/v1/account` is authenticated because unauthenticated it is a per-identity **volume oracle** over on-chain-enumerable batch owners.

---

# Admission: why a challenge at all

The naive claim: *"402 is easy — `/v1/push` commits its status before processing any chunk."*

True, and that is exactly the **problem**: at that moment the relay does not yet know **whose account to check**. The account only exists after `stamp::validate` and `resolve_owner` — both inside the spawned task.

Hoisting them means up to **512 ecrecovers (~40 ms) plus an RPC round-trip synchronously in front of every response** — the precise unauthenticated amplification surface we spend a whole section defending.

---

# The challenge is a capability

Standing is resolved once when the challenge is **issued**. The credit line is baked into the nonce. `/v1/push` admission then reads **no chain state at all**.

```
GET /v1/challenge?account=A&batch=B
  → resolve standing(B)              (cached per batch, TTL)
  → require owner(B) == A, else 403
  → cap = credit_line(standing(B))
  → nonce = HMAC(relay_secret, preimage(A, B, origin, expiry, cap))
```

Admission becomes: verify MAC (constant-time) → verify origin → verify client signature → `reserve = ceil(len/1024) × price`; if `outstanding + reserve > cap` → **402, before reading the body**.

**Stateless.** No server-side nonce table, so a free `GET /v1/challenge` cannot exhaust memory.

---

# Two ways the binding silently becomes a no-op

**1. The preimage is fixed-width and domain-tagged, not a concatenation.**

`origin` is variable-length, so a bare concatenation makes `("host.a","bc")` and `("host.ab","c")` share a preimage — one nonce valid for two hostnames.

**2. `origin` must be *configured*, not derived.**

The obvious implementation compares the challenge's `origin` against the `Host` header. That is a **no-op** — `Host` is supplied by the same client supplying the challenge.

> An attacker replaying a victim's signature at relay B just sends `Host: relay-b.example`. The comparison passes. The cross-relay replay is restored **while the doc claims it is closed.**

---

# Deriving the credit line instead of asserting it

The tempting argument: *"an account is a batch owner, a live batch costs real BZZ, so the margin is three orders of magnitude."* **False.**

The relay checks **liveness**, and liveness is satisfied by the cheapest batch the contract accepts — minimum depth, minimum validity — a fraction of a cent. At a flat credit line the real Sybil margin is of order **1×**.

> **`max_outstanding(A,B) = min(remaining_value_plur(B) ÷ credit_ratio, max_outstanding_plur)`**
> with `credit_ratio = 1000`

The margin is now **1000× by construction, independent of batch size.** There is no cheap corner of the parameter space, because the *ratio* is the invariant.

---

# Parameters, and the invariant that must hold

> **`min_cheque_plur ≤ settle_every_plur < max_outstanding_plur`**
> A client that is 402'd must always be able to clear it with a cheque for exactly what it owes.

| parameter | PLUR | in payload |
|---|---:|---:|
| `price_plur_per_kib` | 4.8e8 | 1 KiB |
| `min_cheque_plur` | 3.9e12 | ~8 MiB |
| `settle_every_plur` | 1.56e13 | ~32 MiB |
| `max_outstanding_plur` | 6.22e13 | ~127 MiB (*ceiling*, not the cap) |

An early draft published `min_cheque` **87× larger** than `settle_every`. Every metered account would have bricked: accrue → cross → sign → **rejected as dust** → accrue → 402 → the only clearing cheque is 21× what is owed. **No exit.**

---

# Reservation: bee's `reserve` was needed after all

A monotone debit counter is **not** sufficient. `/v1/push` deliberately does not serialize, so N concurrent POSTs each read `outstanding` before any of them debits.

A *polite* client at the relay's own advertised `inflight_max` of 8 overshoots the cap on its own.

Fix: reserve `ceil(Content-Length / 1024) × price` **atomically at admission**, release the remainder at completion.

> **But `reserved` must not be persisted.** A reservation belongs to an in-flight POST, and no in-flight POST survives a restart — there is no task left to release it.

Persist `owed`, `last_cumulative`, the chequebook binding. **Reconstruct `reserved` as zero at boot.**

---

# Pricing: the cost basis

| | per 4 KiB chunk relayed |
|---|---|
| Delivery on the wire | ≈ 4.4 KiB |
| Peer race (`CHUNK_PEER_PARALLELISM = 3`) | **×3** |
| Shallow retries at pool 128 | **×1.15** |
| **Egress per chunk** | **≈ 15 KiB** |
| **Egress per GiB of payload** | **≈ 3.7 GiB** |

Suggested price **$0.02/GiB** — ~5× a VPS's raw bandwidth cost, ~18× cheaper than AWS egress.

> **`price_plur_per_kib ≈ 4.8 × 10⁸`**  (1 BZZ = 10¹⁶ PLUR)

Flat per KiB, deliberately: any curve steeper than flat re-introduces a per-item number for the two sides to disagree about.

---

# On per-GB-billed hosts, metering loses money

At 3.7 GiB of real egress per GiB of payload:

| | per GiB of payload |
|---|---:|
| AWS egress at $0.09/GB | **−$0.33** |
| Revenue at $0.02/GiB | **+$0.02** |
| Net | **−$0.31** |

So metered mode only clears cost on **flat-rate or included bandwidth**. A relay on per-GB egress should run `open` and absorb the quota.

That is the same host class already required for durable storage (§11.4), so the two constraints select the same machines.

---

# Revenue per account vs. cashout gas

Issuing a cheque sends no transaction. Only cashing out touches the chain: ≈ **$0.0005** on Gnosis.

| account's lifetime traffic | revenue | gas | gas as % |
|---|---:|---:|---:|
| 71 MB (one browser upload) | $0.0014 | $0.0005 | **36 %** |
| 5 GiB (cashout threshold) | $0.10 | $0.0005 | **0.5 %** |

Because cheques are cumulative, gas is paid once per **account**, not per cheque — so the ratio improves with every return visit, and accounts below the threshold are written off unclaimed.

A relay whose traffic is entirely one-shot uploads should run `open`.

---

# Attack surface — the three that matter

**Stamp replay becomes billing griefing** *(introduced)*
Swarm stamps are public, and a relay holds every stamp it ever relayed. Replay a victim's stamps at a metered relay and the work bills to the *victim*. Cost to attacker: zero.
→ Closed by the **account-signed** challenge + one batch per POST.

**The withdraw race** *(inherited)*
Chequebooks deploy with a hard-deposit timeout of zero, so the balance stays liquid. The funding check is true **at acceptance time, not at cashout time.** Bee has the identical exposure.

**Relay state loss is an unbounded free-service loop** *(introduced)*
An ephemeral filesystem turns one signature into unlimited free service.
→ Durable storage is a **requirement**, not a recommendation.

---

# Part II — Practice

---

# Modes, and the optionality rule

| relay mode | client has a chequebook | client does not |
|---|---|---|
| `open` | used, nothing billed | used, nothing billed |
| `metered`, soft | used, billed, settles | **used, billed, served anyway** |
| `metered`, hard | used, billed, settles | **lane retired at startup** |

Retiring matters because a hard lane answers an unchallenged push with **401**, and only a 402 is exempt from **lane health**. Scheduling one anyway costs each chunk one of its `max_attempts` retries, per chunk, to rediscover what `/v1/status` already stated.

Both drivers drop it up front — native when no `--chequebook` is configured, browser unconditionally.

---

# Soft mode is an instrument, not a migration path

Soft mode meters, reports and accepts cheques, but **never answers 402**.

**It still requires the challenge.** An earlier draft implied unchallenged requests should be served — which would make metering bypassable *by omitting a header*. That is not a degraded mode; it is no mode at all.

> What soft mode drops is enforcement of the cap, **not authentication**.

A relay flipping to `--meter` therefore *does* break clients that predate the protocol. Acceptable: the only dApp using these lanes ships alongside them.

---

# Six bugs that only a *running* relay could find

None is reachable from a single upload against a fresh relay. All six survived the full test suite **and** the Stage 1 round-trip.

Reaching them needed, simultaneously:

- a relay that **remembers what you owe between runs**, so debt carries over
- several uploads in flight at the same time, not one after another
- a batch **used up far enough that the credit limit is what stops you**, rather than anything else

> §17.3 is the one to generalise from: it is not a coding error but an **invariant checked against the wrong quantity**, and it only becomes reachable once a real batch's value has decayed below ~0.39 BZZ.

---

# The six

| # | Bug |
|---|---|
| 17.1 | Debt the relay carried **across sessions** could not be paid |
| 17.2 | The headroom guard admitted a *frame*, then sent a *batch* |
| 17.3 | §10.1's invariant checked against the wrong quantity |
| 17.4 | A lane refused for **bytes in flight** was parked for good |
| 17.5 | A broken response stream made **every later cheque bounce** |
| 17.6 | The first POST of a run was sized **before the debt was known** |

---

# 17.1 — a slow-motion deadlock

The dust floor guarantees a run ends owing something: the residual below `min_cheque_plur` is left unpaid, because a cheque for it would be refused.

The relay is **right** to keep counting it — forgiving it would make *"stay under the floor"* a way to be served free.

But the client's books are per-process. The next run starts believing it owes **nothing**, and the relay's `owed` only ever grows. Once the carry crosses the cap, the first POST is refused — and the refusal is **unpayable**, because the cheque is computed from the client's own `owed`, which is zero.

> Observed live: a second upload failing **151/151** against a relay carrying 290,400,000,000 PLUR.

Fix: **ask rather than remember** — reconcile against `GET /v1/account`.

---

# 17.1 — three ways to get the fix wrong

Each of these was wrong in a draft:

- **Ask what you owe. Don't read it off the refusal.** The refusal already counts the request it is turning down, so paying that number overpays by exactly one request — and the next cheque bounces for being too big.

- **Don't count bytes still on the wire.** The client is already tracking those, so counting them again bills them twice. Guessing low is safe and fixes itself on the next round; guessing high gets the cheque rejected.

- **Cap it at the limit the relay signed up to.** Not the batch's current limit — that falls as the batch is used up, so it can drop below a bill you honestly ran up, and refusing to pay that **keeps you stuck**. And not the chequebook balance either: that lets any relay you point at ask for everything you have.

---

# 17.3 — the invariant was checked against the wrong quantity

`Params::validate` checked `min_cheque ≤ settle_every < max_outstanding` against **`max_outstanding_plur`** — the global ceiling.

But the line that actually binds is **per batch**:

```
min(remaining_value / credit_ratio, ceiling)
```

Below ~0.39 BZZ of batch value, the configured dust floor **exceeds everything the account can owe**. It accrues to its cap and can never write an acceptable cheque.

> Permanent refusal. Nothing broken. No error anywhere.

Thresholds are now resolved via `Params::effective(cap)` on **both** sides.

---

# Results — after all six fixes

Hard-mode relay, uploads 128 KiB → 4 MiB:

| payload | frames acked | 402s | stuck | rejected cheques |
|--------:|-------------:|-----:|------:|-----------------:|
| 128 KiB | 43/43 | 0 | 0 | 0 |
| 512 KiB | 151/151 | 0 | 0 | 0 |
| 1 MiB | 290/290 | 2 | 0 | 0 |
| 4 MiB | 1122/1122 | 4 | 0 | 0 |

The remaining 402s are the **intended** kind: the line genuinely fills, the client pays or waits, the lane resumes.

---

# Results — over public HTTPS, with §17.6 in place

Repeated through a reverse proxy, with the client learning its carried debt *before* sizing anything:

| run | payload | frames acked | 402s | rejected cheques |
|----:|--------:|-------------:|-----:|-----------------:|
| 1 | 2 MiB | 567/567 | 0 | 0 |
| 2 | 2 MiB | 567/567 | 0 | 0 |
| 3 | 2 MiB | 567/567 | 0 | 0 |

Each run settles to **`owed: 0`** on the relay, so the next carries nothing.

> That is the intended steady state: **402 is the recovery path, not the mechanism.**

---

# It settles on-chain

The loop closes end to end on Gnosis mainnet:

- The relay counts what you owe. You sign a cheque for the running total. The relay checks it and marks you paid.
- The relay is holding a cheque for **21,366,720,000,000 PLUR** right now.
- Cashing it happens on a **different machine**. Only the payee can cash a cheque, and the relay must never hold that key.

It worked: the transaction succeeded and used **75,378 gas**, against a 300,000 budget.

> The relay box never holds spendable key material. It needs the beneficiary's **address** only. The property that makes today's pusher safe survives metering intact.

---

# What is actually deployed

**Four `open` lanes** (free tiers, ephemeral disks — they *must* run open) plus **one hard-metered lane**, `pusher.browserbzz.link`, at 4.8e8 PLUR/KiB.

The browser dApp lists all five and **skips the metered one automatically**, because it stamps but never settles.

A native client with `--chequebook` uses all five.

> Payment is a property of a relay, not of the fleet.

---

# Net effect on the design

**Removed:** five mechanisms and every attack on them, just by billing bytes instead of receipts — the staking check, the list-shaped bill, the spot-check audit, the log sweep on both sides, and the shared receipt-checking code.

**Added:** three, all cheap — one signature check when a client asks permission, one chain lookup per batch every half hour, and one amount set aside per upload.

**Now guaranteed rather than hoped for:**

- An attacker gets credit worth a thousandth of what they actually funded, whatever size batch they buy
- Credit shrinks by itself as a batch is used up, with no expiry code to get wrong
- The relay holds no key that can move money

**Found by running it:** six bugs no test suite reached; five needed a ledger outliving the client.

---

# Still open

- **We don't yet know if this pays for itself.** The number to watch is how many accounts ever build up enough debt to be worth cashing. If it stays at zero, metering funds nothing.
- **One chequebook per batch owner.** If you upload using batches owned by different addresses, each one needs its own chequebook.
- **The empty-the-chequebook problem has no fix.** Bee has it too — it comes from how the chequebook contract is deployed, not from anything here.
- **Nothing stops a relay taking your money and dropping chunks.** You lose at most one credit limit and then stop using it — that bounds the damage, but it is not a guarantee of service.

<br>

*Full design: `docs/pusher-incentives.md` — §8 for the billing unit, §10.3 for the Sybil bound, §17 for the bugs.*
