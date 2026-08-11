---
marp: true
theme: default
paginate: true
header: 'Paying for relay — an incentive layer for hoverfly pushers'
---

# Paying for relay

## An incentive layer for hoverfly pushers, reusing parts of SWAP

*docs/pusher-incentives.md · Stages 0–1 shipped · one metered lane in production*

---

# The relay pays for traffic it did not cause

In a **native** upload your own machine opens the pushsync streams, and bee debits *you*:

```
price(po) = (32 − po) × 10 000   accounting units
```

Put a relay in the middle and that debt moves **wholesale to the relay** — it is the peer bee sees, so it is the peer bee charges. The browser client that caused the traffic pays **nothing but postage**.

> Booked today as accepted risk: *“worst case = the platform's free egress for the month burned, $0 lost.”*

---

# What changes

|  | open (today) | metered |
|---|---|---|
| client → relay | nothing | 4.8e8 PLUR per KiB of body sent |
| relay → bee | free pseudosettle | unchanged — free pseudosettle |
| relay's egress | unrecovered | recovered above the cashout threshold |

The relay still pays bee nothing, because credit was never what limited it. Bee hands out enough free credit for **~2,400 chunks a second** across a 128-connection pool, and the relay only manages **~150**. It is limited by connections and round trips, so buying credit would buy nothing.

> The recovered amount is small. A relay pushing 100 GB of egress a month moves ~27 GiB of payload, which at $0.02/GiB is **$0.54**.

---

# Part One — Theory

---

# Who has to trust whom

> The client chose its relay. The relay did not choose its client.

A relay is just an HTTP service. Anyone can run one, and there is no registry or list to get onto. The asymmetry comes from **the client picking**: before sending anything it checks the relay's signed quote and remembers who that relay is. The relay gets no such choice — a client is whoever shows up.

So every defence *the relay* has points at the client: *a client cannot obtain service without paying, and cannot lie about what it owes.*

---

# What protects the client, then

Not cryptography. Four bounds, none of which need the relay to be trustworthy:

- **It works out the bill itself.** The client adds up the bytes it sent. If the relay reports a bigger number, the client sees it immediately — and knows the relay is the one that is wrong.
- **The price is fixed in advance.** It arrives in a quote the relay signs, and the same signed quote comes back with every refusal — so the price cannot move mid-upload.
- **The most it can lose is one credit limit** — about $0.0024 at the maximum, and far less on a small batch, which gets a thousandth of whatever it is still worth.
- **It watches whether chunks actually arrive.** A relay that takes bytes and delivers badly gets sent less work, using the running average that already picks between relays today.

> That last bound had a hole in it. When a client cannot pay a refusal, it asks the relay what it owes and believes the answer. That used to be capped only by the chequebook balance, so any relay could ask for everything. It is now capped by the limit in the quote the relay signed: the credit it granted is the most it can claim.

---

# What the previous design got wrong

> The design was building **two-sided cryptographic verification** for a **one-sided trust relationship.**

About half of it protected the client from the relay — from relays we run ourselves. That cost three real things:

- **The bill was built on someone else's signature** — a receipt from a bee node. Those are easy to fake, so every one had to be checked against the on-chain list of staked nodes.
- **The bill was a list, not a number**, and nothing limited how long that list could get.
- **One unmeasured number could have killed the project**: how many receipt signers are actually staked. Nobody had checked.

---

# What we borrow from SWAP

| Borrowed | Why |
|---|---|
| ERC20SimpleSwap + canonical factory | Audited, deployed, in production. Nothing to write. |
| EIP-712 cheque | Cheque(chequebook, beneficiary, cumulativePayout) |
| Cumulative-payout monotonicity | Loss-tolerant *and* replay-proof |
| Funding check | …but liquidBalanceFor(us), not balance() — bee's version is unsound |
| Reservation vs. concurrent issuance | Needed on *both* sides |
| Payee-only role | The beneficiary is a plain EOA. A payee needs no contract. |

---

# What we drop

| Dropped | Reason |
|---|---|
| swap libp2p stream, Handshake, EmitCheque | We're on HTTP already |
| priceoracle, exchange, deduction | Relay quotes PLUR directly |
| accounting-unit indirection | One unit. No conversion. |
| ghost balances, tolerance, trust ramp | No analogue in request/response |
| StakeRegistry snapshot | Nothing left to anchor — see next |

> Both sides here are hoverfly. Bee is not involved in the payment at all — we reuse SWAP's *contracts and cheque format*, not the protocol it speaks over the network.

---

# The account is the batch owner

Relays already know who is uploading. Every chunk carries a stamp, and its signature has to match the address the postage contract lists as the batch's owner.

> Account = the batch-owner EOA. A cheque is valid for it **iff** its chequebook's on-chain `issuer()` is the same EOA. Credit is keyed one level finer — on the **batch**.

So there is nothing to add: no logins, no sign-up, no extra message. In a browser the key that owns the batch is already loaded, so cheques get signed with **no wallet pop-ups at all**.

---

# Bill bytes admitted

```
owed = (kib_admitted − kib_dedup) × price_plur_per_kib
```

> The client cannot lie about it — the client produced the bytes and the relay counted them.

- Nothing to forge — no outside signature is part of the bill
- Nothing to argue about — no chain lookup, and nothing the client takes on the relay's word

Both numbers are known to both parties *before* any push work happens.

---

# What that replaced

The earlier draft billed per **verified pushsync receipt**. To make a *third party's* signature into a billing input it needed:

- check every receipt signer against the on-chain staking list, or faking one is trivial
- make the bill a list instead of a number, or the same receipt gets submitted twice
- spot-check by fetching chunks back, to catch receipts for work never done
- share one piece of code that decides whether a receipt counts — and if the two copies ever disagree, nobody can settle it

> Billing bytes instead of receipts removed all five, and every attack on them.

---

# Bytes, not successful pushes

Bytes are what actually costs the relay money. It sends every chunk to three peers at once and retries the ones that go nowhere, and that traffic goes out whether or not the chunk ends up stored. Charging only for successes would leave the relay paying for every failure.

| Concern | Mechanism |
|---|---|
| Relay recovers what it spends | Bill attempts |
| Client protects itself from a lane that spends without succeeding | Deweight it in the scheduler — already exists, already works |

---

# Dedup hits are billed at zero

If the relay already pushed the same chunk moments ago, it does no work the second time, so those bytes come off the bill. This is the **only** part of the bill that rests on the relay's own word.

> It is safe for a simple reason: the claim only ever **lowers** the bill. A relay gains nothing by lying, and a client that disputes it is arguing in its own favour.

---

# Cumulative cheques

- **Losing one costs nothing.** Every cheque is a running total, so if a payment fails the next one covers it anyway. No retry logic needed.
- **Old cheques are worthless.** Each must be larger than the last, so sending an old one again pays nothing.
- **Gas is paid once per customer, not once per cheque.** The relay only ever cashes the newest total, so every earlier cheque costs nothing to collect.

> A cheque per chunk was rejected: 137 bytes and a signature on every 4 KiB, and — worse — running totals have to go in order, so chunks would have to be sent one at a time. That would remove the parallel uploads the scheduler relies on for speed.

---

# Cheques are per payee, not per relay

A running total belongs to a **payee**, but a relay is a **URL**. One operator running four relay URLs that all pay into the same account is the obvious way to deploy.

```
lane 1 issues cumulative 10
lane 2, counting from its own zero, issues 8
relay applies ErrChequeNotIncreasing → rejected, forever
```

> So the client must track running totals per *payee*, not per relay URL. Spotting that two relays share one is free — the payee's address is in the signed quote, before the first upload.

---

# Payment happens outside the upload

| Endpoint | Shape |
|---|---|
| GET /v1/status | signed *payment* block; mode: open \| metered |
| GET /v1/challenge | {nonce, expires_ms, max_outstanding_plur} — stateless MAC |
| GET /v1/account | authenticated. owed, reserved, outstanding, kib_admitted … |
| POST /v1/pay | body = SignedCheque JSON |
| POST /v1/push | 402 Payment Required when over cap |

The upload format is **unchanged**. Payment stays off the upload path, so a payment problem never breaks an upload. The balance endpoint needs a login, because otherwise anyone could look up how much any account has uploaded.

---

# Why a challenge at all

The tempting answer is that refusing is easy, because the relay picks its response code before it looks at any chunk. That is true, and it is exactly the **problem** — at that moment it does not yet know **whose account to check**.

Working out who is paying means checking the stamp and looking up the batch owner, and both happen later — after the response has already gone out.

> Doing those checks first means up to 512 signature recoveries (~40 ms) and a chain lookup **before the relay can answer at all** — cheap for an attacker to trigger, expensive for the relay to serve. Exactly the shape the design spends a section trying to avoid.

---

# The permission slip proves the checks were already done

```
GET /v1/challenge?account=A&batch=B
  → resolve standing(B)              (cached per batch, TTL)
  → require owner(B) == A, else 403
  → cap = credit_line(standing(B))
  → nonce = HMAC(relay_secret, preimage(A, B, origin, expiry, cap))
```

The chain lookups happen once, when the slip is issued, and the credit limit is sealed into it. After that an upload needs **no chain lookups at all**: check the slip, check it was issued for this relay, check the signature, set the money aside — and if that would go over the limit, **refuse before reading the upload**.

The relay keeps no list of the slips it has issued, so handing them out for free cannot fill up its memory.

---

# Two ways to make the check do nothing

**1. Glue the fields together carelessly and two different inputs look identical.** The hostname varies in length, so `("host.a","bc")` and `("host.ab","c")` run together into the same bytes — one slip that works for two different hostnames.

**2. The relay must know its own name from its config, not from the request.** The obvious version compares the slip against the `Host` header — which does nothing, because the attacker sends both.

> An attacker reusing a victim's signature at relay B just sends `Host: relay-b.example`. The check passes, and the attack works again — **while the code looks like it is preventing it.**

---

# Credit scales with what the batch is worth

It is tempting to assume a batch owner has real money at stake. They do not have to: the relay only checks that a batch is *alive*, and the cheapest batch the contract accepts costs a fraction of a cent. With one flat credit limit, an attacker gets back roughly what they paid.

```
max_outstanding(A,B) = min(remaining_value_plur(B) ÷ credit_ratio,
                           max_outstanding_plur)          credit_ratio = 1000
```

> An attacker now gets **a thousandth of what they funded, whatever they buy.** There is no cheap way in, because what is fixed is the *ratio* — not an amount someone can undercut.

---

# The rule the three thresholds must satisfy

> min_cheque_plur ≤ settle_every_plur < max_outstanding_plur

| Parameter | PLUR | In payload |
|---|---:|---:|
| price_plur_per_kib | 4.8e8 | 1 KiB |
| min_cheque_plur | 3.9e12 | ~8 MiB |
| settle_every_plur | 1.56e13 | ~32 MiB |
| max_outstanding_plur | 6.22e13 | ~127 MiB |

An early draft set the minimum cheque **87× larger** than the point where you are meant to pay. Every account would have jammed: run up a bill, try to pay, get told the cheque is too small, keep running it up, get refused — and the only cheque that would clear is 21× what you owe. No way out.

---

# Counting is not enough when uploads overlap

Just adding up what is owed is **not** enough. Uploads run in parallel on purpose, so several can each check the balance before any of them has been charged. Even a well-behaved client sending the 8 the relay itself recommends will blow past the limit.

The fix: set the money aside up front, based on the size the client declared, and give back whatever was not used when the upload finishes.

> But the set-aside amounts must not be saved to disk. Each one belongs to an upload in progress, and no upload survives a restart — so nothing would ever release it. Save what is owed and the running total; start the set-aside amounts at zero.

---

# The cost basis

| Per 4 KiB chunk relayed |  |
|---|---:|
| Delivery on the wire | ≈ 4.4 KiB |
| Sent to three peers at once | × 3 |
| Retries for chunks that go nowhere | × 1.15 |
| **Egress per chunk** | ≈ 15 KiB |
| **Egress per GiB of payload** | ≈ 3.7 GiB |

> $0.02 per GiB works out to `4.8 × 10⁸` PLUR per KiB. The rate is flat on purpose: anything more complicated puts a per-chunk number back into the bill, and that is one more thing the two sides can disagree about.

---

# On per-GB-billed hosts, metering loses money

At 3.7 GiB of real egress per GiB of payload:

|  | Per GiB of payload |
|---|---:|
| AWS egress at $0.09/GB | −$0.33 |
| Revenue at $0.02/GiB | +$0.02 |
| **Net** | **−$0.31** |

> Charging only covers costs on hosts with **flat-rate or included bandwidth**. On per-GB hosting, run it free and absorb the quota instead. That is the same kind of host already needed for a disk that survives restarts, so both requirements point at the same machines.

---

# Revenue per account vs. cashout gas

Issuing a cheque sends no transaction. Only cashing out touches the chain: ≈ **$0.0005** on Gnosis.

| Account's lifetime traffic | Revenue | Gas | Gas as % |
|---|---:|---:|---:|
| 71 MB — one browser upload | $0.0014 | $0.0005 | 36 % |
| 5 GiB — the cashout threshold | $0.10 | $0.0005 | 0.5 % |

> Gas is paid once per **customer**, not per cheque, so the ratio gets better every time someone comes back. Anyone who never reaches the threshold is written off. A relay whose users all upload once and leave should not charge at all.

---

# Three attacks worth knowing about

- **Someone else's stamps, billed to them** *(new)* — stamps are public, and a relay has a copy of every one it has forwarded. Send a victim's stamps to a paid relay and the victim's account picks up the bill, at no cost to the attacker. Fixed: the client must sign the challenge with the batch owner's key, and every chunk in a request must belong to the batch named in it.
- **The customer can empty the chequebook after you accept** *(inherited)* — the owner can withdraw at any time, so "this cheque is funded" is true when you take it, not when you cash it. Bee has exactly the same exposure.
- **A relay that forgets serves for free forever** *(new)* — if the ledger does not survive a restart, one signature buys unlimited service. So a paid relay needs a real disk. That is a requirement, not advice.

---

# Part Two — Practice

---

# Paying is optional, per lane

| Relay mode | Client has a chequebook | Client does not |
|---|---|---|
| open | used, nothing billed | used, nothing billed |
| metered, soft | used, billed, settles | used, billed, served anyway |
| metered, hard | used, billed, settles | lane retired at startup |

> A paid relay rejects an upload with no permission slip, and that rejection counts against the relay's health score — only a genuine "you owe too much" is excused. So sending work there anyway burns one retry on every single chunk, to rediscover something the relay already said up front.

---

# What soft mode does and does not drop

Soft mode meters, reports and accepts cheques, but **never answers 402**.

**It still requires the permission slip.** An earlier draft suggested serving requests that arrive without one — which would let anyone skip paying by leaving out a header.

> What soft mode drops is enforcement of the cap, **not authentication**.

A relay flipping to `--meter` therefore does break clients that predate the protocol. Acceptable: the only dApp using these lanes ships alongside them.

---

# Six bugs only a running relay could find

None is reachable from a single upload against a fresh relay. All six survived the full test suite **and** the Stage 1 round-trip. Reaching them needed, simultaneously:

- a relay that **remembers what you owe between runs**, so debt carries over
- several uploads in flight at the same time, not one after another
- a batch **used up far enough that the credit limit is what stops you**, rather than anything else

> The one to learn from is §17.3. It is not a coding mistake — it is a **rule checked against the wrong number**, and it only shows up once a real batch has been worn down below about 0.39 BZZ.

---

# The six bugs

| § | Bug |
|---:|---|
| 17.1 | Debt the relay carried **across sessions** could not be paid |
| 17.2 | The headroom guard admitted a *frame*, then sent a *batch* |
| 17.3 | The §10.1 invariant checked against the wrong quantity |
| 17.4 | A lane refused for **bytes in flight** was parked for good |
| 17.5 | A broken response stream made **every later cheque bounce** |
| 17.6 | The first POST of a run was sized **before the debt was known** |

---

# Debt carried over, and could not be paid

Because there is a minimum cheque size, every upload ends owing a little too small to pay. The relay is **right** to keep counting it — writing it off would make staying under the minimum a way to be served for free.

But the client forgets when it exits. The next run starts thinking it owes **nothing**, while the relay's total keeps growing. Eventually the very first upload is refused — and the client **cannot pay**, because it writes cheques from its own figure, which is zero.

> Observed live: a second upload failing **151/151** against a relay carrying 290,400,000,000 PLUR.

---

# Three ways to get the fix wrong

- **Ask what you owe. Don't read it off the refusal.** The refusal already counts the request it is turning down, so paying that number overpays by exactly one request — and the next cheque bounces for being too big.
- **Don't count bytes still on the wire.** The client is already tracking those, so counting them again bills them twice. Guessing low is safe and fixes itself on the next round; guessing high gets the cheque rejected.
- **Cap it at the limit the relay signed up to.** Not the batch's current limit — that falls as the batch is used up, so it can drop below a bill you honestly ran up, and refusing to pay that keeps you stuck. And not the chequebook balance either: that lets any relay you point at ask for everything you have.

---

# The invariant was checked against the wrong quantity

The check compared the three thresholds against the highest limit the relay ever grants. But the limit that actually applies is **per batch**, and it is usually much smaller:

```
min(remaining_value / credit_ratio, ceiling)
```

Once a batch is worth less than about 0.39 BZZ, the minimum cheque is **bigger than anything that account is allowed to owe**. It runs up to its limit and can never write a cheque the relay will take.

> The account is refused from then on. Nothing is broken and nothing logs an error — it just stops working. Both sides now compare against the limit that actually applies.

---

# After all six fixes

| Payload | Frames acked | 402s | Stuck | Rejected cheques |
|---|---:|---:|---:|---:|
| 128 KiB | 43/43 | 0 | 0 | 0 |
| 512 KiB | 151/151 | 0 | 0 | 0 |
| 1 MiB | 290/290 | 2 | 0 | 0 |
| 4 MiB | 1122/1122 | 4 | 0 | 0 |

> The remaining 402s are the *intended* kind: the line genuinely fills, the client pays or waits, the lane resumes.

---

# Over public HTTPS, with §17.6 in place

| Run | Payload | Frames acked | 402s | Rejected cheques |
|---:|---|---:|---:|---:|
| 1 | 2 MiB | 567/567 | 0 | 0 |
| 2 | 2 MiB | 567/567 | 0 | 0 |
| 3 | 2 MiB | 567/567 | 0 | 0 |

Each run settles to `owed: 0` on the relay, so the next carries nothing.

> That is how it is meant to run: **a refusal is the fallback, not the normal path.**

---

# It settles on-chain

- The relay counts what you owe. You sign a cheque for the running total. The relay checks it and marks you paid.
- The relay is holding a cheque for **21,366,720,000,000 PLUR** right now.
- Cashing it happens on a **different machine**. Only the payee can cash a cheque, and the relay must never hold that key.
- It worked: the transaction succeeded and used 75,378 gas, against a 300,000 budget.

> The relay never holds a key that can spend anything. It only needs the payee's **address**. So charging for relay does not make a relay box worth breaking into.

---

# What is actually deployed

**Four free relays** — on hosts whose disks are wiped on restart, so they have to stay free — plus **one paid relay** that enforces payment.

The browser app lists all five and **skips the paid one automatically**, because it can sign chunks but not cheques. A command-line client with a chequebook uses all five.

> Payment is a property of a relay, not of the fleet.

---

# Net effect on the design

- **Removed:** five mechanisms and every attack on them, just by billing bytes instead of receipts — the staking check, the list-shaped bill, the spot-check audit, the log sweep on both sides, and the shared receipt-checking code.
- **Added:** three, all cheap — one signature check when a client asks permission, one chain lookup per batch every half hour, and one amount set aside per upload.
- **Now guaranteed rather than hoped for:** an attacker gets credit worth a thousandth of what they actually funded, whatever size batch they buy; credit shrinks by itself as a batch is used up, with no expiry code to get wrong; and the relay holds no key that can move money.

> Found by running it: six bugs no test suite reached — five needed a ledger that outlives the client.

---

# Still open

- **We don't yet know if this pays for itself.** The number to watch is how many accounts ever build up enough debt to be worth cashing. If it stays at zero, metering funds nothing.
- **One chequebook per batch owner.** If you upload using batches owned by different addresses, each one needs its own chequebook.
- **The empty-the-chequebook problem has no fix.** Bee has it too — it comes from how the chequebook contract is deployed, not from anything here.
- **Nothing stops a relay taking your money and dropping chunks.** You lose at most one credit limit and then stop using it — that bounds the damage, but it is not a guarantee of service.

Full design: `docs/pusher-incentives.md` — §8 billing unit, §10.3 Sybil bound, §17 the bugs.
