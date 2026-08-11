---
marp: true
theme: default
paginate: true
header: 'Paying for relay — an incentive layer for hoverfly pushers'
---

<!-- This file is the source. `marp-cli` renders it to PDF as-is; the
     self-contained HTML deck is built with:
       python3 docs/deck/build.py docs/pusher-incentives-slides.md -o deck.html
     Directives (<!-- title -->, part:, eyebrow:, hazard) are documented
     in docs/deck/build.py. -->

<!-- title -->

# Paying for relay

## An incentive layer for hoverfly pushers, reusing parts of SWAP

*docs/pusher-incentives.md · Stages 0–1 shipped · one paid relay in production*

---

<!-- eyebrow: 1 · the problem -->

# The relay pays for traffic it did not cause

When you upload directly, bee charges **you** for every chunk. Put a relay in the middle and that cost moves to the relay — it is the peer bee sees, so it is the peer bee bills. The client pays only for postage.

|  | free (today) | paid |
|---|---|---|
| client → relay | nothing | 4.8e8 PLUR per KiB sent |
| relay → bee | nothing | **unchanged** — still nothing |
| relay's bandwidth | unrecovered | recovered above the cashout threshold |

The relay still pays bee nothing, because credit was never what limited it: bee allows about **2,400 chunks a second** and the relay manages **~150**. It runs out of connections long before it runs out of credit.

> The sums are small. A relay pushing 100 GB a month moves ~27 GiB of payload, which at $0.02/GiB is **$0.54**. This works on repeat and bulk traffic, or not at all.

---

<!-- eyebrow: 2 · trust -->

# Who has to trust whom

A relay is just an HTTP service. Anyone can run one, and there is no registry or list to get onto. The asymmetry is that **the client picks**: before sending anything it checks the relay's signed quote and remembers who that relay is. The relay gets no such choice — a client is whoever shows up.

> So every defence the relay has points at the client: it cannot get service without paying, and cannot lie about what it owes.

The client's protections are a different kind of thing — arithmetic and limits, not cryptography:

- **It works out the bill itself**, so a relay reporting a bigger number is caught immediately
- **The price is fixed in advance**, in a quote the relay signed
- **The most it can lose is one credit limit** — about $0.0024, and less on a small batch
- **It watches whether chunks arrive**, and sends less work to relays that deliver badly

---

<!-- eyebrow: 3 · the central decision -->

# Bill bytes, not receipts

```
owed = (KiB admitted − KiB already cached) × price per KiB
```

> The client cannot lie about it, because the client produced the bytes and the relay counted them.

An earlier design billed per delivery receipt — a signature from a **third party**. Making that safe needed five mechanisms: check every signer against the on-chain staking list, turn the bill into a list so receipts cannot be reused, spot-check by fetching chunks back, sweep the staking logs on both sides, and share one piece of receipt-checking code that must never disagree between them.

Changing the unit removed all five, and every attack on them. Bytes are also what the relay actually spends money on: it sends each chunk to three peers at once and retries failures, and that traffic goes out whether or not the chunk sticks.

---

<!-- eyebrow: 4 · admission -->

# Getting in: a permission slip

The relay has to decide whether to accept an upload **before** it has read it — and at that moment it does not yet know whose account to check. Working that out means checking a stamp and looking up a batch owner on chain.

<!-- hazard -->
> Doing all that first would mean up to 512 signature recoveries and a chain lookup **before the relay can answer at all** — cheap for an attacker to trigger, expensive to serve.

So the chain lookups happen **once**, when a slip is issued, and the credit limit is sealed into it. After that an upload needs no chain lookups: check the slip, check the signature, set the money aside — and refuse before reading the body if that would go over the limit.

The slip must be signed by the batch owner. That is what stops an attacker replaying **someone else's public stamps** and having the bill land on them.

---

<!-- eyebrow: 5 · credit -->

# How much a client may owe

It is tempting to assume a batch owner has real money at stake. They need not: the relay only checks that a batch is alive, and the cheapest live batch costs a fraction of a cent. So the limit is tied to what the batch is actually worth:

```
credit limit = min(batch's remaining value ÷ 1000, 6.22e13 PLUR)
```

> An attacker gets back **a thousandth of what they funded**, whatever size batch they buy. What is fixed is the ratio, so there is no cheap corner to aim at.

| Setting | PLUR | In payload |
|---|---:|---:|
| price per KiB | 4.8e8 | 1 KiB |
| smallest cheque accepted | 3.9e12 | ~8 MiB |
| pay when you reach | 1.56e13 | ~32 MiB |
| hard ceiling | 6.22e13 | ~127 MiB |

Those three must stay in that order. Otherwise an account can owe less than the smallest cheque it is allowed to write, and then it can never pay.

---

<!-- eyebrow: 6 · settlement -->

# Cheques are running totals

Each cheque says "you have now paid me *this much in total*", not "here is a payment". Three things follow:

- **Losing one costs nothing.** The next cheque covers it anyway. No retry logic.
- **Old cheques are worthless.** Each must exceed the last, so replaying one pays zero.
- **Gas is paid once per customer**, not per cheque — the relay only ever cashes the newest total.

Uploads run in parallel, so simply adding up what is owed is not enough: several can each check the balance before any of them is charged. The relay sets the money aside up front and refunds the unused part.

<!-- hazard -->
> But the set-aside amounts must never be written to disk. Each one belongs to an upload in progress, and no upload survives a restart — so nothing would ever release them.

---

<!-- eyebrow: 7 · economics -->

# What it costs, and who pays for themselves

Every 4 KiB chunk costs about **15 KiB** of real bandwidth — three peers at once, plus retries — so a GiB of payload costs ~3.7 GiB of traffic. At $0.02/GiB that is a real margin on flat-rate hosting and a loss anywhere else.

|  | per GiB of payload |
|---|---:|
| AWS egress at $0.09/GB | −$0.33 |
| revenue at $0.02/GiB | +$0.02 |
| **net** | **−$0.31** |

| Customer's lifetime traffic | Revenue | Gas | Gas as % |
|---|---:|---:|---:|
| 71 MB — one browser upload | $0.0014 | $0.0005 | 36 % |
| 5 GiB — the cashout threshold | $0.10 | $0.0005 | 0.5 % |

> A relay on per-GB bandwidth, or one whose users upload once and never return, should stay free.

---

<!-- eyebrow: 8 · in production -->

# Paying is optional, per relay

| Relay | Client can pay | Client cannot |
|---|---|---|
| free | used, nothing billed | used, nothing billed |
| paid, soft | used, billed, settles | used, billed, served anyway |
| paid, enforced | used, billed, settles | **dropped at startup** |

A paid relay rejects an upload that arrives with no permission slip, and that rejection counts against the relay's health score — only a genuine "you owe too much" is excused. Sending work there anyway burns one retry on every chunk, to rediscover what the relay already said up front.

> Four free relays run on hosts whose disks are wiped on restart, so they have to stay free — a relay that forgets what it is owed serves for free forever. One paid relay enforces. The browser app skips it automatically, because it can sign chunks but not cheques.

---

<!-- eyebrow: 9 · what running it found -->

# Six bugs no test suite reached

All six survived the full test suite and a working end-to-end round trip. Reaching them needed three things **at once**: a relay that remembers debt between runs, several uploads in flight at the same time, and a batch worn down far enough that the credit limit is what stops you.

| § | Bug |
|---|---|
| 17.1 | Debt carried across runs could not be paid — each run starts believing it owes nothing |
| 17.2 | The headroom check measured one chunk, then sent a whole batch |
| 17.3 | A rule checked against the wrong number |
| 17.4 | A relay refused for bytes still in flight was parked forever |
| 17.5 | One broken response stream made every later cheque bounce |
| 17.6 | The first upload of a run was sized before the debt was known |

> The one to learn from is **17.3**. Not a coding mistake — a rule compared against the highest limit the relay ever grants, when the limit that applies is per batch. Below ~0.39 BZZ of batch value the smallest allowed cheque exceeds everything the account may owe. It is refused from then on, and nothing logs an error.

---

<!-- eyebrow: 10 · where it stands -->

# It works, and here is what is still open

| Run | Payload | Chunks delivered | Refusals | Rejected cheques |
|---|---|---:|---:|---:|
| 1 | 2 MiB | 567/567 | 0 | 0 |
| 2 | 2 MiB | 567/567 | 0 | 0 |
| 3 | 2 MiB | 567/567 | 0 | 0 |

Each run settles to zero owed, so the next carries nothing. Cheques are cashed on a **different machine** — only the payee can cash one, and the relay must never hold that key. The live cashout used 75,378 gas against a 300,000 budget.

- **We do not yet know if this pays for itself.** The number to watch is how many customers ever reach the cashout threshold.
- **Nothing stops a relay taking payment and dropping chunks.** You lose at most one credit limit and stop using it — that bounds the damage, it does not guarantee service.
- **A customer can empty their chequebook after you accept a cheque.** Bee has the same problem; it comes from how the contract is deployed.
