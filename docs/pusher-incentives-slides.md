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
     in docs/deck/build.py.

     Keep slides sparse: a heading, ONE block (a short table, up to four
     bullets, or two short paragraphs), and ONE callout. The detail lives in
     docs/pusher-incentives.md.

     Headings name the mechanism and nothing else — "Trust model", not "Who
     has to trust whom". The claim about that mechanism goes in the body,
     where there is room to say it precisely. Eyebrows mark the act (the
     problem / theory / practice), so they never restate the heading. -->

<!-- title -->

# Paying for relay

## An incentive layer for hoverfly pushers, reusing parts of SWAP

*docs/pusher-incentives.md · one paid relay in production*

---

<!-- eyebrow: the problem -->

# Unpaid relay bandwidth

Upload directly and bee bills you. Put a relay in the middle and bee bills the relay — it is the peer bee sees. The client pays only postage.

|  | free | paid |
|---|---|---|
| client → relay | nothing | 4.8e8 PLUR per KiB |
| relay → bee | nothing | still nothing |

> A relay absorbs 70–100 GB of egress a month. Metered, that same month bills **$1.14–1.63**.

---

<!-- eyebrow: theory -->

# Trust model

A relay is a standalone HTTP service — no registry, no list to get onto. Trust runs one way: the client checks a signed quote before sending a byte; the relay gets whoever shows up.

> So every relay-side defence points at the client, and the client needs none — it counts its own bytes and risks at most one credit limit, about **$0.0024**.

---

<!-- eyebrow: theory -->

# The billing unit

```
owed = KiB the relay accepted × price per KiB
```

> The client cannot lie about it. It produced the bytes; the relay counted them.

Bytes admitted, not delivery receipts. Billing per receipt meant trusting a **third party's signature**, which took five mechanisms to make safe. Changing the unit removed all five.

---

<!-- eyebrow: theory -->

# Admission control

The relay must accept or refuse **before** reading an upload — but at that moment it does not know whose account to check.

<!-- hazard -->
> Checking on every upload would mean 512 signature recoveries before it can answer at all. Cheap to attack, expensive to serve.

So the chain lookups happen **once**, when a slip is issued, and the credit limit is sealed into it. The batch owner signs it, so stolen stamps cannot bill their owner.

---

<!-- eyebrow: theory -->

# The credit limit

The cheapest live batch costs a fraction of a cent, so "owns a batch" proves nothing. The limit tracks what the batch is worth:

```
credit limit = batch's remaining value ÷ 1000
```

> An attacker gets back **a thousandth of what they funded**, at any batch size. The ratio is what is fixed, so there is no cheap corner to aim at.

---

<!-- eyebrow: theory -->

# Settlement

A cheque is a running total: "you have now paid me *this much in total*".

- **Losing one costs nothing** — the next covers it
- **Old ones are worthless** — each must exceed the last
- **Gas is paid once per customer**, not per cheque

<!-- hazard -->
> Money set aside for an upload in progress must never be written to disk. No upload survives a restart, so nothing would ever release it.

---

<!-- eyebrow: practice -->

# Unit economics

Relaying earns **$0.02 per GiB** admitted. On a host you already pay for, the only cost is cashout gas — 110k gas, a fraction of a microcent.

| egress per byte relayed | fits under a 2 TB/mo cap | earns |
|---|---:|---:|
| 3.7× modelled | 503 GiB | $10 |
| 1.15× measured | 1.58 TiB | $33 |

> So the ceiling is the bandwidth allowance, not the cost. Past the cap, egress runs $0.11 per GiB against $0.02 of revenue and it inverts.

---

<!-- eyebrow: practice -->

# Deployment

Paying is optional, and each relay sets its own mode.

| Relay | Client can pay | Client cannot |
|---|---|---|
| free | nothing billed | nothing billed |
| paid, soft | billed, settles | billed, served anyway |
| paid, enforced | billed, settles | **dropped at startup** |

> Four free relays run on hosts that wipe their disk on restart — and a relay that forgets what it is owed serves for free forever. The browser app skips the paid one: it can sign chunks, not cheques.

---

<!-- eyebrow: practice -->

# Bugs found in production

| § | Bug |
|---|---|
| 17.1 | Carried-over debt could not be paid |
| 17.2 | Headroom measured one chunk, then sent a batch |
| 17.3 | A rule checked against the wrong number |
| 17.4 | A relay refused for bytes in flight was parked forever |
| 17.5 | One broken stream bounced every later cheque |
| 17.6 | The first upload was sized before the debt was known |

> No test suite reached any of them. All six needed the same three things at once: debt surviving restarts, parallel uploads, and a nearly-spent batch.

---

<!-- eyebrow: practice -->

# Results

Three runs of 2 MiB, 567/567 chunks delivered, no refusals, no rejected cheques, each settling to zero owed. Cheques cash from a separate machine — the relay must never hold that key.

| | to date |
|---|---:|
| billed | $0.0003 |
| cashed on-chain | $0.00006 |
| paying clients | 1, and it was me |

> The mechanism works; nobody is paying it. The browser dApp is the only real traffic and it signs stamps, not cheques. Nothing yet stops a relay billing for chunks it drops.
