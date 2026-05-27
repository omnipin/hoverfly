# isheika

Minimal [Swarm][swarm] (Ethereum Swarm) micro-client. Native build speaks
libp2p over plain TCP **and** WebSocket; WASM build speaks WebSocket only
(browsers can't open raw TCP sockets).

[swarm]: https://www.ethswarm.org/

Three operations: `discover`, `fetch`, `upload`.

## Setup

isheika identifies itself on the network with **two persistent pieces of
state** — generate them once, reuse forever:

1. **A secp256k1 private key** (`--key` / `--identity`, 32 bytes hex).
   This is your long-lived signer; bee uses the derived Ethereum address
   to recognize you across reconnects, route cheques, and verify your
   postage stamps. Generate however you'd usually generate one
   (`openssl rand -hex 32`, hardware wallet, etc.).

2. **An overlay nonce** (`--nonce-file`, 32 bytes hex). Your Swarm
   overlay is `keccak256(eth_addr ‖ network_id ‖ nonce)`. A random nonce
   works, but `isheika vanity-overlay` can search for a nonce that lands
   your overlay in less-saturated kademlia bins — empirically **~25%
   higher upload throughput** versus random (see
   `.github/workflows/bench.yml` A/B in commit history).

   ```bash
   ./target/release/isheika vanity-overlay \
     --key 0xYOUR_KEY \
     --output overlay-nonce
   ```

   This is **CPU-bound and one-time** (seconds to minutes depending on
   target PO and seed size). The written `overlay-nonce` is then reused
   by the daemon/upload commands forever. Treat it like a secret — losing
   it means losing your accumulated kademlia presence on the network.

A `(key, overlay-nonce)` pair is your Swarm identity. Don't share nonces
across keys (a vanity nonce for key A is just a random nonce for key B),
and don't run two daemons with the same identity simultaneously (bee
disconnects both for conflicting underlay).

## Quick start

Build the native binary:

```bash
cargo build --release --bin isheika
```

Bench daemon-mode upload throughput (default `--pool-size 256`):

```bash
# In one terminal — start a long-lived daemon.
./target/release/isheika \
  --nonce-file overlay-nonce \
  --buffer-multiplier 2 \
  daemon \
  --socket /tmp/isheika.sock \
  --peerlist peers.json \
  --pool-size 256 \
  --listen /ip4/0.0.0.0/tcp/1635 \
  --identity 0xYOUR_KEY \
  --advertise /ip4/YOUR_PUBLIC_IP/tcp/1635 \
  --discover-rounds 3

# In another — upload through the daemon.
./target/release/isheika upload \
  --daemon /tmp/isheika.sock \
  --batch YOUR_BATCH_ID_HEX \
  --key 0xYOUR_KEY \
  path/to/file.bin
```

The daemon ships with a curated `peers.seed.json` (committed); copy it
to your daemon's `--peerlist peers.json` for fast cold-start without
running `discover` first.

## Bench targets

Both CircleCI and GitHub Actions ship a workflow under `.circleci/` and
`.github/workflows/` that runs a 3-upload manual benchmark on demand
(`workflow_dispatch` on GH, `run_bench` parameter on CircleCI). Median
throughput on a fresh runner currently lands ~400–500 KiB/s — see
`PERFORMANCE.md` for the full empirical-ceilings table.

## Compatibility

Tracks the upstream [bee][bee] mainnet protocols:

| Protocol      | Versions accepted          | Notes                                                |
| ------------- | -------------------------- | ---------------------------------------------------- |
| handshake     | `15.0.0` (preferred), `14.0.0` (fallback) | v15 added timestamp + chequebook in the signed payload (bee 2.8.0, May 2026) |
| hive          | `2.0.0` (preferred), `1.1.0` (fallback)   | Same field-set bump as v15 handshake                 |
| retrieval     | `1.4.0`                                   |                                                      |
| pushsync      | `1.3.1`                                   |                                                      |
| pricing       | `1.0.0`                                   |                                                      |
| pseudosettle  | `1.0.0`                                   |                                                      |
| status        | `1.1.3`                                   | Inbound-only (responds to bee's salud probes)        |
| swap          | `1.0.0`                                   | Cheque issuance only, no cashout                     |
| libp2p ping   | `1.0.0`                                   | Responds — bee 2.8's reacher uses this for reachability checks |

[bee]: https://github.com/ethersphere/bee

## Documentation

* `AGENTS.md` — architecture map, build/test workflow, file:line for
  the constants that matter when tuning.
* `PERFORMANCE.md` — every optimisation in the project's history with
  empirical numbers and the bee-vs-isheika end-to-end comparison.

## Status

Not stable. The Cargo description is `0.1.0` and the API will change.
Useful as an audit reference for the bee protocols and as a
deployment client when you need uploads from somewhere bee won't run
(WASM, CI, light constrained environments).

## License

MIT.
