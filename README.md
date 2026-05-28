# isheika

Minimal [Swarm][swarm] (Ethereum Swarm) micro-client. Native build speaks
libp2p over plain TCP **and** WebSocket; WASM build speaks WebSocket only
(browsers can't open raw TCP sockets).

[swarm]: https://www.ethswarm.org/

Three operations: `discover`, `fetch`, `upload`.

## Features

- **Small binaries.** ~6-8 MB compressed per platform. The full
  end-to-end client — libp2p stack, bee wire protocols, mantaray
  manifests, postage stamps, on-chain batch creation — fits in
  10 MB of stripped native binary.
- **Runs in a browser.** First-class `wasm32` target builds against
  WebSocket transport (browsers can't open raw TCP). The same crate
  uploads to Swarm from a Node script, a Cloudflare Worker, or a
  browser page.
- **No bee dependency.** isheika talks bee's wire protocols directly.
  No reverse-proxying to a bee HTTP gateway; uploads push chunks
  straight to mainnet over libp2p.
- **Daemon + one-shot modes.** Long-running `daemon` for sustained
  uploads (warm session pool, ~5x throughput); or invoke `upload`
  directly for a single file with no setup.
- **TAR collections.** Multi-file uploads as bee-compatible mantaray
  manifests, addressable by path. `*.tar` inputs auto-trigger
  collection mode.
- **On-chain batch creation.** `isheika batch create` issues postage
  stamp batches on Gnosis chain directly (no bee node needed).
  `--size 2GB --duration 30d` resolves to the right depth+amount via
  the official postage-stamp calculator's formulas.
- **CI-friendly.** A drop-in GitHub Actions example uploads a
  `./dist` directory to Swarm at ~500 KiB/s on a fresh runner. See
  [`examples/upload.yml`](examples/upload.yml).
- **Reproducible releases.** Multi-platform release tarballs (Linux
  x86_64/aarch64, macOS x86_64/aarch64) shipped with SHA-256 sidecars
  and SLSA Build Provenance attestations. Verify with
  `gh attestation verify`.

## Setup

### 1. Install isheika

```bash
curl -fsSL https://raw.githubusercontent.com/omnipin/isheika/main/install.sh | sh
```

Drops the latest prebuilt `isheika` into `~/.local/bin` (override with
`ISHEIKA_BIN_DIR=…`, pin with `ISHEIKA_VERSION=v0.1.0`). Prebuilts
cover Linux x86_64 / aarch64 and macOS x86_64 / aarch64; on anything
else, build from source:

```bash
cargo install --git https://github.com/omnipin/isheika
```

### 2. Generate a key

Your secp256k1 private key (`--key` / `--identity`, 32 bytes hex) is
your long-lived signer. Bee uses the derived Ethereum address to
recognize you across reconnects, route cheques, and verify your
postage stamps.

```bash
cast wallet new
```

Save the printed `Private key` and `Address` — both are useful (the
key for `--key`/`--identity`, the address for funding xDAI + BZZ).

### 2. Generate a vanity overlay nonce

Your Swarm overlay is `keccak256(eth_addr ‖ network_id ‖ nonce)`. A
random nonce works, but `isheika vanity-overlay` searches for one
that lands your overlay in less-saturated kademlia bins — empirically
**~25% higher upload throughput** vs random.

```bash
isheika vanity-overlay --key 0xYOUR_KEY --output overlay-nonce
```

CPU-bound and one-time (seconds to minutes depending on target PO and
seed size). The written `overlay-nonce` file is reused by every
subsequent daemon / upload command. Treat it like a secret — losing
it means losing your accumulated kademlia presence on the network.

A `(key, overlay-nonce)` pair is your Swarm identity. Don't share
nonces across keys (a vanity nonce for key A is just a random nonce
for key B), and don't run two daemons with the same identity
simultaneously (bee disconnects both for conflicting underlay).

### 3. Create a postage batch

Uploads need a postage stamp batch on-chain. Fund the address from
step 1 with a little xDAI (for gas) and some BZZ (for the batch
itself), then:

```bash
isheika batch create --rpc-url https://rpc.gnosischain.com --key 0xYOUR_KEY --size 2GB --duration 30d
```

`--size` and `--duration` map to `--depth` and `--amount-per-chunk`
via the same formulas as the [official postage stamp
calculator](https://docs.ethswarm.org/docs/develop/tools-and-features/buy-a-stamp-batch/#calculators)
(smallest depth whose effective volume covers the requested size,
unencrypted + no erasure coding). `--depth` + `--amount-per-chunk`
still works if you want to set them explicitly.

The on-chain `BatchCreated` event takes 1-3 minutes to propagate to
the bee nodes that'll accept your stamps. Poll
[Swarmscan](https://swarmscan.io/) until it 200s the batch:

```bash
curl -s "https://api.swarmscan.io/v1/postage/batches/<BATCH_ID>"
# 404 = network hasn't indexed it yet
# 200 with a JSON body = ready to use
```

### 4. Run the daemon

A long-lived daemon holds a warm session pool across uploads — ~5x
throughput vs running a fresh upload each time (the per-upload cost
of filling 256 peer sessions is paid once at daemon startup, not per
upload). For one-shot uploads you can skip this step and pass
`--peerlist` directly to `isheika upload`.

```bash
isheika daemon --socket /tmp/isheika.sock --pool-size 256 --listen /ip4/0.0.0.0/tcp/1635 --identity 0xYOUR_KEY --advertise /ip4/YOUR_PUBLIC_IP/tcp/1635 --discover-rounds 3
```

The repo ships a curated `peers.seed.json` (committed); the daemon
loads it via `--peerlist` (default: `peers.json`) for fast cold-start
without running `discover` first. Copy it before first start:

```bash
cp peers.seed.json peers.json
```

### 5. Upload

```bash
isheika upload --daemon /tmp/isheika.sock --batch YOUR_BATCH_ID_HEX --key 0xYOUR_KEY path/to/file.bin
```

A `.tar` input is auto-treated as a collection (multi-file mantaray);
pass `--collection` to force collection mode on any other extension.
See `isheika upload --help` for the rest.

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

## Status

Not stable. The Cargo description is `0.1.0` and the API will change.
Useful as an audit reference for the bee protocols and as a
deployment client when you need uploads from somewhere bee won't run
(WASM, CI, light constrained environments).
