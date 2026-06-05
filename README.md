# isheika

Minimal [Swarm][swarm] (Ethereum Swarm) micro-client — `discover`, `fetch`,
`upload`. It talks bee's mainnet libp2p protocols directly; there's no bee
node or HTTP gateway in between.

Native builds use plain TCP and WebSocket. The `wasm32` build is
WebSocket-only, because browsers can't open raw TCP sockets.

[swarm]: https://www.ethswarm.org/

## What it does

- Fetches and uploads Swarm content by speaking bee's libp2p protocols
  directly (see the compatibility table below).
- Discovers peers by walking bee's hive gossip out from a bootnode, and
  caches the result in a reusable `peers.json`.
- Wraps a file in a mantaray manifest, or packs a directory into a TAR
  collection — one manifest, every file still addressable by path.
- Creates postage batches on-chain: `isheika batch create --size 2GB
  --duration 30d` issues a stamp batch on Gnosis chain.
- Runs one-shot, or as a daemon that keeps a warm session pool so the
  pool-fill cost is paid once at startup, not on every upload.
- Cross-compiles to `wasm32` and runs the same client in the browser. The
  WASM bindings are on npm as
  [`@omnipin/isheika`](https://www.npmjs.com/package/@omnipin/isheika).

The native binary is a single static file — about 5 MB gzipped, 14 MB
unpacked (x86_64-linux). Release tarballs carry a SLSA build-provenance
attestation; verify a download with `gh attestation verify`. A ready-made
GitHub Actions upload workflow lives in [`examples/upload.yml`](examples/upload.yml).

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/omnipin/isheika/main/install.sh | sh
```

Drops the latest prebuilt `isheika` into `~/.local/bin` (override with
`ISHEIKA_BIN_DIR=…`, pin with `ISHEIKA_VERSION=v0.1.2`). The installer
covers Linux x86_64 / aarch64 and macOS x86_64 / aarch64; releases also
ship FreeBSD and NetBSD x86_64 tarballs.

### Build from source

On any other platform, or to track `main`:

```bash
cargo install --git https://github.com/omnipin/isheika
```

## Setup

### 1. Generate a key

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
random nonce works, but most random overlays land in bee's already-full
low kademlia bins and get dropped right after the handshake. `isheika
vanity-overlay` searches for a nonce that puts you in deeper,
undersaturated bins instead. Anchoring against a few stable peers
(`--target-overlay`) roughly **doubled** upload throughput in testing —
see `PERFORMANCE.md` for the method and numbers.

```bash
isheika vanity-overlay --key 0xYOUR_KEY --output overlay-nonce
```

One-time, CPU-bound (seconds to minutes). The resulting
`overlay-nonce` is your Swarm identity together with `--key` — keep
it, don't reuse across keys, and don't run two daemons with the
same `(key, nonce)` pair at once (bee disconnects both).

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

A long-lived daemon holds a warm session pool across uploads. Filling a
256-session pool takes ~80 s; the daemon pays that once at startup
instead of on every upload, which is a big win for repeated or
one-shot-heavy workloads. For a single upload you can skip this step and
pass `--peerlist` directly to `isheika upload`.

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

## Benchmarks

`.github/workflows/bench.yml` and `.circleci/config.yml` run a manual
mainnet upload benchmark on demand (`workflow_dispatch` on GitHub, the
`run_bench` pipeline parameter on CircleCI). They're never automatic —
each upload spends real BZZ. Throughput is bandwidth- and
peer-coverage-bound and moves a lot with the pool / buffer / overlay
knobs; `PERFORMANCE.md` has the empirical-ceilings table and the full
sweep behind each one.

## Compatibility

Tracks the upstream [bee][bee] mainnet protocols:

| Protocol     | Versions accepted                         | Notes                            |
| ------------ | ----------------------------------------- | -------------------------------- |
| handshake    | `15.0.0` (preferred), `14.0.0` (fallback) |                                  |
| hive         | `2.0.0` (preferred), `1.1.0` (fallback)   |                                  |
| retrieval    | `1.4.0`                                   |                                  |
| pushsync     | `1.3.1`                                   |                                  |
| pricing      | `1.0.0`                                   |                                  |
| pseudosettle | `1.0.0`                                   |                                  |
| status       | `1.1.3`                                   | inbound-only                     |
| swap         | `1.0.0`                                   | cheque issuance only, no cashout |

[bee]: https://github.com/ethersphere/bee

## Status

Not stable. The crate is at `0.1.2` and the API will change. Useful as
an audit reference for the bee protocols, and as a deployment client when
you need uploads from somewhere bee won't run — WASM, CI, light or
otherwise constrained environments.
