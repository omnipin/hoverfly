# hoverfly

Experimental [Swarm][swarm] light client. Works natively and in a browser.

[swarm]: https://www.ethswarm.org/

## Features

- **Light node functionality.** End-to-end content and upload and download.
- **Static peerlist bootstrap.** Faster peer discovery with cached peer info.
- **Collection support.** Upload, download and list content-addressable tarballs.
- **Onchain postage batch creation.** Single command postage batch issuance, no `bee` needed.
- **One-shot and daemon modes.** Static commands for ease of use, daemon mode for max performance and warm connection pool.
- **Cross-platform.** Supports WebAssembly, Linux x86/ARM, MacOS and FreeBSD.
- **JavaScript bindings.** Use [`@omnipin/hoverfly`](https://www.npmjs.com/package/@omnipin/hoverfly) in a browser.
- **Small size.** 5MB gzipped, 14MB unpacked x86 Linux binary.
- **Build-provenance attestation.** Each release is signed via SLSA. Verify via `gh attestation verify`.
- **CI-friendly.** ~400-500KB/s uploads in GitHub Actions.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/omnipin/hoverfly/main/install.sh | sh
```

### Specific version

```sh
curl -fsSL https://raw.githubusercontent.com/omnipin/hoverfly/main/install.sh | HOVERFLY_VERSION=v0.1.2 sh
```

### Build from source

```bash
cargo install --git https://github.com/omnipin/hoverfly
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
low kademlia bins and get dropped right after the handshake. `hoverfly
vanity-overlay` searches for a nonce that puts you in deeper,
undersaturated bins instead. Anchoring against a few stable peers
(`--target-overlay`) roughly **doubled** upload throughput in testing —
see `PERFORMANCE.md` for the method and numbers.

```bash
hoverfly vanity-overlay --key 0xYOUR_KEY --output overlay-nonce
```

One-time, CPU-bound (seconds to minutes). The resulting `overlay-nonce` is your Swarm identity together with `--key` — keep it.

### 3. Create a postage batch

Uploads need a postage stamp batch on-chain. Fund the address from step 1 with a little xDAI (for gas) and some BZZ (for the batch itself), then:

```bash
hoverfly batch create --rpc-url https://rpc.gnosischain.com --key 0xYOUR_KEY --size 2GB --duration 30d
```

`--size` and `--duration` map to `--depth` and `--amount-per-chunk` via the same formulas as the [official postage stamp
calculator](https://docs.ethswarm.org/docs/develop/tools-and-features/buy-a-stamp-batch/#calculators).

The on-chain `BatchCreated` event takes 1-3 minutes to propagate to the bee nodes that'll accept your stamps. Poll [Swarmscan](https://swarmscan.io/) until it 200s:

```bash
curl -s "https://api.swarmscan.io/v1/postage/batches/<BATCH_ID>"
# 404 = network hasn't indexed it yet
# 200 with a JSON body = ready to use
```

### 4. Run the daemon

A long-lived daemon holds a warm session pool across uploads. Filling a 256-session pool takes ~80 s; the daemon pays that once at startup
instead of on every upload, which is a big win for repeated or one-shot-heavy workloads. For a single upload you can skip this step and pass `--peerlist` directly to `hoverfly upload`.

```bash
hoverfly daemon --socket /tmp/hoverfly.sock --pool-size 256 --listen /ip4/0.0.0.0/tcp/1635 --identity 0xYOUR_KEY --advertise /ip4/YOUR_PUBLIC_IP/tcp/1635 --discover-rounds 3
```

The repo ships a curated `peers.seed.json`; the daemon loads it via `--peerlist` (default: `peers.json`) for fast cold-start without running `discover` first.

```bash
cp peers.seed.json peers.json
```

### 5. Upload

```bash
hoverfly upload --daemon /tmp/hoverfly.sock --batch YOUR_BATCH_ID_HEX --key 0xYOUR_KEY path/to/file.bin
```

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
