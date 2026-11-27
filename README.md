<img src="./logo.svg" width="75" height="75" alt="hoverfly logo" />

# hoverfly

Extremely fast, minimal and portable [Swarm][swarm] light client. Upload and
download files to decentralized storage natively and in a browser.

[swarm]: https://www.ethswarm.org/

## Features

- **Runs in the browser.** Try the [demo](https://hoverfly.bzz.link) or embed
  it with [`@omnipin/hoverfly`](https://www.npmjs.com/package/@omnipin/hoverfly).
- **Collections.** Upload, download and list tarballs as Swarm manifests.
- **Onchain postage batches.** Buy storage straight from the CLI.
- **Three modes.** One-shot commands, a daemon with a warm connection pool, and
  pusher relays that split signing from uploading.
- **Cross-platform.** WebAssembly, Linux x86/ARM, macOS and FreeBSD.
- **Small.** 7MB gzipped, 17MB unpacked (x86 Linux).
- **CI-friendly.** ~1-1.6 MiB/s uploads in GitHub Actions.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/omnipin/hoverfly/main/install.sh | sh
```

This installs to `~/.local/bin`. Pin a version with `HOVERFLY_VERSION=v0.3.2`
before `sh`, or build from source:

```sh
cargo install --git https://github.com/omnipin/hoverfly
```

## Setup

Run every step from the same directory: hoverfly reads `peers.json` and
`overlay-nonce` from the current directory by default.

### 1. Create a key

```sh
cast wallet new
export KEY=0x...   # the printed private key
```

The key signs your postage stamps and identifies you to the network. Fund the
printed address in step 4.

### 2. Fetch a peerlist

```sh
curl -fsSL -o peers.json https://raw.githubusercontent.com/omnipin/hoverfly-peers/main/peers.seed.json
```

The list is refreshed hourly. hoverfly records which peers it reached in this
file, so later runs start faster.

### 3. Pick an overlay nonce

```sh
hoverfly vanity-overlay --key $KEY
```

This takes a few seconds to minutes and writes `overlay-nonce`. Generating it
up front keeps uploads fast. Keep the file with your key: together they make up
your identity.

### 4. Get xDAI and BZZ on Gnosis

Your address needs a little xDAI for gas and some BZZ to pay for storage. If
you already have them, skip ahead. Otherwise `bridge` swaps them in through
[Relay](https://docs.relay.link), for example from 3 USDC on Base:

```sh
hoverfly bridge --from-chain base --from-token USDC --amount 3 \
  --rpc-url https://mainnet.base.org --key $KEY
```

You need a little ETH on the origin chain for gas.

### 5. Buy a postage batch

```sh
hoverfly batch create --rpc-url https://rpc.gnosischain.com --key $KEY --size 2GB --duration 30d
export BATCH=0x...   # the printed batch_id
```

`--size` and `--duration` follow the
[official stamp calculator](https://docs.ethswarm.org/docs/develop/tools-and-features/buy-a-stamp-batch/#calculators).
Bee nodes only accept stamps from the new batch 1-3 minutes after it lands
on-chain, so wait a few minutes before the first upload.

### 6. Upload

```sh
hoverfly upload --batch $BATCH --key $KEY path/to/file
```

It prints the root reference and a `bzz.limo` link to the file.

For repeated uploads, start a daemon in another terminal. It keeps a pool of
peer connections open, so each upload skips connecting to the network first:

```sh
hoverfly daemon --socket /tmp/hoverfly.sock
hoverfly upload --daemon /tmp/hoverfly.sock --batch $BATCH --key $KEY path/to/file
```

### Redundancy

Uploads use Reed–Solomon erasure coding at level `medium` by default, the same
as a bee gateway. The parity chunks keep a fresh upload readable while its data
chunks are still reaching their neighbourhood ([bee#5541]). They add about 8%
more chunks (and postage) on large files, and more on small ones.

```sh
hoverfly upload --redundancy none   ...   # no parity, cheapest
hoverfly upload --redundancy strong ...   # more parity
```

The level is part of the reference, so the same file gets a different
reference at each level. `hoverfly bmt --redundancy <level> <file>` computes it
offline.

[bee#5541]: https://github.com/ethersphere/bee/issues/5541

## Compatibility

Tracks the upstream [bee][bee] mainnet protocols:

| Protocol     | Version  | Notes                            |
| ------------ | -------- | -------------------------------- |
| handshake    | `15.0.0` |                                  |
| hive         | `2.0.0`  |                                  |
| retrieval    | `1.4.0`  |                                  |
| pushsync     | `1.3.1`  |                                  |
| pricing      | `1.0.0`  |                                  |
| pseudosettle | `1.0.0`  |                                  |
| status       | `1.1.3`  | inbound-only                     |
| swap         | `1.0.0`  | cheque issuance only, no cashout |

[bee]: https://github.com/ethersphere/bee
