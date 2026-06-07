# @omnipin/hoverfly

Minimal [Swarm](https://www.ethswarm.org/) (Ethereum Swarm) micro-client for the
browser — `discover`, `fetch`, and `upload` over libp2p WebSocket. This is the
WebAssembly build of [`omnipin/hoverfly`](https://github.com/omnipin/hoverfly); for
the native CLI, see the repository.

## Install

```sh
npm install @omnipin/hoverfly
```

## Usage

ESM, no bundler required (this is a `wasm-bindgen --target web` build — call the
default `init()` once to load the wasm module):

```js
import init, { HoverflyClient, initThreadPool } from "@omnipin/hoverfly";

await init();                                        // instantiate the wasm
await initThreadPool(navigator.hardwareConcurrency); // optional: parallel hashing

// All constructor args optional: (privateKeyHex?, networkId = 1, dohUrl?, timeoutSecs = 30)
const client = new HoverflyClient();

const peers = await client.discover("/dnsaddr/mainnet.ethswarm.org", 5); // -> peer count
const bytes = await client.fetch(rootHex, /* maxRetries */ 3);           // -> Uint8Array

// upload needs a signer key and a postage batch:
const signer = new HoverflyClient(privateKeyHex);
const root = await signer.upload(data, batchIdHex, depth, 3);            // -> root hash hex
```

Peer-store helpers: `loadPeers(json)`, `exportPeers()`, `peerCount()`.

## Cross-origin isolation

The module is built with threads (atomics + shared memory), so the page must be
[cross-origin isolated](https://developer.mozilla.org/docs/Web/API/Window/crossOriginIsolated)
to use `SharedArrayBuffer`. Serve the page with:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

## License

MIT — see [omnipin/hoverfly](https://github.com/omnipin/hoverfly).
