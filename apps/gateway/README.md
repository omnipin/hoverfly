# hoverfly Swarm gateway (in-browser, subdomain)

A browser-only [subdomain gateway](https://specs.ipfs.tech/http-gateways/subdomain-gateway/)
for **Ethereum Swarm**, inspired by the IPFS
[service-worker-gateway](https://github.com/ipfs/service-worker-gateway). It
fetches and verifies Swarm websites **entirely in the browser** using
[hoverfly](https://github.com/omnipin/hoverfly) (the Rust Swarm micro-client)
compiled to WebAssembly — no backend gateway, no Bee node to run.

Each site is served from its own origin (`<cid>.bzz.localhost`) for proper
origin isolation, just like `*.ipfs.dweb.link`.

```
┌─────────────────────────────────────────────────────────────────────┐
│  bzz.localhost:3000   (gateway root + shared daemon origin)           │
│  ├── landing page  → enter a Swarm reference, redirect to subdomain   │
│  ├── daemon.js     → SharedWorker: ONE hoverfly node for the whole     │
│  │                   gateway (warm peers + warm session cache)        │
│  └── daemon-frame.html → broker iframe embedded by content origins    │
└─────────────────────────────────────────────────────────────────────┘
        ▲ MessagePort (cross-origin, via the broker iframe)
        │
┌───────┴─────────────────────────────────────────────────────────────┐
│  <cid>.bzz.localhost:3000   (one origin per site)                     │
│  ├── boot shell (top document) ── holds the daemon bridge             │
│  │     └── <iframe> the real site ── served by the service worker     │
│  └── sw.js → routes requests; resolves <path> against the mantaray    │
│              manifest via the daemon and returns Response objects      │
└───────────────────────────────────────────────────────────────────────┘
```

## "Daemon mode", in the browser

hoverfly's native daemon (`src/daemon.rs`) is a Unix-socket process and can't
run in a browser. The point of running a daemon — a **long-lived node that
keeps peers and sessions warm for better stability** — is recreated here with a
**`SharedWorker` on the gateway root origin**. A `SharedWorker` is keyed by
origin + script URL, so every gateway tab and every content subdomain's broker
iframe connects to the **same instance**: one hoverfly node, one warm
`PeerStore`, one warm retrieval cache (session pool + peer scoreboard) shared
across all sites. The first request pays discovery; every later one reuses live
forwarders.

The daemon calls `start()` on the wasm client, which runs one eager discovery
round, opens a warm pool of retrieval sessions, and then keeps both topped up
in the background — but only while no fetch is in flight, since dialing on the
browser's single ws+yamux connection driver resets in-flight retrieval
substreams. Requests go through `fetchManifestPath(root, path, retries)`, which
resolves `path` through the mantaray manifest (and through a feed, for mutable
sites) and returns `{ bytes, contentType, feedResolved }`.

## Running

```bash
# 1. Build the threaded hoverfly wasm package (from the repo root) if pkg/ is
#    stale. `wasm-threads` is required: the gateway runs the node over shared
#    memory with a thread pool. The wasm-bindgen CLI version must match the
#    `wasm-bindgen` crate version in Cargo.lock.
RUSTUP_TOOLCHAIN=nightly cargo build --release --locked \
  --target wasm32-unknown-unknown --no-default-features --features wasm-threads --lib
wasm-bindgen --target web --out-dir pkg \
  target/wasm32-unknown-unknown/release/hoverfly.wasm

# 2. Build + serve the gateway:
cd apps/gateway
pnpm install
pnpm start            # builds, then serves on http://bzz.localhost:3000
```

Open **http://bzz.localhost:3000** in Chrome. (`*.localhost` resolves to
127.0.0.1 automatically — no `/etc/hosts` needed.) Enter a Swarm reference
(64-char hex), a swarm CID (`b…`) or an ENS name (`swarm.eth`) and it opens at
`<cid>.bzz.localhost:3000`. ENS names are resolved through the ENS Universal
Resolver (so offchain/wildcard names work too) and must carry a Swarm
contenthash.

`pnpm watch` rebuilds on source changes (re-run `pnpm build` after editing
`public/`). `pnpm selftest` runs the pure-logic tests.

## Connectivity: browser-dialable peers

Browsers can't open raw TCP, so the wasm build dials **`/ws` / `/wss` only**.
The good news: Swarm mainnet bootnodes (and bee's AutoTLS / `libp2p.direct`
feature) advertise secure WebSocket underlays, e.g.

```
/ip4/135.181.84.53/tcp/1635/tls/sni/135-181-84-53.<hash>.libp2p.direct/ws/p2p/Qm…
```

which a browser dials as `wss://135-181-84-53.<hash>.libp2p.direct:1635` (valid
AutoTLS cert). The caveat: these are **scarce** — in a 617-peer mainnet harvest
only ~0.6% exposed `/ws`; the rest are TCP-only. That's still workable because
Swarm retrieval **forwards recursively**, so a few well-connected ws full nodes
can serve arbitrary chunks.

What the daemon does:

- Seeds from **`peers.ws.json`** (ws peers harvested from mainnet) so the
  first fetch has something to dial immediately. It's re-derived from
  `peers.seed.json` hourly in the
  [`omnipin/hoverfly-peers`](https://github.com/omnipin/hoverfly-peers) repo
  (ws/wss underlays only — these go stale within ~2-3h, so the seed is kept
  fresh well inside that window) and fetched from the CDN on every daemon
  start and every 30 minutes after. The fresh seed replaces the peer store
  rather than merging into it: a store saved on an earlier visit is mostly
  rotated-away underlays. The store persisted in IndexedDB is only used when
  the CDN is unreachable, and the snapshot `build.js` vendors into
  `dist/__gw__/` is the last resort.
- Runs `discover(/dnsaddr/mainnet.ethswarm.org)` in the background to pick up
  any extra ws peers from the live bootnodes.

The landing page shows the live **connected peer** count (open retrieval
sessions) and a **Discover** box. For best reliability, point discovery at a
WebSocket-capable bee you control (`/ip4/…/tcp/…/tls/sni/…/ws/p2p/…` or
`/dns4/host/tcp/443/wss/p2p/…`).

> Regenerate the seed by hand (the cron in `omnipin/hoverfly-peers` normally
> does this):
> `target/release/hoverfly discover /dnsaddr/mainnet.ethswarm.org --rounds 3 --ws-only -o peers.ws.json`.

## How a request is served

1. First navigation to `<cid>.bzz.localhost/…` → the dev server returns the
   **boot shell** (`boot.html`). The SW can't stream top-level HTML from an
   *external* daemon (it has no client to bridge through before the page
   exists), so the shell is the chicken-and-egg fix.
2. The shell registers the SW, embeds the cross-origin **broker iframe**
   (`daemon-frame.html` on `bzz.localhost`), opens a `MessagePort` to the shared
   daemon, and **mints a second port for the SW**.
3. The shell loads the real site in a full-viewport `<iframe>`.
4. The iframe's document + subresource requests (`destination` ≠ `document`) are
   intercepted by the SW, which calls `fetchPath(ref, path)` on the daemon and
   returns a `Response` (Content-Type from manifest metadata; directories fall
   back to `index.html`). Content-addressed responses are cached; responses
   resolved through a feed are mutable and never cached.

## Cross-origin isolation

The hoverfly wasm is built with shared memory (atomics), so pages must be
**cross-origin isolated**. The dev server sends `Cross-Origin-Opener-Policy:
same-origin` + `Cross-Origin-Embedder-Policy: require-corp` +
`Cross-Origin-Resource-Policy: cross-origin` on everything. `require-corp` (not
`credentialless`) is required: `credentialless` partitions the cross-origin
broker iframe into a **separate agent cluster**, so its `postMessage` to the
content-origin shell wouldn't land and the daemon `SharedWorker` wouldn't be
genuinely shared. `require-corp` keeps the broker iframe in the same agent
cluster — embeddable cross-subdomain via the `Cross-Origin-Resource-Policy:
cross-origin` header above — and the SAB-backed SharedWorker stays shared. The
broker iframe is granted `allow="cross-origin-isolated"` so it can host the
SharedWorker. Chrome-first (uses `request.destination`, module SharedWorkers).

## Layout

```
src/shared/   swarm-cid.ts (CIDv1 <-> ref, ports cid.rs; ENS contenthash decode),
              ens.ts (ENS -> Swarm ref via the Universal Resolver), swarm-ref,
              parse-request, config, protocol (daemon RPC + DaemonRpc client), bytes
src/daemon/   daemon.ts          SharedWorker: the one warm hoverfly node
src/frame/    frame.ts           broker iframe (relays ports to the daemon)
src/sw/       sw.ts              content-origin service worker
src/boot/     boot.ts            subdomain boot shell + content iframe
src/app/      landing.ts         root landing page + daemon status
public/       index.html, boot.html, __gw__/ (daemon-frame.html, unregister.html,
              styles.css, favicon.svg)
build.js      esbuild + vendors ../../pkg into dist/__gw__/hoverfly/
serve.js      subdomain-aware dev server with isolation headers
Caddyfile     production static server (on-demand TLS per <cid> subdomain)
deploy/       ask-tls.js + systemd unit: the Caddy on-demand TLS gatekeeper
scripts/      selftest.ts        pure-logic tests (CID codec, host parsing, ENS decode)
```

## Limitations

- Mainnet retrieval depends on reachable `/ws[s]` peers (see caveat).
- The served site renders inside an iframe (address bar stays at the shell URL);
  in-site navigation works within the iframe.
- Encrypted references aren't supported.
- Targets recent Chrome.
