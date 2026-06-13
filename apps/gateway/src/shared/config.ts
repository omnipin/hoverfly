// Static configuration for the in-browser Swarm subdomain gateway.

/**
 * Subdomain infix label. Content lives at `<cid>.bzz.<host>`; the gateway
 * root / shared daemon lives at `bzz.<host>` (e.g. `bzz.localhost:3000`).
 */
export const GATEWAY_INFIX = 'bzz'

/**
 * Namespace for the gateway's own assets, kept off the Swarm path space so a
 * served site can use any path without colliding (everything NOT under this
 * prefix on a content origin is treated as Swarm content).
 */
export const ASSET_PREFIX = '/__gw__/'

/**
 * Query marker the boot shell appends when it loads the real site into its
 * content iframe (`/?__gw_content=1`). Both the top shell navigation and the
 * inner content-iframe navigation are `mode: 'navigate'` + `destination:
 * 'document'`, so the service worker cannot otherwise tell them apart — without
 * this it would pass the iframe navigation through to the network too, which
 * returns the boot shell again (Caddy `try_files {path} /boot.html`). The
 * iframe would then re-run boot.js, see `window.top !== window`, and bail —
 * leaving a blank page and never fetching any Swarm chunks. The SW treats a
 * document navigation carrying this marker as Swarm content; it is stripped
 * before the path is resolved against the manifest.
 */
export const CONTENT_MARKER = '__gw_content'

export const SW_SCRIPT = `${ASSET_PREFIX}sw.js`
export const BOOT_SCRIPT = `${ASSET_PREFIX}boot.js`
export const LANDING_SCRIPT = `${ASSET_PREFIX}landing.js`

/**
 * Build version, stamped by build.js (`define`) from the vendored wasm's
 * content hash. Used to key the daemon SharedWorker so a new deploy spawns a
 * fresh instance instead of rejoining the stale one the browser keeps alive
 * across reloads. Defaults to `'dev'` when not injected (e.g. typecheck).
 */
declare const __GW_VERSION__: string | undefined
export const GW_VERSION: string =
  typeof __GW_VERSION__ !== 'undefined' ? __GW_VERSION__ : 'dev'

/**
 * Daemon SharedWorker script URL + name. A SharedWorker is keyed by
 * (origin, script URL, name), so these are kept STABLE (no per-deploy version
 * tag) — every tab and every content-subdomain broker iframe joins the exact
 * same single instance, instead of a new deploy spawning a parallel worker that
 * coexists with the old one until every stale client closes (which left several
 * daemons running at once, each holding its own wss connections).
 *
 * The previous version tag existed to dodge a "wedged stale daemon" after a wasm
 * change; that's handled instead by the daemon's own self-healing (the
 * maintenance loop re-discovers and re-warms continuously) and by clients
 * fetching `daemon.js` fresh on a cold start. The cost of a stable key — a
 * just-deployed tab can rejoin an already-running older worker — is acceptable:
 * one warm daemon beats N stale ones, and the next time every tab is closed the
 * worker dies and the new code loads.
 */
export const DAEMON_WORKER_SCRIPT = `${ASSET_PREFIX}daemon.js`
/** Matching SharedWorker `name` (also part of the worker key). Stable. */
export const DAEMON_WORKER_NAME = 'hoverfly-daemon'
export const DAEMON_FRAME_PATH = `${ASSET_PREFIX}daemon-frame.html`
/** wasm-bindgen `--target web` entry, vendored from the repo's pkg/. */
export const HOVERFLY_JS = `${ASSET_PREFIX}hoverfly/hoverfly.js`

// ---- Swarm network ----
export const NETWORK_ID = 1 // 1 = mainnet, 10 = testnet/sepolia
export const DEFAULT_BOOTSTRAP = '/dnsaddr/mainnet.ethswarm.org'
/** Seconds to wait for hive announcements per dialed underlay during discover. */
export const DISCOVER_WAIT_SECS = 8
/**
 * How often (seconds) the in-browser daemon re-runs discovery in the
 * background to keep its peer set warm. Mirrors the native daemon's
 * maintenance loop. The first round runs eagerly at `start()`.
 */
export const DAEMON_REFRESH_SECS = 45
/**
 * How often (seconds) the daemon re-reads its live "connected peers" count and
 * broadcasts it to the UI. Decoupled from DAEMON_REFRESH_SECS: reading the
 * session count is cheap (a single lock-guarded `len()` — no dialing), so it can
 * tick fast for a live-feeling counter, while the expensive discovery + pool
 * re-warm (which dials peers and must stay infrequent to avoid colliding with
 * in-flight fetches on the single ws+yamux driver) stays on DAEMON_REFRESH_SECS.
 */
export const STATUS_POLL_SECS = 5
/**
 * Per-chunk peer-attempt cap for fetches — how many candidate forwarders a
 * single chunk tries before failing. On the thin, flaky browser /ws pool a
 * given chunk commonly needs to walk past several reachable-but-empty peers
 * (`storage: not found`) or dead dials before hitting a forwarder that has it,
 * so a small cap (was 6) made LARGE files fail: a multi-hundred-chunk video
 * fails entirely if any ONE chunk exhausts its 6 attempts (observed:
 * `all peers failed (6/6 attempted): timeout`). With ~262 candidates per chunk,
 * a much larger cap lets retrieval explore enough forwarders to find the chunk;
 * a genuinely unretrievable chunk still bails at the daemon's 90s per-fetch
 * ceiling, so this can't hang. (0 would mean "try every candidate, no cap".)
 */
export const FETCH_RETRIES = 24
/**
 * Target size of the warm retrieval-session pool — the wasm "daemon mode"
 * connection pool. The daemon proactively opens sessions to dialable ws peers
 * and the wasm maintenance loop keeps the pool topped up in the background (see
 * `HoverflyClient::start`'s `warm_pool` arg), so the warm forwarder set — and
 * the "connected peers" the gateway shows — climbs at idle and the first site
 * load reuses live sessions instead of dialing cold.
 *
 * `0` means UNLIMITED: warm every reachable dialable peer (the effective
 * ceiling is just how many of the scarce, flaky browser /ws peers actually
 * accept a connection). A positive value caps the pool at that many sessions.
 * Passed to `start()` so warming happens inside wasm (between page loads, gated
 * on no in-flight fetch). Also used by the JS-side prewarm nudge/poll.
 */
export const PREWARM_SESSIONS = 0
/** Cloudflare is hoverfly's built-in default; leave undefined to use it. */
export const DOH_URL: string | undefined = undefined
/**
 * Cold-start peer seed, fetched fresh from the GitHub raw CDN at daemon warm
 * time. The `refresh-peers` workflow re-derives `peers.ws.json` from a live
 * mainnet harvest HOURLY and commits it to `main`; mainnet /ws[s] underlays go
 * stale within ~2-3h (AutoTLS SNI rotation, churn), so the committed file is the
 * freshest seed available. The locally-bundled copy (`peers.ws.json` in dist/,
 * a build-time symlink snapshot) is only as fresh as the last gateway DEPLOY —
 * which can be days old — so we prefer the CDN at runtime and fall back to the
 * local copy only when the CDN is unreachable (offline / GitHub down / rate
 * limited). raw.githubusercontent.com serves it with `access-control-allow-
 * origin: *` + `cross-origin-resource-policy: cross-origin` (so the fetch
 * succeeds from the cross-origin-isolated daemon) and a 5-minute `max-age` (so
 * it tracks the hourly cron closely without hammering origin). Set to undefined
 * to disable the CDN seed and use only the bundled copy.
 */
export const PEERS_SEED_URL: string | undefined =
  'https://raw.githubusercontent.com/omnipin/hoverfly/main/peers.ws.json'

// ---- persistence ----
export const IDB_NAME = 'hoverfly-gateway'
export const IDB_STORE = 'kv'
export const IDB_PEERS_KEY = 'peerstore-json'
/**
 * Persisted browser-daemon identity. The node's Swarm overlay is
 * `keccak256(eth_addr || network_id || nonce)`, so a stable identity across
 * page loads/sessions requires persisting BOTH the secp256k1 node key and the
 * overlay nonce — replaying only the key still rotates the overlay every
 * launch. Stored as 32-byte hex strings in the kv store on the root origin.
 */
export const IDB_NODEKEY_KEY = 'node-key-hex'
export const IDB_NONCE_KEY = 'node-nonce-hex'
/**
 * Persisted feed head-index hints (`exportFeedHints`/`loadFeedHints`). A feed's
 * head only moves forward, so the last resolved index lets a returning visitor
 * resolve a feed (e.g. swarm.eth) in ~1 fast round from the cached head instead
 * of a cold gallop from index 0 (~30s observed on the thin browser pool).
 */
export const IDB_FEED_HINTS_KEY = 'feed-hints-json'
/**
 * IndexedDB database name for the persistent, content-addressed chunk cache
 * (L2). Managed inside the hoverfly wasm via `enableChunkStore`. Immutable
 * Swarm chunks persist here across fetches and sessions, on top of the SW's
 * file-level Cache API.
 */
export const IDB_CHUNKS_DB = 'hoverfly-gw-chunks'
export const LS_USER_PEERS = 'hoverfly-gw:user-wss-peers'

// ---- caching ----
export const CONTENT_CACHE = 'swarm-content-v2'
