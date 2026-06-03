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

export const SW_SCRIPT = `${ASSET_PREFIX}sw.js`
export const BOOT_SCRIPT = `${ASSET_PREFIX}boot.js`
export const LANDING_SCRIPT = `${ASSET_PREFIX}landing.js`
export const DAEMON_WORKER_SCRIPT = `${ASSET_PREFIX}daemon.js`
export const DAEMON_FRAME_PATH = `${ASSET_PREFIX}daemon-frame.html`
/** wasm-bindgen `--target web` entry, vendored from the repo's pkg/. */
export const ISHEIKA_JS = `${ASSET_PREFIX}isheika/isheika.js`

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
/** Per-chunk retry budget for fetches. */
export const FETCH_RETRIES = 6
/** Cloudflare is isheika's built-in default; leave undefined to use it. */
export const DOH_URL: string | undefined = undefined

// ---- persistence ----
export const IDB_NAME = 'isheika-gateway'
export const IDB_STORE = 'kv'
export const IDB_PEERS_KEY = 'peerstore-json'
export const LS_USER_PEERS = 'isheika-gw:user-wss-peers'

// ---- caching ----
export const CONTENT_CACHE = 'swarm-content-v2'
