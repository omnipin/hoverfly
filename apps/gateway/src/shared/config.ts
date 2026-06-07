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
/** Per-chunk retry budget for fetches. */
export const FETCH_RETRIES = 6
/** Cloudflare is hoverfly's built-in default; leave undefined to use it. */
export const DOH_URL: string | undefined = undefined

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
 * IndexedDB database name for the persistent, content-addressed chunk cache
 * (L2). Managed inside the hoverfly wasm via `enableChunkStore`. Immutable
 * Swarm chunks persist here across fetches and sessions, on top of the SW's
 * file-level Cache API.
 */
export const IDB_CHUNKS_DB = 'hoverfly-gw-chunks'
export const LS_USER_PEERS = 'hoverfly-gw:user-wss-peers'

// ---- caching ----
export const CONTENT_CACHE = 'swarm-content-v2'
