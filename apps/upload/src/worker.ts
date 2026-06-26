/// <reference lib="webworker" />
//
// hoverfly node Worker.
//
// Owns the wasm HoverflyClient and runs everything off the main thread: wasm
// instantiation, the rayon hashing/stamping pool, libp2p dial churn, the
// background discover/warm loop, and hoverfly's verbose `INFO` tracing. The
// page talks to it over postMessage (see worker-protocol.ts), so none of that
// work can jank the UI — which it badly did when the node ran in the foreground
// (hundreds of wss dials + per-dial console logs on the main thread).

import {
  DEFAULT_BOOTSTRAP, DISCOVER_WAIT_SECS, HOVERFLY_JS, IDB_NAME, IDB_PEERS_KEY,
  IDB_STORE, MAINTENANCE_SECS, NETWORK_ID, PEERS_SEED_BUNDLED, PEERS_SEED_URL,
  STATUS_POLL_SECS, UPLOAD_RETRIES, WARM_POOL
} from './config.ts'
import type { Req, Res } from './worker-protocol.ts'

declare const self: DedicatedWorkerGlobalScope

interface HoverflyClient {
  start: (bootstrap: string, intervalSecs: number, waitSecs: number, warmPool?: number) => Promise<number>
  loadPeers: (json: string) => void
  mergePeers: (json: string) => number
  exportPeers: () => string
  peerCount: () => number
  connectedPeerCount?: () => Promise<number>
  uploadFile: (data: Uint8Array, path: string, contentType: string | undefined, batchIdHex: string, depth: number, maxRetries: number) => Promise<string>
  uploadCollection: (files: Array<{ path: string, data: Uint8Array, contentType?: string }>, indexDocument: string | undefined, errorDocument: string | undefined, batchIdHex: string, depth: number, maxRetries: number) => Promise<string>
}
interface HoverflyModule {
  default: (input?: unknown) => Promise<unknown>
  initThreadPool?: (n: number) => Promise<unknown>
  HoverflyClient: new (
    key?: string | null, networkId?: bigint | null, doh?: string | null,
    timeout?: number | null, nonceHex?: string | null
  ) => HoverflyClient
}

const HOVERFLY_URL = new URL(HOVERFLY_JS, self.location.href).href

let client: HoverflyClient | null = null
let startPromise: Promise<void> | null = null

function log (message: string): void { post({ kind: 'log', message }) }
function post (msg: Res, transfer?: Transferable[]): void {
  self.postMessage(msg, transfer ?? [])
}

// ---- peer-store persistence (mirrors the gateway daemon) ----
function idb (): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, 1)
    req.onupgradeneeded = () => req.result.createObjectStore(IDB_STORE)
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
}
async function idbGet (key: string): Promise<string | undefined> {
  try {
    const db = await idb()
    return await new Promise((resolve, reject) => {
      const r = db.transaction(IDB_STORE, 'readonly').objectStore(IDB_STORE).get(key)
      r.onsuccess = () => resolve(r.result as string | undefined)
      r.onerror = () => reject(r.error)
    })
  } catch { return undefined }
}
async function idbSet (key: string, value: string): Promise<void> {
  try {
    const db = await idb()
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, 'readwrite')
      tx.objectStore(IDB_STORE).put(value, key)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
  } catch { /* best effort */ }
}

/** CDN-first cold-start seed, bundled fallback. Mirrors gateway `loadSeed`. */
async function loadSeed (): Promise<string | undefined> {
  if (PEERS_SEED_URL != null) {
    try {
      const ctrl = new AbortController()
      const t = setTimeout(() => ctrl.abort(), 5_000)
      const resp = await fetch(PEERS_SEED_URL, { signal: ctrl.signal, cache: 'no-store' })
      clearTimeout(t)
      if (resp.ok) { log('Peer seed: loaded fresh from CDN'); return await resp.text() }
      log(`Peer seed: CDN returned ${resp.status} — falling back to bundled copy`)
    } catch { log('Peer seed: CDN fetch failed — falling back to bundled copy') }
  }
  try {
    const resp = await fetch(new URL(PEERS_SEED_BUNDLED, self.location.href).href)
    if (resp.ok) { log('Peer seed: loaded bundled copy'); return await resp.text() }
  } catch { /* offline */ }
  return undefined
}

async function loadModule (): Promise<HoverflyModule> {
  // No crossOriginIsolated check: this build uses the no-shared-memory hoverfly
  // wasm (built threadless by build-wasm.sh — no wasm-bindgen-rayon, plain linear
  // memory), so SharedArrayBuffer / COOP / COEP are NOT required. That's what
  // lets the dApp run on the eth.limo ENS gateway. There is no initThreadPool to
  // call; nectar's parallel splitter (`sync_split`) runs inline on this single
  // worker thread (no rayon pool), which is correct and also sidesteps the
  // wasm `parking_lot` "Parking not supported" panic that a contended pool hit.
  log('Loading hoverfly wasm…')
  const mod = await import(/* @vite-ignore */ HOVERFLY_URL) as HoverflyModule
  await mod.default()
  log('hoverfly wasm ready (single-threaded hashing)')
  return mod
}

/** One-time node bring-up: wasm → client → seed → discover/warm → persist. */
async function start (sessionKeyHex: string): Promise<void> {
  if (startPromise != null) return startPromise
  startPromise = (async () => {
    const mod = await loadModule()
    log('Constructing hoverfly client (session-key signer)…')
    const c = new mod.HoverflyClient(sessionKeyHex, BigInt(NETWORK_ID), undefined, 30, undefined)
    client = c

    // Load the IndexedDB cache first (peers we actually reached last session),
    // then MERGE the freshly-fetched CDN seed on top. The cache alone goes
    // stale fast: mainnet /ws[s] underlays are AutoTLS SNI hostnames that
    // rotate within ~2-3h, so a cache from a previous session is mostly dead
    // underlays — dialing them spams the browser console with `can't establish
    // a connection` and finds nothing. The CDN seed is re-derived hourly
    // precisely to beat that rotation. `mergePeers` (NOT loadPeers, which
    // REPLACES the store) upserts the seed into the cache: underlays are
    // unioned and the newer reachability wins, so we keep last session's live
    // peers AND gain the fresh underlays. On a true cold start the cache is
    // absent and the seed is the only source.
    const saved = await idbGet(IDB_PEERS_KEY)
    if (saved != null) {
      try { c.loadPeers(saved); log(`Loaded ${c.peerCount()} peers from cache`) } catch (e) { console.warn(e) }
    }
    const seed = await loadSeed()
    if (seed != null) {
      try {
        const before = c.peerCount()
        const total = saved != null ? c.mergePeers(seed) : (c.loadPeers(seed), c.peerCount())
        log(`Merged fresh seed (+${total - before} new, ${total} total)`)
      } catch (e) { console.warn(e) }
    }

    log('Discovering browser-dialable peers…')
    const n = await c.start(DEFAULT_BOOTSTRAP, MAINTENANCE_SECS, DISCOVER_WAIT_SECS, WARM_POOL)
    log(`Discovery done: ${n} peers known`)
    await pushStatus()
    try { void idbSet(IDB_PEERS_KEY, c.exportPeers()) } catch { /* ignore */ }
    startStatusPoll()
  })()
  return startPromise
}

let statusTimer: ReturnType<typeof setInterval> | null = null
let lastConnected = -1
async function pushStatus (): Promise<void> {
  const c = client
  if (c?.connectedPeerCount == null) return
  try {
    const n = await c.connectedPeerCount()
    if (n !== lastConnected) { lastConnected = n; post({ kind: 'status', connected: n }) }
  } catch { /* ignore */ }
}
function startStatusPoll (): void {
  if (statusTimer != null) return
  statusTimer = setInterval(() => { void pushStatus() }, STATUS_POLL_SECS * 1000)
}

function requireClient (): HoverflyClient {
  if (client == null) throw new Error('node not started')
  return client
}

self.onmessage = async (e: MessageEvent<Req>) => {
  const msg = e.data
  try {
    switch (msg.kind) {
      case 'start':
        await start(msg.sessionKeyHex)
        post({ kind: 'result', id: msg.id, ok: true, value: null })
        break
      case 'connected': {
        let n = 0
        try { n = (await requireClient().connectedPeerCount?.()) ?? 0 } catch { /* 0 */ }
        post({ kind: 'result', id: msg.id, ok: true, value: n })
        break
      }
      case 'uploadFile': {
        const root = await requireClient().uploadFile(
          new Uint8Array(msg.data), msg.path, msg.contentType, msg.batchIdHex, msg.depth, UPLOAD_RETRIES
        )
        await pushStatus()
        post({ kind: 'result', id: msg.id, ok: true, value: root })
        break
      }
      case 'uploadCollection': {
        const files = msg.files.map(f => ({ path: f.path, data: new Uint8Array(f.data), contentType: f.contentType }))
        const root = await requireClient().uploadCollection(
          files, msg.indexDocument, msg.errorDocument, msg.batchIdHex, msg.depth, UPLOAD_RETRIES
        )
        await pushStatus()
        post({ kind: 'result', id: msg.id, ok: true, value: root })
        break
      }
    }
  } catch (err) {
    post({ kind: 'result', id: (msg as { id: number }).id, ok: false, error: err instanceof Error ? err.message : String(err) })
  }
}
