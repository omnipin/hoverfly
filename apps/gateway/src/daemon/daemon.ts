/// <reference lib="webworker" />
//
// The shared in-browser isheika "daemon".
//
// Runs as a SharedWorker on the gateway ROOT origin (bzz.<host>). Because a
// SharedWorker is keyed by origin + script URL, every gateway tab and every
// content subdomain's broker iframe connects to the *same* instance — so there
// is exactly one long-lived isheika node, one warm peer set and one warm
// session/score cache for the whole gateway. That is the "daemon mode → better
// peer stability" the native CLI gets from a long-running process, recreated
// in the browser. (The native unix-socket daemon in `src/daemon.rs` can't run
// in a browser; this is the in-browser analogue.)
//
// It serves RPC (see ../shared/protocol.ts) on each connection port and on any
// port transferred via an `attach` control message.

import {
  DAEMON_REFRESH_SECS, DEFAULT_BOOTSTRAP, DISCOVER_WAIT_SECS, DOH_URL,
  FETCH_RETRIES, IDB_NAME, IDB_PEERS_KEY, IDB_STORE, NETWORK_ID
} from '../shared/config.ts'
import { ATTACH, type DaemonStatus } from '../shared/protocol.ts'

declare const self: SharedWorkerGlobalScope & typeof globalThis

// --- isheika wasm glue (vendored, loaded at runtime so esbuild doesn't try to
//     bundle the wasm-bindgen module — it relies on import.meta.url). ---
interface ManifestFetch { readonly bytes: Uint8Array, readonly contentType: string | undefined }
interface IsheikaClient {
  /** Launch the daemon: eager initial discovery + a background maintenance
   *  loop that re-discovers every `intervalSecs`. Resolves with the peer
   *  count after the initial round. Idempotent. */
  start: (bootstrap: string, intervalSecs: number, waitSecs: number) => Promise<number>
  /** Stop the background maintenance loop. */
  stop: () => void
  /** Manual one-shot discovery round (the daemon loop does this automatically). */
  discover: (bootstrap: string, waitSecs: number) => Promise<number>
  fetchManifestPath: (rootHex: string, path: string, maxRetries: number) => Promise<ManifestFetch>
  fetch: (rootHex: string, maxRetries: number) => Promise<Uint8Array>
  loadPeers: (json: string) => void
  exportPeers: () => string
  peerCount: () => number
}
interface IsheikaModule {
  default: (input?: any) => Promise<unknown>
  initThreadPool?: (n: number) => Promise<unknown>
  IsheikaClient: new (key?: string | null, networkId?: bigint | null, doh?: string | null, timeout?: number | null) => IsheikaClient
}
// Resolved at runtime (relative to this worker script) so esbuild leaves the
// dynamic import alone — the wasm-bindgen module must load itself + its wasm
// via import.meta.url, so it must NOT be bundled.
const ISHEIKA_URL = new URL('isheika/isheika.js', self.location.href).href

const status: DaemonStatus = {
  ready: false,
  warming: false,
  peerCount: 0,
  dialable: 0,
  network: NETWORK_ID,
  bootstrap: DEFAULT_BOOTSTRAP
}

const ports = new Set<MessagePort>()
let client: IsheikaClient | null = null
let warmPromise: Promise<void> | null = null
let statusTimer: ReturnType<typeof setInterval> | null = null

// NB: every IsheikaClient method now takes `&self` (the node keeps its peers
// behind interior mutability and runs discovery on a background task inside the
// wasm daemon), so overlapping calls no longer trip wasm-bindgen's mutable-
// borrow guard. Fetches can run concurrently with each other and with the
// daemon's background maintenance — no JS-side serialization required.

// ---------------- IndexedDB (peer-store persistence) ----------------
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

// ---------------- helpers ----------------
function broadcastStatus (): void {
  const msg = { kind: 'event', event: 'status', status: { ...status } }
  for (const p of ports) {
    try { p.postMessage(msg) } catch { /* dead port */ }
  }
}

/** Count peers that expose at least one browser-dialable (/ws or /wss) underlay. */
function countDialable (peersJson: string): number {
  try {
    const parsed = JSON.parse(peersJson)
    const peers = parsed?.peers ?? {}
    let n = 0
    for (const overlay of Object.keys(peers)) {
      const underlays: string[] = peers[overlay]?.underlays ?? []
      if (underlays.some(u => /\/wss?(\/|$)/.test(u))) n++
    }
    return n
  } catch { return 0 }
}

function refreshCounts (): void {
  if (client == null) return
  try {
    status.peerCount = client.peerCount()
    status.dialable = countDialable(client.exportPeers())
  } catch { /* ignore */ }
}

function ensureWarm (): Promise<void> {
  if (warmPromise == null) warmPromise = warm()
  return warmPromise
}

async function warm (): Promise<void> {
  status.warming = true
  broadcastStatus()
  try {
    console.log('[daemon] warm: importing', ISHEIKA_URL)
    const mod = await import(/* @vite-ignore */ ISHEIKA_URL) as IsheikaModule
    console.log('[daemon] warm: init wasm')
    await mod.default() // instantiate wasm (shared memory; needs crossOriginIsolated)
    console.log('[daemon] warm: wasm ready')
    if (typeof mod.initThreadPool === 'function') {
      try { await mod.initThreadPool(self.navigator.hardwareConcurrency || 4) } catch (e) {
        // single-threaded fetch is fine; hashing parallelism is for uploads
        console.warn('[daemon] initThreadPool unavailable, continuing single-threaded:', e)
      }
    }
    const c = new mod.IsheikaClient(undefined, BigInt(NETWORK_ID), DOH_URL ?? undefined, 30)
    client = c

    const saved = await idbGet(IDB_PEERS_KEY)
    if (saved != null) {
      try { c.loadPeers(saved); refreshCounts() } catch (e) { console.warn('[daemon] loadPeers (idb) failed:', e) }
    } else {
      // First run: load the committed browser seed — peers harvested from
      // mainnet that expose a browser-dialable /ws (AutoTLS / libp2p.direct)
      // underlay. Gives the first fetch something to dial before discovery.
      try {
        const resp = await fetch(new URL('peers.ws.json', self.location.href).href)
        if (resp.ok) { c.loadPeers(await resp.text()); refreshCounts() }
      } catch (e) { console.warn('[daemon] seed load failed:', e) }
    }
    console.log('[daemon] warm: peers loaded, count=', status.peerCount, 'dialable=', status.dialable)

    // Launch the daemon: this runs one eager discovery round (so the first
    // fetch starts with fresh, browser-dialable peers — the live bootnodes
    // advertise /ws and hive gossip surfaces current AutoTLS ws peers, since
    // the committed seed goes stale) and then keeps the peer set warm on a
    // background loop inside the wasm node. `fetch` just talks to it.
    console.log('[daemon] starting daemon (eager discover + maintenance loop)')
    const count = await c.start(status.bootstrap, DAEMON_REFRESH_SECS, DISCOVER_WAIT_SECS)
    refreshCounts()
    console.log('[daemon] daemon up: peers=', count, 'dialable=', status.dialable)
    void idbSet(IDB_PEERS_KEY, c.exportPeers())

    status.ready = true
  } catch (e) {
    status.lastError = errMsg(e)
    console.error('[daemon] warm failed:', e)
  } finally {
    status.warming = false
    broadcastStatus()
  }

  // Poll the node periodically so the UI peer counts track the daemon's
  // background discovery, and persist the warm peer set for next launch.
  if (status.ready) startStatusPoll()
}

/** Periodically mirror the daemon's live peer counts into `status` (the
 *  background discovery loop runs inside wasm and doesn't call back), and
 *  persist the peer store so the next launch starts warm. */
function startStatusPoll (): void {
  if (statusTimer != null) return
  statusTimer = setInterval(() => {
    if (client == null) return
    const before = status.peerCount
    refreshCounts()
    if (status.peerCount !== before) broadcastStatus()
    try { void idbSet(IDB_PEERS_KEY, client.exportPeers()) } catch { /* best effort */ }
  }, DAEMON_REFRESH_SECS * 1000)
}

async function discover (bootstrap = DEFAULT_BOOTSTRAP, waitSecs = DISCOVER_WAIT_SECS): Promise<{ ok: boolean, error?: string }> {
  await ensureWarm()
  const c = client
  if (c == null) return { ok: false, error: status.lastError ?? 'client not ready' }
  status.warming = true
  status.bootstrap = bootstrap
  broadcastStatus()
  try {
    console.log('[daemon] discover (manual): start bootstrap=', bootstrap, 'waitSecs=', waitSecs)
    await c.discover(bootstrap, waitSecs)
    refreshCounts()
    console.log('[daemon] discover (manual): done count=', status.peerCount, 'dialable=', status.dialable)
    await idbSet(IDB_PEERS_KEY, c.exportPeers())
    status.lastError = undefined
    return { ok: true }
  } catch (e) {
    status.lastError = errMsg(e)
    return { ok: false, error: status.lastError }
  } finally {
    status.warming = false
    broadcastStatus()
  }
}

const MIME: Record<string, string> = {
  html: 'text/html', htm: 'text/html', css: 'text/css', js: 'text/javascript',
  mjs: 'text/javascript', json: 'application/json', svg: 'image/svg+xml',
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif',
  webp: 'image/webp', ico: 'image/x-icon', txt: 'text/plain', xml: 'application/xml',
  pdf: 'application/pdf', wasm: 'application/wasm', woff: 'font/woff', woff2: 'font/woff2',
  ttf: 'font/ttf', mp4: 'video/mp4', webm: 'video/webm', mp3: 'audio/mpeg', wav: 'audio/wav'
}
function guessType (path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  return MIME[ext] ?? 'application/octet-stream'
}

interface FetchResult { httpStatus: number, contentType?: string, body?: ArrayBuffer, error?: string }

async function handleFetchPath (refHex: string, rawPath: string): Promise<FetchResult> {
  console.log('[daemon] fetchPath:', refHex.slice(0, 12), 'path=', rawPath, '(await warm)')
  await ensureWarm()
  const c = client
  if (c == null) return { httpStatus: 503, error: status.lastError ?? 'daemon not ready' }

  const p = rawPath.replace(/^\/+/, '')
  const candidates = (p === '' || p.endsWith('/'))
    ? [p + 'index.html']
    : [p, p + '/index.html']

  let lastErr: string | undefined
  for (const candidate of candidates) {
    try {
      console.log('[daemon] fetchPath: calling client.fetchManifestPath', candidate)
      const r = await c.fetchManifestPath(refHex, candidate, FETCH_RETRIES)
      const bytes = r.bytes.slice()
      console.log('[daemon] fetchPath: got', candidate, bytes.length, 'bytes')
      return { httpStatus: 200, contentType: r.contentType ?? guessType(candidate), body: bytes.buffer }
    } catch (e) {
      console.log('[daemon] fetchPath:', candidate, 'failed:', errMsg(e))
      lastErr = errMsg(e)
    }
  }

  // Last resort: maybe `refHex` is a raw single file (not a manifest) and the
  // request is for the root.
  if (p === '') {
    try {
      const bytes = (await c.fetch(refHex, FETCH_RETRIES)).slice()
      return { httpStatus: 200, contentType: 'application/octet-stream', body: bytes.buffer }
    } catch (e) { lastErr = errMsg(e) }
  }

  return { httpStatus: 404, error: lastErr ?? 'not found' }
}

function errMsg (e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}

// ---------------- RPC ----------------
function serveRpc (port: MessagePort): void {
  ports.add(port)
  port.onmessage = async (e: MessageEvent) => {
    const msg = e.data
    if (msg?.type === ATTACH && e.ports[0] != null) {
      serveRpc(e.ports[0])
      return
    }
    try {
      switch (msg?.kind) {
        case 'status':
          void ensureWarm()
          port.postMessage({ kind: 'status', id: msg.id, status: { ...status } })
          break
        case 'discover': {
          const r = await discover(msg.bootstrap ?? undefined, msg.waitSecs ?? undefined)
          port.postMessage({ kind: 'discover', id: msg.id, ok: r.ok, status: { ...status }, error: r.error })
          break
        }
        case 'fetchPath': {
          const r = await handleFetchPath(msg.refHex, msg.path)
          port.postMessage(
            { kind: 'fetchPath', id: msg.id, ok: r.httpStatus < 400, httpStatus: r.httpStatus, contentType: r.contentType, body: r.body, error: r.error },
            r.body != null ? [r.body] : []
          )
          break
        }
        default:
          break
      }
    } catch (err) {
      port.postMessage({ kind: (msg?.kind ?? 'error'), id: msg?.id, ok: false, httpStatus: 500, error: errMsg(err) })
    }
  }
  port.start?.()
  // push current status immediately so a freshly-connected client can render
  port.postMessage({ kind: 'event', event: 'status', status: { ...status } })
}

self.onconnect = (e: MessageEvent) => {
  const port = (e as MessageEvent).ports[0]
  serveRpc(port)
  void ensureWarm() // begin warming as soon as anything connects
}

// Fallback for environments that surface SharedWorker as a module worker with
// a direct message channel rather than onconnect (defensive).
self.addEventListener('error', (e) => console.error('[daemon] error', e))
