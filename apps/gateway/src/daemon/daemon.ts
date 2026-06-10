/// <reference lib="webworker" />
//
// The shared in-browser hoverfly "daemon".
//
// Runs as a SharedWorker on the gateway ROOT origin (bzz.<host>). Because a
// SharedWorker is keyed by origin + script URL, every gateway tab and every
// content subdomain's broker iframe connects to the *same* instance — so there
// is exactly one long-lived hoverfly node, one warm peer set and one warm
// session/score cache for the whole gateway. That is the "daemon mode → better
// peer stability" the native CLI gets from a long-running process, recreated
// in the browser. (The native unix-socket daemon in `src/daemon.rs` can't run
// in a browser; this is the in-browser analogue.)
//
// It serves RPC (see ../shared/protocol.ts) on each connection port and on any
// port transferred via an `attach` control message.

import {
  DAEMON_REFRESH_SECS, DEFAULT_BOOTSTRAP, DISCOVER_WAIT_SECS, DOH_URL,
  FETCH_RETRIES, IDB_CHUNKS_DB, IDB_FEED_HINTS_KEY, IDB_NAME, IDB_NODEKEY_KEY,
  IDB_NONCE_KEY, IDB_PEERS_KEY, IDB_STORE, NETWORK_ID
} from '../shared/config.ts'
import { ATTACH, type DaemonStatus } from '../shared/protocol.ts'

declare const self: SharedWorkerGlobalScope & typeof globalThis

// --- hoverfly wasm glue (vendored, loaded at runtime so esbuild doesn't try to
//     bundle the wasm-bindgen module — it relies on import.meta.url). ---
interface ManifestFetch {
  readonly bytes: Uint8Array
  readonly contentType: string | undefined
  /** True iff the reference resolved through a feed manifest — i.e. the content
   *  is mutable (feed head moves forward), so the gateway must not cache it as
   *  immutable. Older wasm builds don't expose this getter; treated as false. */
  readonly feedResolved?: boolean
}
interface HoverflyClient {
  /** Launch the daemon: eager initial discovery + a background maintenance
   *  loop that re-discovers every `intervalSecs`. Resolves with the peer
   *  count after the initial round. Idempotent. */
  start: (bootstrap: string, intervalSecs: number, waitSecs: number) => Promise<number>
  /** Stop the background maintenance loop. */
  stop: () => void
  /** Enable the persistent IndexedDB chunk cache (L2). Retrieved chunks are
   *  written back and reused across fetches/sessions. */
  enableChunkStore: (dbName: string) => Promise<void>
  /** Chunks served from the L2 (IndexedDB) cache since load. */
  chunkStoreHits: () => number
  /** Manual one-shot discovery round (the daemon loop does this automatically). */
  discover: (bootstrap: string, waitSecs: number) => Promise<number>
  fetchManifestPath: (rootHex: string, path: string, maxRetries: number) => Promise<ManifestFetch>
  /** List every entry in the manifest as JSON: [{path, reference, contentType}]. */
  listManifest: (rootHex: string, maxRetries: number) => Promise<string>
  fetch: (rootHex: string, maxRetries: number) => Promise<Uint8Array>
  loadPeers: (json: string) => void
  exportPeers: () => string
  peerCount: () => number
  /** Export resolved feed head-index hints as JSON ({ "<owner||topic>": idx }). */
  exportFeedHints: () => string
  /** Merge persisted feed hints back in (monotonic). */
  loadFeedHints: (json: string) => void
}
interface HoverflyModule {
  default: (input?: any) => Promise<unknown>
  initThreadPool?: (n: number) => Promise<unknown>
  HoverflyClient: new (key?: string | null, networkId?: bigint | null, doh?: string | null, timeout?: number | null, nonceHex?: string | null) => HoverflyClient
}
// Resolved at runtime (relative to this worker script) so esbuild leaves the
// dynamic import alone — the wasm-bindgen module must load itself + its wasm
// via import.meta.url, so it must NOT be bundled.
const HOVERFLY_URL = new URL('hoverfly/hoverfly.js', self.location.href).href

const status: DaemonStatus = {
  ready: false,
  warming: false,
  peerCount: 0,
  dialable: 0,
  network: NETWORK_ID,
  bootstrap: DEFAULT_BOOTSTRAP
}

const ports = new Set<MessagePort>()
let client: HoverflyClient | null = null
let warmPromise: Promise<void> | null = null
let statusTimer: ReturnType<typeof setInterval> | null = null

// NB: every HoverflyClient method now takes `&self` (the node keeps its peers
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

// ---------------- node identity (persisted) ----------------
function randomHex32 (): string {
  const b = new Uint8Array(32)
  crypto.getRandomValues(b)
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')
}

/**
 * Load the persisted browser-daemon identity (secp256k1 node key + overlay
 * nonce) from IndexedDB, generating and storing one on first run. Persisting
 * BOTH values keeps the node's libp2p peer id AND its Swarm overlay stable
 * across reloads/sessions, so peers' kademlia memory of us survives instead of
 * us rejoining as a brand-new node on every page load. The pair lives in the
 * shared daemon's (root-origin) kv store, so all tabs/subdomains reuse it.
 */
async function loadOrCreateIdentity (): Promise<{ key: string, nonce: string }> {
  let key = await idbGet(IDB_NODEKEY_KEY)
  let nonce = await idbGet(IDB_NONCE_KEY)
  if (key == null || nonce == null) {
    key = randomHex32()
    nonce = randomHex32()
    await idbSet(IDB_NODEKEY_KEY, key)
    await idbSet(IDB_NONCE_KEY, nonce)
    console.log('[daemon] minted new persistent node identity')
  } else {
    console.log('[daemon] reusing persisted node identity')
  }
  return { key, nonce }
}

// ---------------- helpers ----------------
function broadcastStatus (): void {
  const msg = { kind: 'event', event: 'status', status: { ...status } }
  for (const p of ports) {
    try { p.postMessage(msg) } catch { /* dead port */ }
  }
}

/** Record + broadcast a coarse warm/runtime phase. Logs to the daemon console
 *  AND pushes it over the status channel so the SW/page can see daemon
 *  progress without opening the SharedWorker console. */
function setPhase (phase: string): void {
  status.phase = phase
  console.log('[daemon] phase:', phase)
  broadcastStatus()
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

/** Persist resolved feed head-index hints to IndexedDB (best effort, fire and
 *  forget). Cheap and idempotent — the cache exports the full hint map. */
function persistFeedHints (): void {
  const c = client
  if (c == null) return
  try {
    const json = c.exportFeedHints()
    if (json != null && json !== '{}') void idbSet(IDB_FEED_HINTS_KEY, json)
  } catch { /* best effort */ }
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
    setPhase('importing wasm (cOI=' + String((self as any).crossOriginIsolated) + ')')
    const mod = await import(/* @vite-ignore */ HOVERFLY_URL) as HoverflyModule
    setPhase('instantiating wasm')
    await mod.default() // instantiate wasm (shared memory; needs crossOriginIsolated)
    setPhase('wasm ready')
    if (typeof mod.initThreadPool === 'function') {
      setPhase('init thread pool')
      try { await mod.initThreadPool(self.navigator.hardwareConcurrency || 4) } catch (e) {
        // single-threaded fetch is fine; hashing parallelism is for uploads
        console.warn('[daemon] initThreadPool unavailable, continuing single-threaded:', e)
      }
    }
    // Reuse a persisted node identity (key + overlay nonce) so this browser
    // daemon keeps one stable Swarm overlay across reloads — see config.ts.
    setPhase('loading identity')
    const identity = await loadOrCreateIdentity()
    setPhase('constructing client')
    const c = new mod.HoverflyClient(identity.key, BigInt(NETWORK_ID), DOH_URL ?? undefined, 30, identity.nonce)
    client = c

    // Enable the persistent L2 chunk cache (IndexedDB) before any fetch. Best
    // effort: if storage is unavailable the daemon still works, just without
    // cross-session chunk persistence (the SW file cache still applies).
    setPhase('enabling chunk store')
    try {
      await c.enableChunkStore(IDB_CHUNKS_DB)
      console.log('[daemon] chunk store enabled:', IDB_CHUNKS_DB)
    } catch (e) {
      console.warn('[daemon] chunk store unavailable:', e)
    }

    setPhase('loading peers')
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

    // Restore persisted feed head-index hints so a returning visitor resolves a
    // feed (e.g. swarm.eth) in ~1 fast round from the cached head instead of a
    // cold gallop from index 0 (observed ~30s on a cold session vs ~1.5s warm).
    try {
      const hints = await idbGet(IDB_FEED_HINTS_KEY)
      if (hints != null) { c.loadFeedHints(hints); console.log('[daemon] feed hints restored') }
    } catch (e) { console.warn('[daemon] loadFeedHints failed:', e) }

    // Start the daemon: one eager discovery round + the maintenance loop. The
    // eager round dials peers on the shared swarm; on the browser's single
    // ws+yamux connection driver, a discovery round running CONCURRENTLY with a
    // retrieval resets the in-flight retrieval substream (observed as
    // `retrieval: unexpected end of file` / `ConnectionReset: Canceled`). So we
    // AWAIT the eager round here — restoring "discover, then fetch" ordering —
    // before marking ready and admitting fetches. The maintenance loop then
    // only re-discovers every DAEMON_REFRESH_SECS (≥45s), leaving long quiet
    // windows for retrieval. We bound the await with a timeout so a stalled
    // bootnode dial can't wedge warm forever; start() keeps running in the
    // background past the timeout (its loop is already spawned internally).
    setPhase('eager discovery (peers may collide with fetch if skipped)')
    const startPromise = c.start(status.bootstrap, DAEMON_REFRESH_SECS, DISCOVER_WAIT_SECS)
      .then((count) => {
        refreshCounts()
        console.log('[daemon] daemon up: peers=', count, 'dialable=', status.dialable)
        void idbSet(IDB_PEERS_KEY, c.exportPeers())
        broadcastStatus()
      })
      .catch((e) => { console.warn('[daemon] start() failed:', errMsg(e)) })
    try {
      await Promise.race([
        startPromise,
        new Promise<void>((resolve) => setTimeout(resolve, (DISCOVER_WAIT_SECS + 4) * 1000))
      ])
    } catch { /* timeout: proceed; start() continues in background */ }
    refreshCounts()

    status.ready = true
    setPhase('ready (' + String(status.dialable) + ' dialable peers)')
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

interface FetchResult { httpStatus: number, contentType?: string, body?: ArrayBuffer, error?: string, mutable?: boolean }

async function handleFetchPath (refHex: string, rawPath: string): Promise<FetchResult> {
  console.log('[daemon] fetchPath:', refHex.slice(0, 12), 'path=', rawPath, '(await warm)')
  // Bound the wait on warm so a wedged init (e.g. wasm shared-memory failure)
  // surfaces as a 503 to the SW instead of hanging the request forever.
  try {
    await Promise.race([
      ensureWarm(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('daemon warm timed out')), 30_000))
    ])
  } catch (e) {
    return { httpStatus: 503, error: status.lastError ?? errMsg(e) }
  }
  const c = client
  if (c == null) return { httpStatus: 503, error: status.lastError ?? 'daemon not ready' }

  const p = rawPath.replace(/^\/+/, '')
  // Build the manifest lookup candidates for the requested path:
  //   - ""  /  "dir/"        -> try the bare path FIRST, then "<p>index.html".
  //     The bare path matters for SINGLE-FILE references: `hoverfly upload`
  //     (and bee's single-file `POST /bzz`) produce a manifest whose root node
  //     carries a top-level entry — so resolving the empty path "" yields the
  //     file directly (e.g. deboot.eth points at a bare JSON file, no
  //     index.html exists). For a multi-file website the bare root has no entry
  //     and we fall through to "index.html" as before.
  //   - "dir/file.png"       -> the path itself ONLY. A last segment with a file
  //                             extension is a file, never a directory; appending
  //                             "/index.html" (e.g. "uploads/0626.png/index.html")
  //                             is always wrong.
  //   - "page" (extensionless, no slash) -> a "clean URL". Try, in order:
  //       1. "page"            exact (a genuine extensionless file)
  //       2. "page.html"       static-site-generator page (VitePress/Docusaurus/
  //                            Next export with cleanUrls=false emit "<path>.html")
  //       3. "page/index.html" directory index
  //     This mirrors how eth.limo's dweb proxy resolves clean URLs against an
  //     SSG export. (ethlimo/dweb-proxy-api). Trailing-slash handling is left
  //     as-is for now.
  const lastSeg = p.split('/').pop() ?? ''
  const hasExtension = /\.[^./]+$/.test(lastSeg)
  let candidates: string[]
  if (p === '' || p.endsWith('/')) {
    // Directory/root: the bare path resolves on its own when the manifest
    // carries a `website-index-document` (bee collection upload) — the wasm
    // manifest walker honours that root metadata and redirects to the index —
    // or for a single-file reference. `<dir>index.html` is the explicit
    // fallback for manifests without that metadata.
    candidates = [p, p + 'index.html']
  } else if (hasExtension) {
    candidates = [p]
  } else {
    candidates = [p, p + '.html', p + '/index.html']
  }

  let lastErr: string | undefined
  for (const candidate of candidates) {
    try {
      setPhase('fetching ' + (candidate || '(root)') + ' from ' + status.dialable + ' peers…')
      // Bound each manifest fetch: if a wss:// dial hangs without timing out
      // inside the wasm (the unverified browser /ws dial path), surface it as
      // a timeout error instead of leaving the SW request pending forever.
      const r = await withFetchTimeout(
        c.fetchManifestPath(refHex, candidate, FETCH_RETRIES),
        FETCH_TIMEOUT_MS,
        candidate
      )
      const bytes = r.bytes.slice()
      setPhase('got ' + candidate + ' (' + bytes.length + ' bytes, L2 hits ' + c.chunkStoreHits() + ')')
      // A feed-resolved fetch may have advanced a feed's cached head index;
      // persist hints so the next session resolves it fast (best effort, off
      // the hot path).
      if (r.feedResolved === true) persistFeedHints()
      return { httpStatus: 200, contentType: r.contentType ?? guessType(candidate), body: bytes.buffer, mutable: r.feedResolved === true }
    } catch (e) {
      setPhase('fetch ' + candidate + ' failed: ' + errMsg(e))
      lastErr = errMsg(e)
    }
  }

  // Single-file fallback for the root. `hoverfly upload <file>` and bee's
  // single-file `POST /bzz` build a manifest with ONE entry stored at the
  // file's basename (e.g. "deboot.json"), no index.html and no root entry — so
  // neither "" nor "index.html" resolves. When the root is requested, list the
  // manifest and, if it holds exactly one entry, serve that file. This is what
  // public Swarm gateways do for single-file references. Scoped to the root so
  // a genuine deep 404 on a multi-file site still surfaces as a 404.
  if (p === '') {
    try {
      const list = JSON.parse(await c.listManifest(refHex, FETCH_RETRIES)) as Array<{ path: string, contentType?: string }>
      const files = list.filter(e => e.path !== '' && e.path !== '/')
      if (files.length === 1) {
        const only = files[0]
        setPhase('single-file fallback: fetching ' + only.path)
        const r = await withFetchTimeout(
          c.fetchManifestPath(refHex, only.path, FETCH_RETRIES),
          FETCH_TIMEOUT_MS,
          only.path
        )
        const bytes = r.bytes.slice()
        setPhase('got ' + only.path + ' (' + bytes.length + ' bytes, single-file)')
        return { httpStatus: 200, contentType: r.contentType ?? only.contentType ?? guessType(only.path), body: bytes.buffer, mutable: r.feedResolved === true }
      }
    } catch (e) {
      lastErr = errMsg(e)
    }
  }

  // NB: no raw-chunk fallback here. Previously, when manifest resolution failed
  // for the root we fetched `refHex` as a raw chunk and returned it as
  // `application/octet-stream`. For a website reference that raw chunk is the
  // mantaray manifest node itself (binary), and serving octet-stream for a
  // top-level *document* navigation makes the browser DOWNLOAD it (a stray file
  // in ~/Downloads) instead of showing anything useful. A genuine single-file
  // reference is already served by `fetchManifestPath` above (it returns the
  // file bytes with a real Content-Type), so the only thing this fallback ever
  // produced for a website was a junk download. Return the error instead so the
  // SW renders a readable error page and we can see *why* resolution failed.
  return { httpStatus: 404, error: lastErr ?? 'not found' }
}

/** Per-candidate fetch ceiling. Must comfortably exceed the wasm transport's
 *  per-peer dial budget (20s) times a couple of sequential peer attempts, so a
 *  slow-but-working browser ws dial isn't cut off prematurely — while still
 *  bounding a wholly wedged fetch. */
const FETCH_TIMEOUT_MS = 90_000

function withFetchTimeout<T> (p: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('fetch timed out after ' + ms + 'ms for ' + label)), ms)
    p.then((v) => { clearTimeout(t); resolve(v) }, (e) => { clearTimeout(t); reject(e) })
  })
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
            { kind: 'fetchPath', id: msg.id, ok: r.httpStatus < 400, httpStatus: r.httpStatus, contentType: r.contentType, body: r.body, error: r.error, mutable: r.mutable === true },
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
