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
  PUSH_BATCH_SIZE, PUSHER_URLS, STATUS_POLL_SECS, UPLOAD_RETRIES, WARM_POOL, usePushers
} from './config.ts'
import type { Req, Res } from './worker-protocol.ts'

declare const self: DedicatedWorkerGlobalScope

interface HoverflyClient {
  start: (bootstrap: string, intervalSecs: number, waitSecs: number, warmPool?: number, skipPrewarm?: boolean) => Promise<number>
  loadPeers: (json: string) => void
  mergePeers: (json: string) => number
  exportPeers: () => string
  peerCount: () => number
  connectedPeerCount?: () => Promise<number>
  uploadProgress?: () => number[]
  uploadDiagnostics?: () => string
  uploadFile: (data: Uint8Array, path: string, contentType: string | undefined, batchIdHex: string, depth: number, immutable: boolean, maxRetries: number) => Promise<string>
  uploadCollection: (files: Array<{ path: string, data: Uint8Array, contentType?: string }>, indexDocument: string | undefined, errorDocument: string | undefined, batchIdHex: string, depth: number, immutable: boolean, maxRetries: number) => Promise<string>
  // Pusher path: stamp locally, return POST-ready frame batches (no network).
  prepareUpload?: (data: Uint8Array, path: string, contentType: string | undefined, batchIdHex: string, depth: number, immutable: boolean, raw: boolean, batchSize: number) => PreparedUpload
  prepareCollection?: (files: Array<{ path: string, data: Uint8Array, contentType?: string }>, indexDocument: string | undefined, errorDocument: string | undefined, batchIdHex: string, depth: number, immutable: boolean, batchSize: number) => PreparedUpload
}
/** Locally-stamped upload ready for the pusher relay path (wasm PreparedUpload). */
interface PreparedUpload {
  readonly root: string
  readonly chunkCount: number
  readonly batchCount: number
  batch: (i: number) => Uint8Array | undefined
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

    // Pusher mode: no in-browser p2p at all. The wasm client exists only to
    // stamp chunks locally (BMT + EIP-191); the relays do the actual pushing
    // over TCP. Skip the whole discover/warm/seed path — the wss-sliver
    // problem it fights simply doesn't apply when we never dial a bee.
    if (usePushers()) {
      log(`Pusher mode: ${PUSHER_URLS.length} relay(s), no in-browser p2p (browser only stamps).`)
      post({ kind: 'status', connected: PUSHER_URLS.length })
      return
    }

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
    // skipPrewarm=true: this dApp only uploads. The retrieval warm pool `start`
    // would otherwise open is never used by the pushsync upload path, and
    // warming it just doubled cold-start dialing (retrieval sessions + the
    // upload's own pushsync pool), making bring-up far slower than native for
    // no benefit. Discover peers, skip the retrieval warm-up.
    const n = await c.start(DEFAULT_BOOTSTRAP, MAINTENANCE_SECS, DISCOVER_WAIT_SECS, WARM_POOL, true)
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

/**
 * Run an upload while polling the wasm client's `uploadProgress()` and posting
 * `progress` events, so the UI can render a real per-chunk bar. Emits a final
 * `done === total` frame on completion so the bar reaches 100%. The poll timer
 * is always cleared, even if the upload throws.
 */
async function withProgress<T> (c: HoverflyClient, run: () => Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setInterval> | null = null
  if (c.uploadProgress != null) {
    const poll = (): void => {
      try {
        const [done, total] = c.uploadProgress!()
        if (total > 0) post({ kind: 'progress', done, total })
      } catch { /* ignore */ }
    }
    timer = setInterval(poll, 200)
  }
  try {
    return await run()
  } finally {
    if (timer != null) clearInterval(timer)
    // Dump the transport diagnostic counters so browser throughput can be
    // debugged from real data (push RTT vs open-stream vs retirement churn).
    try {
      const diag = c.uploadDiagnostics?.()
      if (diag != null && diag.length > 0) log(`diag: ${diag}`)
    } catch { /* ignore */ }
    // Final snapshot so the bar snaps to 100% rather than stopping at the last
    // poll (which may lag a few hundred chunks behind completion).
    try {
      const [done, total] = c.uploadProgress?.() ?? [0, 0]
      if (total > 0) post({ kind: 'progress', done: Math.max(done, total), total })
    } catch { /* ignore */ }
  }
}

// ---- pusher relay path (stamp local, POST frames) ----

/** POST one frame batch to a relay's /v1/push and stream the NDJSON acks,
 *  invoking `onAck()` per chunk acked "ok" (for live progress). Returns the
 *  total "ok" count. Any HTTP/transport error → 0 (batch treated unacked). */
async function postBatch (pushUrl: string, body: Uint8Array, onAck: () => void): Promise<number> {
  try {
    const resp = await fetch(pushUrl, {
      method: 'POST',
      body: body as BodyInit,
      headers: { 'content-type': 'application/x-hoverfly-frames' }
    })
    if (!resp.ok) {
      const t = (await resp.text().catch(() => '')).slice(0, 300)
      log(`Pusher ${pushUrl} → HTTP ${resp.status}: ${t}`)
      return 0
    }
    let ok = 0
    let sampleErr: string | undefined
    const handle = (line: string): void => {
      if (line.length === 0) return
      try {
        const v = JSON.parse(line) as { s?: string, e?: string }
        if (v.s === 'ok') { ok++; onAck() } else if (v.s === 'err' && sampleErr == null) sampleErr = v.e
      } catch { /* skip non-JSON */ }
    }
    const reader = resp.body?.getReader()
    if (reader != null) {
      // Stream the acks so progress advances per chunk, not per batch.
      const dec = new TextDecoder()
      let buf = ''
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        let nl: number
        while ((nl = buf.indexOf('\n')) >= 0) { handle(buf.slice(0, nl)); buf = buf.slice(nl + 1) }
      }
      handle(buf)
    } else {
      for (const line of (await resp.text()).split('\n')) handle(line)
    }
    if (ok === 0 && sampleErr != null) log(`Pusher ${pushUrl} rejected: ${sampleErr}`)
    return ok
  } catch (e) {
    log(`Pusher ${pushUrl} fetch failed: ${e instanceof Error ? e.message : String(e)}`)
    return 0
  }
}

/**
 * Push an already-stamped upload (frames) across the configured relays.
 * Batches are distributed across lanes and pushed concurrently; a batch a
 * lane fails to fully ack is re-tried on the next lane (rank+1). The server
 * is all-or-nothing per POST, so a batch is either fully acked or retried
 * whole. Returns the reference root.
 */
async function pushPrepared (prep: PreparedUpload): Promise<string> {
  const total = prep.chunkCount
  const nBatches = prep.batchCount
  // Copy each batch out of wasm linear memory before any await (the view
  // would otherwise dangle / detach across the fetch).
  const bodies: Uint8Array[] = []
  for (let i = 0; i < nBatches; i++) {
    const b = prep.batch(i)
    if (b != null) bodies.push(b.slice())
  }
  const lanes = PUSHER_URLS.map(u => `${u.replace(/\/+$/, '')}/v1/push`)
  const batchChunks = (i: number): number =>
    i < nBatches - 1 ? PUSH_BATCH_SIZE : total - PUSH_BATCH_SIZE * (nBatches - 1)
  log(`Stamped ${total} chunks → ${nBatches} batch(es); pushing across ${lanes.length} lane(s)…`)

  // Live progress from streamed acks. `done` drives only the bar (clamped to
  // total); completion is gated on confirmed batches below, so an over-count
  // from a mid-stream reconnect is cosmetic.
  let done = 0
  let lastPost = 0
  const onAck = (): void => {
    done++
    const now = Date.now()
    if (now - lastPost >= 150 || done >= total) {
      lastPost = now
      post({ kind: 'progress', done: Math.min(done, total), total })
    }
  }

  let pending = bodies.map((_, i) => ({ i, rank: 0 }))
  const MAX_ROUNDS = 6
  for (let round = 0; round < MAX_ROUNDS && pending.length > 0; round++) {
    const results = await Promise.all(pending.map(async ({ i, rank }) => {
      const lane = lanes[(i + rank) % lanes.length]
      const acked = await postBatch(lane, bodies[i], onAck)
      return { i, rank, ok: acked === batchChunks(i) }
    }))
    const next: Array<{ i: number, rank: number }> = []
    for (const r of results) if (!r.ok) next.push({ i: r.i, rank: r.rank + 1 })
    if (next.length > 0) log(`Pusher: ${next.length} batch(es) unacked, failing over to next lane…`)
    pending = next
  }
  if (pending.length > 0) {
    throw new Error(`${pending.length} batch(es) unacked after ${MAX_ROUNDS} rounds across ${lanes.length} lane(s)`)
  }
  post({ kind: 'progress', done: total, total })
  return prep.root
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
        const c = requireClient()
        let root: string
        if (usePushers()) {
          if (c.prepareUpload == null) throw new Error('wasm build lacks prepareUpload (rebuild)')
          root = await pushPrepared(c.prepareUpload(
            new Uint8Array(msg.data), msg.path, msg.contentType, msg.batchIdHex, msg.depth, msg.immutable, false, PUSH_BATCH_SIZE
          ))
        } else {
          root = await withProgress(c, async () => await c.uploadFile(
            new Uint8Array(msg.data), msg.path, msg.contentType, msg.batchIdHex, msg.depth, msg.immutable, UPLOAD_RETRIES
          ))
          await pushStatus()
        }
        post({ kind: 'result', id: msg.id, ok: true, value: root })
        break
      }
      case 'uploadCollection': {
        const c = requireClient()
        const files = msg.files.map(f => ({ path: f.path, data: new Uint8Array(f.data), contentType: f.contentType }))
        let root: string
        if (usePushers()) {
          if (c.prepareCollection == null) throw new Error('wasm build lacks prepareCollection (rebuild)')
          root = await pushPrepared(c.prepareCollection(
            files, msg.indexDocument, msg.errorDocument, msg.batchIdHex, msg.depth, msg.immutable, PUSH_BATCH_SIZE
          ))
        } else {
          root = await withProgress(c, async () => await c.uploadCollection(
            files, msg.indexDocument, msg.errorDocument, msg.batchIdHex, msg.depth, msg.immutable, UPLOAD_RETRIES
          ))
          await pushStatus()
        }
        post({ kind: 'result', id: msg.id, ok: true, value: root })
        break
      }
    }
  } catch (err) {
    post({ kind: 'result', id: (msg as { id: number }).id, ok: false, error: err instanceof Error ? err.message : String(err) })
  }
}
