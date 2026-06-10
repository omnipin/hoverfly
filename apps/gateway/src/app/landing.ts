// Landing page — served at the gateway ROOT origin (bzz.<host>).
//
// Lets you enter a Swarm reference (hex), a swarm CID, or an ENS name and opens
// it on its own content subdomain. Connects to the shared daemon SharedWorker
// directly (same origin) to show live peer status and trigger discovery.

import { DAEMON_WORKER_SCRIPT, DEFAULT_BOOTSTRAP, DISCOVER_WAIT_SECS, IDB_CHUNKS_DB } from '../shared/config.ts'
import { DaemonRpc, type DaemonStatus } from '../shared/protocol.ts'
import { subdomainUrl } from '../shared/parse-request.ts'
import { normalizeRef } from '../shared/swarm-ref.ts'
import { referenceToCid } from '../shared/swarm-cid.ts'
import { looksLikeEnsName, resolveEnsToSwarm } from '../shared/ens.ts'

const app = document.getElementById('app') as HTMLElement

app.innerHTML = `
  <main class="wrap">
    <h1>Hoverfly <span class="accent">Swarm gateway</span></h1>
    <p class="lede">Fetches &amp; verifies Swarm websites entirely in your browser via a shared
      <a href="https://github.com/omnipin/hoverfly" target="_blank" rel="noopener">hoverfly</a> node
      running in daemon mode (one warm node, shared across every site).</p>

    <form id="open" class="open">
      <input id="ref" type="text" spellcheck="false" autocomplete="off"
        placeholder="Swarm reference (64-hex), CID (b…), or ENS name (name.eth)" />
      <button id="open-btn" type="submit">Open</button>
    </form>
    <p id="err" class="err" hidden></p>

    <section class="panel">
      <div class="row">
        <span class="dot" id="dot"></span>
        <strong>Daemon</strong>
        <span id="state" class="muted">connecting…</span>
      </div>
      <dl class="stats">
        <div><dt>Dialable peers</dt><dd id="peers">–</dd></div>
        <div><dt>Cached chunks</dt><dd id="chunks">–</dd></div>
        <div><dt>Network</dt><dd id="net">–</dd></div>
      </dl>
      <details class="adv">
        <summary>Peer discovery</summary>
        <p class="muted small">Browsers can only dial <code>/ws</code> / <code>/wss</code> peers (no raw TCP).
          Most mainnet bee nodes advertise TCP only, so you usually need a WebSocket-capable
          bootstrap to get dialable peers. Override below and re-discover.</p>
        <div class="discover">
          <input id="bootstrap" type="text" spellcheck="false" value="${DEFAULT_BOOTSTRAP}" />
          <button id="discover" type="button">Discover</button>
        </div>
      </details>
    </section>

    <p class="foot small muted">Tip: append <code>?gw-unregister</code> on a content origin to remove its service worker.</p>
  </main>`

const $ = <T extends HTMLElement>(id: string): T => document.getElementById(id) as T
const errEl = $<HTMLParagraphElement>('err')

$('open').addEventListener('submit', (e) => {
  e.preventDefault()
  void handleOpen()
})

async function handleOpen (): Promise<void> {
  errEl.hidden = true
  const raw = $<HTMLInputElement>('ref').value.trim()
  const btn = $<HTMLButtonElement>('open-btn')

  // Fast path: a 64-hex reference or a swarm CID needs no network round-trip.
  try {
    const { cid } = normalizeRef(raw)
    location.href = subdomainUrl(cid, location, '/')
    return
  } catch { /* not a raw ref/CID — try ENS below */ }

  // ENS path: <name>.eth (or other ENS name) -> resolve contenthash -> Swarm ref.
  if (looksLikeEnsName(raw)) {
    const prev = btn?.textContent
    if (btn != null) { btn.disabled = true; btn.textContent = 'Resolving ENS…' }
    try {
      const { refHex } = await resolveEnsToSwarm(raw)
      location.href = subdomainUrl(referenceToCid(refHex), location, '/')
      return
    } catch (err) {
      errEl.textContent = (err as Error).message
      errEl.hidden = false
    } finally {
      if (btn != null) { btn.disabled = false; btn.textContent = prev ?? 'Open' }
    }
    return
  }

  errEl.textContent = 'Enter a Swarm reference (64-hex), a swarm CID (b…), or an ENS name (name.eth).'
  errEl.hidden = false
}

// ---- daemon status ----
const worker = new SharedWorker(DAEMON_WORKER_SCRIPT, { type: 'module', name: 'hoverfly-daemon' })
const rpc = new DaemonRpc(worker.port)

function render (s: DaemonStatus): void {
  // Only browser-dialable (/ws, /wss) peers can actually be used from the
  // browser, so that's the single count worth surfacing — the raw PeerStore
  // total mostly counts TCP-only peers we can never connect to.
  $('peers').textContent = String(s.dialable)
  $('net').textContent = s.network === 1 ? 'mainnet' : s.network === 10 ? 'testnet' : String(s.network)
  const dot = $('dot')
  const state = $('state')
  if (s.lastError != null) {
    dot.style.background = '#f85149'; state.textContent = 'error: ' + s.lastError
  } else if (s.warming || !s.ready) {
    dot.style.background = '#d29922'; state.textContent = s.ready ? 'discovering…' : 'starting…'
  } else if (s.dialable > 0) {
    dot.style.background = '#3fb950'; state.textContent = 'ready'
  } else {
    dot.style.background = '#d29922'; state.textContent = 'ready — no dialable peers yet'
  }
}

rpc.onStatus(render)
void rpc.status().then((r) => render(r.status))

$('discover').addEventListener('click', () => {
  const bootstrap = $<HTMLInputElement>('bootstrap').value.trim() || DEFAULT_BOOTSTRAP
  const btn = $<HTMLButtonElement>('discover')
  btn.disabled = true; btn.textContent = 'Discovering…'
  void rpc.discover(bootstrap, DISCOVER_WAIT_SECS).then((r) => {
    render(r.status)
    btn.disabled = false; btn.textContent = 'Discover'
    if (!r.ok && r.error != null) { errEl.textContent = 'Discover failed: ' + r.error; errEl.hidden = false }
  })
})

// ---- cached-chunk counter (live) ----
//
// The daemon SharedWorker persists retrieved Swarm chunks in the
// `hoverfly-gw-chunks` IndexedDB (object store `chunks`, one record per chunk
// address). That DB lives on this ROOT origin — the same origin as this page —
// and every fetch (even those initiated from a content subdomain) routes
// through the root-origin daemon, so its chunk count reflects ALL cached
// content. We count the keys here in plain JS and poll so the number ticks up
// live as sites are fetched.
const CHUNK_STORE = 'chunks'
const chunksEl = $<HTMLElement>('chunks')

/** Open the chunk DB read-only WITHOUT bumping its version, so we never race
 *  the daemon's `open(name, 1)`. Returns null if it doesn't exist yet (no
 *  fetch has happened, so the daemon hasn't created it). */
function openChunkDb (): Promise<IDBDatabase | null> {
  return new Promise((resolve) => {
    let created = false
    const req = indexedDB.open(IDB_CHUNKS_DB)
    // If onupgradeneeded fires, the DB (or store) didn't exist. We MUST abort
    // the upgrade transaction here: a versionless `open()` of a non-existent DB
    // otherwise commits an empty version-1 database. The daemon opens with
    // `open(name, 1, …)` — the SAME version — so its store-creating
    // `onupgradeneeded` would then NEVER fire, leaving a permanent storeless DB
    // that shadows the daemon's and pins the counter at "–". Aborting rolls the
    // creation back so the daemon still gets to run its v1 upgrade.
    req.onupgradeneeded = (e) => {
      created = true
      try { (e.target as IDBOpenDBRequest).transaction?.abort() } catch { /* ignore */ }
    }
    // An aborted upgrade surfaces as onerror; treat it as "not ready", not a
    // real failure (and swallow the abort so it doesn't reject elsewhere).
    req.onerror = () => resolve(null)
    req.onsuccess = () => {
      const db = req.result
      if (created || !db.objectStoreNames.contains(CHUNK_STORE)) {
        db.close()
        resolve(null)
        return
      }
      resolve(db)
    }
    req.onblocked = () => resolve(null)
  })
}

async function countChunks (): Promise<number | null> {
  const db = await openChunkDb()
  if (db == null) return null
  try {
    return await new Promise<number>((resolve, reject) => {
      const r = db.transaction(CHUNK_STORE, 'readonly').objectStore(CHUNK_STORE).count()
      r.onsuccess = () => resolve(r.result)
      r.onerror = () => reject(r.error)
    })
  } catch {
    return null
  } finally {
    db.close()
  }
}

const numberFmt = new Intl.NumberFormat()
async function refreshChunkCount (): Promise<void> {
  const n = await countChunks()
  chunksEl.textContent = n == null ? '–' : numberFmt.format(n)
}

void refreshChunkCount()
setInterval(() => { void refreshChunkCount() }, 2000)
