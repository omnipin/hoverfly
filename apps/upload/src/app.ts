// Swarm upload dApp — main UI.
//
// Flow:
//   1. Connect wallet (MetaMask / EIP-1193) → ensures Gnosis (Swarm mainnet).
//   2. Pick a file → quote a postage batch (depth + cost) for it.
//   3. Buy the batch: approve BZZ + createBatch(owner = in-browser session key).
//      This is the ONLY wallet interaction (1–2 signatures).
//   4. Upload: the embedded hoverfly wasm stamps every chunk with the session
//      key (no prompts) and pushes them to the network, returning a bzz ref.
//
// See session-key.ts for why the session-key-as-batch-owner trick avoids
// thousands of per-chunk MetaMask signatures, and how it maps onto EIP-7702/
// 7579/4337 account abstraction if we want a revocable, user-owned variant.

import type { Hex } from 'viem'
import { PUBLIC_GATEWAY } from './config.ts'
import { deriveSessionKey, cachedSessionKey, rotateSessionKey, type SessionKey } from './session-key.ts'
import {
  connectWallet, eagerConnectWallet, quoteBatch, createBatch, formatBzz,
  type WalletConn, type BatchQuote, type CreatedBatch
} from './wallet.ts'
import { saveBatch, verifyBatches, importBatch, type VerifiedBatch } from './batches.ts'
import { startUploadSession, type UploadSession } from './hoverfly.ts'
import { classifyArchive, guessContentType, readTar, type CollectionEntry } from './tar.ts'

const TTL_OPTIONS: Array<[string, number]> = [
  ['1 day', 86400], ['1 week', 7 * 86400], ['1 month', 30 * 86400],
  ['6 months', 182 * 86400], ['1 year', 365 * 86400]
]

// ---- state ----
// Derived from a wallet signature on connect (see session-key.ts), so it's null
// until the wallet is connected.
let session: SessionKey | null = null
let conn: WalletConn | null = null
let file: File | null = null
let quote: BatchQuote | null = null
let batch: CreatedBatch | null = null
let upload: UploadSession | null = null

const app = document.getElementById('app') as HTMLElement
app.innerHTML = `
  <main class="wrap">
    <h1>Hoverfly <span class="accent">Swarm upload</span></h1>
    <p class="lede">Serverless, decentralized file upload to
      <a href="https://www.ethswarm.org/" target="_blank" rel="noopener">Ethereum Swarm</a>,
      powered by a <a href="https://github.com/omnipin/hoverfly" target="_blank" rel="noopener">hoverfly</a>
      node running in your browser.</p>

    <!-- 1. wallet -->
    <section class="step" id="s-wallet">
      <h2><span class="num">1</span> Connect wallet</h2>
      <div id="wallet-body">
        <button id="connect">Connect wallet</button>
        <p class="note">Swarm settles on <strong>Gnosis</strong>; you'll be asked to switch chains.
          You need <strong>xBZZ</strong> (for storage) and a little <strong>xDAI</strong> (for gas).</p>
      </div>
    </section>

    <!-- 2. file -->
    <section class="step" id="s-file" aria-disabled="true">
      <h2><span class="num">2</span> Choose a file</h2>
      <div class="drop" id="drop">
        <strong>Click to choose</strong> or drop a file here
        <div class="small" id="file-info"></div>
      </div>
      <input type="file" id="file" hidden />
    </section>

    <!-- 3. buy -->
    <section class="step" id="s-buy" aria-disabled="true">
      <h2><span class="num">3</span> Postage batch</h2>
      <p class="note">Storing on Swarm needs a postage batch — reuse one you already own, or buy a new one below.</p>
      <div id="reuse"></div>
      <!-- buy-new controls: hidden whenever an existing batch is selected -->
      <div id="buy-pane">
        <div class="grid2">
          <div>
            <label for="ttl">Keep for</label>
            <select id="ttl">${TTL_OPTIONS.map(([l, s], i) => `<option value="${s}"${i === 1 ? ' selected' : ''}>${l}</option>`).join('')}</select>
          </div>
          <div>
            <label for="immutable">Type</label>
            <select id="immutable">
              <option value="false" selected>Mutable (can top up / dilute)</option>
              <option value="true">Immutable</option>
            </select>
          </div>
        </div>
        <dl class="kv" id="quote"></dl>
        <div class="actions">
          <button id="buy">Buy batch</button>
        </div>
        <p class="err" id="buy-err" hidden></p>
      </div>
      <div class="actions" style="margin-top:0.5rem">
        <span class="pill"><span class="dot" id="batch-dot"></span><span id="batch-state">no batch yet</span></span>
      </div>
      <dl class="kv" id="batch-info"></dl>
      <details class="adv">
        <summary class="muted small">Import a batch by ID</summary>
        <p class="note">Batches owned by this session key (<span class="mono" id="import-owner"></span>) are
          auto-discovered via Swarmscan and verified on-chain. If an older batch isn't found, paste
          its ID here to import it manually (it must be owned by this session key to be usable).</p>
        <div class="actions">
          <input id="import-id" type="text" class="mono" spellcheck="false" placeholder="0x… (64 hex)" />
          <button class="ghost" id="import-btn" type="button">Import</button>
        </div>
        <p class="err" id="import-err" hidden></p>
      </details>
    </section>

    <!-- 4. upload -->
    <section class="step" id="s-upload" aria-disabled="true">
      <h2><span class="num">4</span> Upload</h2>
      <div class="actions">
        <button id="go">Upload to Swarm</button>
        <span class="pill"><span class="dot" id="net-dot"></span><span id="net-state">node idle</span></span>
      </div>
      <div class="bar"><span id="bar"></span></div>
      <div class="log" id="log" hidden></div>
      <div class="result" id="result"></div>
    </section>

    <details class="step">
      <summary class="muted small">Session key &amp; how this works</summary>
      <p class="note">A bee postage stamp signature is just <code>personal_sign</code> of a 32-byte
        digest, signed once <em>per chunk</em>. To avoid thousands of wallet popups, this dApp
        uses a separate <strong>session key</strong> as the batch <code>owner</code> in
        <code>createBatch</code>; it stamps every chunk locally. The key is <strong>derived from a
        one-time wallet signature</strong>, so the same wallet reproduces the same key (and owns the
        same batches) on any device — no funds at risk; its only power is stamping chunks for
        batches it owns.</p>
      <dl class="kv">
        <dt>Session key</dt><dd class="mono" id="sk-addr">—</dd>
      </dl>
      <div class="actions"><button class="ghost" id="rotate">Rotate session key</button></div>
    </details>
  </main>`

const $ = <T extends HTMLElement>(id: string): T => document.getElementById(id) as T
const enable = (id: string, on: boolean): void => { $(id).setAttribute('aria-disabled', String(!on)) }
const done = (id: string, on: boolean): void => { $(id).classList.toggle('done', on) }
/** Step 4 (Upload) needs BOTH a chosen file AND a ready batch. Either can be
 *  satisfied first (e.g. a batch auto-selects on connect before a file is
 *  picked), so gate on both rather than on whichever happened last. */
const refreshUploadGate = (): void => { enable('s-upload', file != null && batch != null) }
/** Show/hide the "buy a new batch" controls. Hidden when an existing batch is
 *  selected (reused/bought) — buying is then irrelevant. */
const showBuyPane = (on: boolean): void => { $('buy-pane').hidden = !on }
const logEl = $('log')
function log (m: string): void {
  logEl.hidden = false
  logEl.textContent += (logEl.textContent ? '\n' : '') + m
  logEl.scrollTop = logEl.scrollHeight
  console.log('[upload]', m)
}

// ---- 1. connect ----
/** Render the connected state and unlock the next steps. */
function wireConnection (c: WalletConn, sk: SessionKey): void {
  conn = c
  session = sk
  $('wallet-body').innerHTML = `
    <dl class="kv">
      <dt>Account</dt><dd class="mono">${c.account}</dd>
      <dt>Session key</dt><dd class="mono">${sk.address}</dd>
    </dl>`
  const skCell = document.getElementById('sk-addr'); if (skCell != null) skCell.textContent = sk.address
  done('s-wallet', true)
  enable('s-file', true)
  void refreshReuse()
}

$('connect').addEventListener('click', async () => {
  const btn = $<HTMLButtonElement>('connect')
  btn.disabled = true
  try {
    const c = await connectWallet()
    $('wallet-body').innerHTML = `
      <dl class="kv">
        <dt>Account</dt><dd class="mono">${c.account}</dd>
        <dt>Session key</dt><dd class="mono" id="wallet-sk">sign in your wallet to derive…</dd>
      </dl>`
    // Derive the stamping session key from a one-time wallet signature (cached
    // per wallet thereafter). Deterministic → same wallet owns the same batches
    // on any device.
    const sk = await deriveSessionKey(c.wallet, c.account)
    wireConnection(c, sk)
  } catch (e) {
    btn.disabled = false
    alert(errMsg(e))
  }
})

// Eager connect on mount: if the wallet has already authorized this site AND we
// have a cached session key for it, wire up silently — no prompts. (If the
// wallet is authorized but the key isn't cached, we leave the manual Connect
// button, which will derive it with one signature.)
void (async () => {
  const c = await eagerConnectWallet()
  if (c == null) return
  const sk = cachedSessionKey(c.account)
  if (sk == null) return // would need a signature; let the user click Connect
  wireConnection(c, sk)
})()

// ---- 2. file ----
const drop = $('drop')
const fileInput = $<HTMLInputElement>('file')
drop.addEventListener('click', () => fileInput.click())
;['dragover', 'dragenter'].forEach(ev => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add('hover') }))
;['dragleave', 'drop'].forEach(ev => drop.addEventListener(ev, () => drop.classList.remove('hover')))
drop.addEventListener('drop', (e) => {
  e.preventDefault()
  const f = (e as DragEvent).dataTransfer?.files?.[0]
  if (f != null) setFile(f)
})
fileInput.addEventListener('change', () => { if (fileInput.files?.[0] != null) setFile(fileInput.files[0]) })
$('ttl').addEventListener('change', () => void refreshQuote())
$('immutable').addEventListener('change', () => void refreshQuote())

function setFile (f: File): void {
  file = f
  $('file-info').innerHTML = `<strong>${escapeHtml(f.name)}</strong> · ${fmtBytes(f.size)} · ${f.type || 'application/octet-stream'}`
  done('s-file', true)
  enable('s-buy', true)
  refreshUploadGate() // a batch may already be selected; unlock upload now that a file exists
  void refreshQuote() // size + TTL → batch cost (shown in step 3)
}

/** Quote the batch cost for the chosen file + TTL. The quote lives in step 3
 *  (it's a postage-batch property), and re-runs when TTL changes. */
async function refreshQuote (): Promise<void> {
  if (conn == null || file == null) return
  const ttl = Number($<HTMLSelectElement>('ttl').value)
  try {
    quote = await quoteBatch(conn, file.size, ttl)
    $('quote').innerHTML = `
      <dt>Estimated cost</dt><dd>${formatBzz(quote.totalPlur)} xBZZ</dd>
      <dt>Your balance</dt><dd class="${quote.enoughBalance ? 'ok' : 'err'}">${formatBzz(quote.balancePlur)} xBZZ${quote.enoughBalance ? '' : ' — insufficient'}</dd>`
  } catch (e) {
    $('quote').innerHTML = `<dt class="err">Quote failed</dt><dd class="err">${escapeHtml(errMsg(e))}</dd>`
  }
}

// ---- reuse existing batch (on-chain verified) ----
let verified: VerifiedBatch[] = []

/** Verify this session key's saved batches against the chain and render a
 *  picker of the usable ones. Called after connect, buy, import, and rotate. */
async function refreshReuse (): Promise<void> {
  if (conn == null || session == null) { $('reuse').innerHTML = ''; return }
  const sk = session // non-null capture for the closures below
  const importOwner = $('import-owner'); if (importOwner != null) importOwner.textContent = sk.address
  $('reuse').innerHTML = '<p class="note">Checking for existing batches…</p>'
  try {
    verified = await verifyBatches(conn, sk.address)
  } catch {
    verified = []
  }
  if (verified.length === 0) {
    // Nothing to reuse → buy-new is the only path.
    $('reuse').innerHTML = ''
    showBuyPane(true)
    return
  }

  const opt = (b: VerifiedBatch): string => {
    const label = `${b.batchId.slice(0, 14)}… · depth ${b.onChain.depth}${b.usable ? '' : ' · expired'}`
    return `<option value="${b.batchId}"${b.usable ? '' : ' disabled'}>${label}</option>`
  }
  $('reuse').innerHTML = `
    <label for="existing">Use an existing batch</label>
    <select id="existing">
      ${verified.map(opt).join('')}
      <option value="">— buy a new batch instead —</option>
    </select>`

  const selectBatch = (v: VerifiedBatch): void => {
    batch = { batchId: v.batchId, depth: v.onChain.depth, owner: sk.address, createTx: '0x' as Hex }
    setBatchState('batch ready', true)
    $('batch-info').innerHTML = `
      <dt>Batch ID</dt><dd class="mono">${v.batchId}</dd>
      <dt>Owner</dt><dd class="mono">${sk.address} <span class="muted">(session key)</span></dd>`
    showBuyPane(false)
    done('s-buy', true)
    refreshUploadGate()
  }

  $('existing').addEventListener('change', (e) => {
    const sel = e.target as HTMLSelectElement
    if (sel.value === '') {
      // "buy a new batch instead" → reveal buy controls, clear any selection.
      batch = null
      $('batch-info').innerHTML = ''
      setBatchState('no batch yet', false)
      showBuyPane(true)
      refreshUploadGate()
      return
    }
    const v = verified.find(b => b.batchId.toLowerCase() === sel.value.toLowerCase())
    if (v != null && v.usable) selectBatch(v)
  })

  // Default to reusing the first usable batch (hides the buy UI). If none are
  // usable (all expired), fall back to buy-new.
  const firstUsable = verified.find(b => b.usable)
  if (firstUsable != null) {
    ;($('existing') as HTMLSelectElement).value = firstUsable.batchId
    selectBatch(firstUsable)
  } else {
    showBuyPane(true)
  }
}

// import-by-id
$('import-btn').addEventListener('click', async () => {
  if (conn == null || session == null) return
  const input = $<HTMLInputElement>('import-id')
  const err = $('import-err'); err.hidden = true
  const btn = $<HTMLButtonElement>('import-btn')
  btn.disabled = true
  try {
    const v = await importBatch(conn, session.address, input.value.trim() as Hex)
    input.value = ''
    await refreshReuse()
    // auto-select the freshly imported batch
    const sel = document.getElementById('existing') as HTMLSelectElement | null
    if (sel != null) { sel.value = v.batchId; sel.dispatchEvent(new Event('change')) }
  } catch (e) {
    err.textContent = errMsg(e); err.hidden = false
  } finally {
    btn.disabled = false
  }
})

// ---- 3. buy ----
function setBatchState (msg: string, ok: boolean): void {
  $('batch-state').textContent = msg
  $('batch-dot').className = 'dot' + (ok ? ' ok' : '')
}
$('buy').addEventListener('click', async () => {
  if (conn == null || quote == null || session == null) return
  const btn = $<HTMLButtonElement>('buy')
  const err = $('buy-err'); err.hidden = true
  btn.disabled = true
  try {
    const immutable = $<HTMLSelectElement>('immutable').value === 'true'
    setBatchState('buying…', false)
    batch = await createBatch(conn, session.address, quote, immutable, (m) => setBatchState(m, false))
    saveBatch({ batchId: batch.batchId, depth: batch.depth, owner: batch.owner, createdAt: Date.now(), sizeBytes: file?.size })
    $('batch-info').innerHTML = `
      <dt>Batch ID</dt><dd class="mono">${batch.batchId}</dd>
      <dt>Owner</dt><dd class="mono">${batch.owner} <span class="muted">(session key)</span></dd>
      ${batch.approveTx ? `<dt>Approve tx</dt><dd class="mono">${batch.approveTx}</dd>` : ''}
      <dt>Create tx</dt><dd class="mono">${batch.createTx}</dd>`
    setBatchState('batch ready', true)
    // The new batch now exists → fold the buy UI away and let it appear in the
    // reuse list (selected), so "have a batch" looks the same however you got it.
    showBuyPane(false)
    done('s-buy', true)
    refreshUploadGate()
  } catch (e) {
    err.textContent = errMsg(e); err.hidden = false
    setBatchState('failed', false)
  } finally {
    btn.disabled = false
  }
})

// ---- 4. upload ----
$('go').addEventListener('click', async () => {
  if (file == null || batch == null || session == null) return
  const btn = $<HTMLButtonElement>('go')
  btn.disabled = true
  $('result').innerHTML = ''
  const bar = $('bar')
  bar.style.width = '8%'
  try {
    setNet('starting node…', 'warn')
    if (upload == null) {
      upload = await startUploadSession(session.bareKeyHex, log, (n) => {
        setNet(`${n} peers connected`, n > 0 ? 'ok' : 'warn')
      })
    }
    const connected = await upload.connected()
    setNet(`${connected} peers connected`, connected > 0 ? 'ok' : 'warn')
    bar.style.width = '30%'

    log(`Reading ${file.name} (${fmtBytes(file.size)})…`)
    const bytes = new Uint8Array(await file.arrayBuffer())
    bar.style.width = '45%'

    const kind = classifyArchive(file.name, file.type)
    setNet('uploading…', 'warn')
    let ref: string
    let suffix = '' // appended to the gateway URL (a single file resolves at /<name>)

    if (kind === 'tar' || kind === 'tgz') {
      // Tar archive → Swarm collection (multi-entry manifest). If it has an
      // index.html (or a single .../index.html), pass it as the website index
      // so the bzz root serves a browsable site.
      const entries = await readTar(bytes, kind === 'tgz')
      const indexDoc = pickIndexDocument(entries)
      log(`Collection: ${entries.length} files${indexDoc != null ? `, index = ${indexDoc}` : ''}`)
      log(`Stamping + pushing chunks (batch ${batch.batchId.slice(0, 12)}…, depth ${batch.depth})…`)
      ref = await upload.uploadCollection(entries, indexDoc, undefined, batch.batchId, batch.depth)
    } else {
      // Single file → one-entry manifest with filename + content-type, so the
      // gateway serves it with a sensible Content-Type.
      const contentType = file.type || guessContentType(file.name)
      log(`Single file: ${file.name} (${contentType ?? 'application/octet-stream'})`)
      log(`Stamping + pushing chunks (batch ${batch.batchId.slice(0, 12)}…, depth ${batch.depth})…`)
      ref = await upload.uploadFile(bytes, file.name, contentType, batch.batchId, batch.depth)
      suffix = encodeURIComponent(file.name)
    }

    bar.style.width = '100%'
    setNet('done', 'ok')
    log('Upload complete: ' + ref)

    const url = PUBLIC_GATEWAY + ref + '/' + suffix
    $('result').innerHTML = `
      <dl class="kv" style="margin-top:1rem">
        <dt>Swarm reference</dt><dd class="mono ok">${ref}</dd>
        <dt>Open</dt><dd><a href="${url}" target="_blank" rel="noopener">${escapeHtml(url)}</a></dd>
      </dl>`
    done('s-upload', true)
  } catch (e) {
    setNet('failed', 'warn')
    log('ERROR: ' + errMsg(e))
    $('result').innerHTML = `<p class="err">${escapeHtml(errMsg(e))}</p>`
  } finally {
    btn.disabled = false
  }
})
function setNet (msg: string, kind: 'ok' | 'warn' | ''): void {
  $('net-state').textContent = msg
  $('net-dot').className = 'dot' + (kind ? ' ' + kind : '')
}

// ---- session key rotate ----
$('rotate').addEventListener('click', () => {
  if (conn == null) { alert('Connect a wallet first.'); return }
  if (!confirm('Rotate to a fresh RANDOM session key for this wallet? It overrides the wallet-derived key, so it won\'t reproduce on other devices, and existing batches owned by the current key can no longer receive new uploads.')) return
  session = rotateSessionKey(conn.account)
  $('sk-addr').textContent = session.address
  batch = null; upload = null
  setBatchState('no batch yet', false)
  refreshUploadGate()
  void refreshReuse()
})

// ---- utils ----
/** Pick a website index document for a collection: a top-level `index.html`,
 *  else a sole `index.html` nested one level down, else none. */
function pickIndexDocument (entries: CollectionEntry[]): string | undefined {
  if (entries.some(e => e.path === 'index.html')) return 'index.html'
  const indexes = entries.filter(e => e.path.endsWith('/index.html') || e.path === 'index.html')
  if (indexes.length === 1) return indexes[0].path
  return undefined
}

function errMsg (e: unknown): string {
  if (e instanceof Error) return e.message
  if (typeof e === 'object' && e != null && 'shortMessage' in e) return String((e as { shortMessage: unknown }).shortMessage)
  return String(e)
}
function fmtBytes (n: number): string {
  if (n < 1024) return n + ' B'
  const u = ['KB', 'MB', 'GB']
  let i = -1
  do { n /= 1024; i++ } while (n >= 1024 && i < u.length - 1)
  return n.toFixed(n < 10 ? 1 : 0) + ' ' + u[i]
}
function escapeHtml (s: string): string {
  return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c] as string))
}
