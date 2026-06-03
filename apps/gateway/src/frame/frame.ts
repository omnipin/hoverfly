// Broker frame — served from the gateway ROOT origin (bzz.<host>) and embedded
// as a hidden iframe by every content subdomain.
//
// A content origin (<cid>.bzz.<host>) cannot create the daemon SharedWorker
// directly (it's a different origin). This frame, being same-origin as the
// daemon, creates/joins the SharedWorker and relays MessagePorts from its
// embedder to the worker via `attach`. After that, the embedder talks to the
// daemon directly over the transferred port; this frame is out of the hot path.

import { ATTACH } from '../shared/protocol.ts'
import { DAEMON_WORKER_SCRIPT } from '../shared/config.ts'

// Surface any frame error to the embedder (its console is hard to capture).
const report = (tag: string, detail: unknown): void => {
  try { window.parent.postMessage({ type: 'frame-error', tag, detail: String(detail) }, '*') } catch { /* ignore */ }
}
window.addEventListener('error', (e) => report('error', e.message))
window.addEventListener('unhandledrejection', (e) => report('rejection', (e as PromiseRejectionEvent).reason))
console.log('[frame] loaded at', location.href, 'parent?', window.parent !== window)
report('loaded', location.href)

let worker: SharedWorker
try {
  worker = new SharedWorker(DAEMON_WORKER_SCRIPT, { type: 'module', name: 'isheika-daemon' })
  worker.port.start()
  worker.onerror = (e) => console.error('[frame] shared worker error', e)
  console.log('[frame] SharedWorker created')
} catch (e) {
  console.error('[frame] SharedWorker creation failed', e)
  throw e
}

function originAllowed (origin: string): boolean {
  if (origin === location.origin) return true
  try {
    // embedder must be a subdomain of this (root) origin: <label>.bzz.<host>
    return new URL(origin).host.endsWith('.' + location.host)
  } catch {
    return false
  }
}

window.addEventListener('message', (e: MessageEvent) => {
  if (!originAllowed(e.origin)) return
  const msg = e.data
  if (msg?.type === 'connect' && e.ports[0] != null) {
    console.log('[frame] got connect from', e.origin, '— attaching port to daemon')
    // Hand the embedder's port to the daemon; it becomes a direct RPC channel.
    worker.port.postMessage({ type: ATTACH }, [e.ports[0]])
    try { (e.source as Window | null)?.postMessage({ type: 'connected' }, e.origin) } catch { /* ignore */ }
  }
})

// Announce readiness to the embedder so it knows it can send `connect`. Post
// repeatedly for a short while in case the embedder's listener attaches late.
function announce (): void {
  if (window.parent === window) return
  try { window.parent.postMessage({ type: 'frame-ready' }, '*') } catch { /* ignore */ }
}
announce()
let n = 0
const iv = setInterval(() => { announce(); if (++n > 20) clearInterval(iv) }, 250)
window.addEventListener('message', (e) => { if (e.data?.type === 'connect') clearInterval(iv) })
