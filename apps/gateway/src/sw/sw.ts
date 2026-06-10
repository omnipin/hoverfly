/// <reference lib="webworker" />
//
// Service worker for a content origin (<cid>.bzz.<host>).
//
// It does NOT run hoverfly itself (the node lives in the shared cross-origin
// daemon). Instead a controlling document hands it a MessagePort to the daemon
// (`daemon-port`), and the SW serves Swarm content by RPC over it.
//
// Routing:
//   - /__gw__/*                      -> passthrough (the gateway's own assets)
//   - top-level document navigation  -> passthrough (network returns the boot
//                                       shell; the shell renders the site in a
//                                       child iframe)
//   - iframe navigation + subresources on this origin -> served as Swarm
//     content via the daemon bridge.

import { ASSET_PREFIX, CONTENT_CACHE, CONTENT_MARKER } from '../shared/config.ts'
import { DaemonRpc } from '../shared/protocol.ts'
import { parseHost } from '../shared/parse-request.ts'
import { cidToReference } from '../shared/swarm-cid.ts'

declare const self: ServiceWorkerGlobalScope

let daemon: DaemonRpc | null = null
let resolveDaemon: ((r: DaemonRpc) => void) | null = null
const daemonReady = new Promise<DaemonRpc>((resolve) => { resolveDaemon = resolve })

self.addEventListener('install', () => { console.log('[sw] install'); void self.skipWaiting() })
self.addEventListener('activate', (event) => { console.log('[sw] activate + claim'); event.waitUntil(self.clients.claim()) })

self.addEventListener('message', (event: ExtendableMessageEvent) => {
  const msg = event.data
  if (msg?.type === 'daemon-port' && event.ports[0] != null) {
    console.log('[sw] received daemon-port')
    daemon = new DaemonRpc(event.ports[0])
    // Surface the daemon's warm/runtime phase + peer counts here, since the
    // daemon's own console (SharedWorker context) is awkward to open.
    daemon.onStatus((s) => {
      console.log('[sw] daemon status: phase=', s.phase, '| ready=', s.ready, '| warming=', s.warming,
        '| dialable=', s.dialable, '| peers=', s.peerCount, s.lastError != null ? '| ERROR=' + s.lastError : '')
    })
    resolveDaemon?.(daemon)
    resolveDaemon = null
  }
})

self.addEventListener('fetch', (event: FetchEvent) => {
  const req = event.request
  const url = new URL(req.url)

  if (url.origin !== self.location.origin) return // not ours
  if (url.pathname.startsWith(ASSET_PREFIX)) return // gateway's own assets

  // A document navigation is EITHER the top boot shell (passthrough → network
  // returns boot.html) OR a document inside the content iframe the shell hosts.
  // Both have mode 'navigate' + destination 'document', so we need another
  // signal to tell them apart:
  //   1. CONTENT_MARKER query — set by the boot shell on the INITIAL iframe load
  //      (boot.ts contentUrl), the one reliable cross-browser discriminator.
  //   2. `Sec-Fetch-Dest: iframe` — set by the browser for a NESTED document
  //      navigation (in-iframe link clicks / full-page nav within the site),
  //      which the marker wouldn't cover. Chrome 80+/Firefox 90+/Safari 16.4+.
  // Either signal => serve Swarm content; neither => it's the top shell, pass
  // through. (Without this the iframe nav would re-fetch boot.html, the inner
  // boot.js would see window.top !== window and bail, and the page would stay
  // blank with no chunk fetches — the observed failure.)
  if (req.mode === 'navigate' && req.destination === 'document') {
    const isContent = url.searchParams.has(CONTENT_MARKER) ||
      req.headers.get('sec-fetch-dest') === 'iframe'
    if (!isContent) {
      console.log('[sw] passthrough top-level document nav', url.pathname)
      return // boot shell
    }
    console.log('[sw] serve content iframe document', url.pathname)
    event.respondWith(serveContent(req))
    return
  }

  console.log('[sw] serve content:', url.pathname, 'dest=', req.destination, 'mode=', req.mode)
  event.respondWith(serveContent(req))
})

async function serveContent (req: Request): Promise<Response> {
  const url = new URL(req.url)

  // The browser auto-probes /favicon.ico for the top-level document. The
  // gateway shell ships its own icon (see boot.html), and a content site's
  // favicon would never be shown anyway — it renders inside an iframe, so the
  // tab icon is always the shell's. Answer the probe with an empty 204 instead
  // of doing a daemon round-trip and emitting a (harmless but noisy) manifest
  // 404 for a file the site simply doesn't ship.
  if (url.pathname === '/favicon.ico') {
    const headers = new Headers({ 'cache-control': 'no-cache', server: 'hoverfly-gateway' })
    isolation(headers)
    return new Response(null, { status: 204, headers })
  }

  const host = parseHost(url.host)
  if (host.kind !== 'subdomain' || host.id == null) {
    return errorPage(404, 'Not a Swarm content subdomain', url.host)
  }

  let refHex: string
  try {
    refHex = cidToReference(host.id).refHex
  } catch (e) {
    return errorPage(400, 'Invalid Swarm CID label', (e as Error).message)
  }

  const path = safeDecode(url.pathname)

  const cache = await caches.open(CONTENT_CACHE)
  const cacheKey = new Request(`${url.origin}${url.pathname}`)
  // A cache hit is ALWAYS safe to serve: we only ever store immutable,
  // content-addressed responses below (feed-backed/mutable responses are never
  // written to the cache). So a present entry can't be a stale feed head.
  const cached = await cache.match(cacheKey)
  if (cached != null) return cached

  let rpc: DaemonRpc
  try {
    rpc = await withTimeout(daemonReady, 25_000)
  } catch {
    console.warn('[sw] daemon bridge not connected for', path)
    return errorPage(504, 'Daemon bridge not connected', 'The gateway shell did not provide a daemon channel in time.')
  }

  console.log('[sw] fetchPath', refHex.slice(0, 12) + '…', 'path=', path || '(root)')
  let res
  try {
    // Bound the RPC so a lost/dropped daemon message can't hang the document
    // request forever (which would leave the boot overlay stuck with no error).
    // The daemon bounds each candidate at 90s and tries up to 3, so allow
    // headroom over that worst case (3×90s) before declaring the bridge dead.
    res = await withTimeout(rpc.fetchPath(refHex, path), 300_000)
  } catch (e) {
    console.error('[sw] fetchPath RPC error', e)
    return errorPage(504, 'Swarm fetch timed out', (e as Error).message)
  }

  console.log('[sw] fetchPath result ' + JSON.stringify({ path, ok: res.ok, httpStatus: res.httpStatus, bytes: res.body?.byteLength, contentType: res.contentType, error: res.error }))
  if (!res.ok || res.body == null) {
    return errorPage(res.httpStatus >= 400 ? res.httpStatus : 502, 'Could not retrieve from Swarm', res.error ?? 'unknown error')
  }

  const headers = new Headers()
  headers.set('content-type', res.contentType ?? 'application/octet-stream')
  // Mutability depends on whether the reference resolved through a FEED
  // manifest. A feed's reference (e.g. an ENS contenthash like swarm.eth) is
  // stable, but the content it points at moves forward with each feed update —
  // so it must NOT be cached as immutable, or a visitor would be pinned to the
  // first feed head they ever fetched and never see updates (and mixed-version
  // splits — cached HTML vs freshly-resolved assets from a newer head — surface
  // as broken/unstyled pages). A bare content-addressed CID, by contrast, is
  // fixed forever (new site = new CID = new origin).
  if (res.mutable === true) {
    // Revalidate every load; allow brief reuse but force a freshness check.
    headers.set('cache-control', 'no-cache, max-age=0, must-revalidate')
  } else {
    headers.set('cache-control', 'public, max-age=31536000, immutable')
  }
  headers.set('x-swarm-reference', refHex)
  if (res.mutable === true) headers.set('x-swarm-mutable', '1')
  headers.set('server', 'hoverfly-gateway')
  // The boot shell is crossOriginIsolated (COEP: require-corp) so the nested
  // daemon broker iframe → SharedWorker can use shared wasm memory. Per the
  // HTML spec's embedder-policy compatibility check, a nested frame document
  // is only allowed to load into a require-corp embedder if its OWN COEP is
  // compatible — a same-origin document served with no COEP (unsafe-none) is
  // refused (shown as a broken "couldn't display" frame). So the served site
  // must carry require-corp too; all its subresources are same-origin (served
  // by this same SW) so they satisfy COEP, and CORP lets them be embedded.
  isolation(headers)
  const response = new Response(res.body, { status: 200, headers })

  // Only persist immutable, content-addressed responses. Feed-backed (mutable)
  // content is intentionally NOT cached: the daemon re-resolves the feed head
  // on every fetch, so caching it would defeat updates and risk serving a stale
  // head from the Cache API forever (the bug this guards against).
  if (res.mutable !== true) {
    void cache.put(cacheKey, response.clone()).catch(() => {})
  }
  return response
}

function safeDecode (pathname: string): string {
  try { return decodeURIComponent(pathname) } catch { return pathname }
}

function withTimeout<T> (p: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), ms)
    p.then((v) => { clearTimeout(t); resolve(v) }, (e) => { clearTimeout(t); reject(e) })
  })
}

function errorPage (status: number, title: string, detail: string): Response {
  const body = `<!doctype html><html><head><meta charset="utf-8"><title>${status} ${escapeHtml(title)}</title>
<style>body{font:15px/1.6 system-ui,sans-serif;margin:0;display:grid;place-items:center;min-height:100vh;background:#0e1116;color:#e6edf3}
.card{max-width:32rem;padding:2rem;border:1px solid #30363d;border-radius:12px;background:#161b22}
h1{font-size:1.1rem;margin:0 0 .5rem} code{color:#7ee787;word-break:break-all} .s{color:#8b949e}</style></head>
<body><div class="card"><h1>${status} · ${escapeHtml(title)}</h1><p class="s">${escapeHtml(detail)}</p>
<p class="s">Served by the in-browser hoverfly Swarm gateway.</p></div></body></html>`
  const headers = new Headers({ 'content-type': 'text/html; charset=utf-8', server: 'hoverfly-gateway' })
  isolation(headers) // so the error page itself can embed in the require-corp shell
  return new Response(body, { status, headers })
}

/** Stamp the cross-origin-isolation headers needed for a document/resource to
 *  be embedded in the crossOriginIsolated boot shell. */
function isolation (headers: Headers): void {
  headers.set('cross-origin-embedder-policy', 'require-corp')
  headers.set('cross-origin-resource-policy', 'same-origin')
  // Allow CORS-mode requests to succeed. Even same-origin subresources fetched
  // with `crossorigin` (e.g. Next.js self-hosted fonts emit
  // `<link rel="preload" as="font" crossorigin="anonymous">`) are issued in
  // CORS mode, and under COEP: require-corp a CORS-mode response with no
  // Access-Control-Allow-Origin is a CORS failure — which silently breaks font
  // loading / framework hydration and leaves the page blank/unstyled. Swarm
  // content is public and content-addressed, so `*` is safe. `crossorigin`
  // without a value (or ="anonymous") is an uncredentialed request, so `*` is
  // accepted (it would only be rejected for credentialed requests).
  headers.set('access-control-allow-origin', '*')
}

function escapeHtml (s: string): string {
  return s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c] as string))
}
