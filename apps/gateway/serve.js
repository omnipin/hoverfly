// Dev server for the Swarm subdomain gateway.
//
// Serves the built dist/ for both the gateway root (bzz.localhost) and every
// content subdomain (<cid>.bzz.localhost), and sets the headers required for
// cross-origin isolation (SharedArrayBuffer) — the hoverfly wasm is built with
// shared memory, so the page MUST be cross-origin isolated:
//
//   Cross-Origin-Opener-Policy: same-origin
//   Cross-Origin-Embedder-Policy: credentialless   (lets the cross-origin
//     daemon iframe + CDN-free same-origin wasm load without explicit CORP)
//   Cross-Origin-Resource-Policy: cross-origin      (so the broker iframe is
//     embeddable from content subdomains)
//
// *.localhost resolves to 127.0.0.1 in Chrome automatically, so no /etc/hosts
// entries are needed.

import { createServer } from 'node:http'
import { readFile, stat } from 'node:fs/promises'
import { dirname, extname, join, normalize, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const dist = resolve(here, 'dist')
const PORT = Number(process.env.PORT ?? 3000)
const INFIX = 'bzz'

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.map': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8'
}

function classifyHost (host = '') {
  const hostname = host.split(':')[0]
  const labels = hostname.split('.')
  const idx = labels.indexOf(INFIX)
  if (idx === 0) return 'root'
  if (idx === 1) return 'subdomain'
  return 'other'
}

function isolationHeaders () {
  return {
    'Cross-Origin-Opener-Policy': 'same-origin',
    // require-corp (not credentialless): the broker iframe on the root origin
    // stays embeddable cross-subdomain via the CORP header below, and — unlike
    // credentialless — it is NOT partitioned into a separate agent cluster, so
    // its postMessage('frame-ready') reaches the content-origin shell and the
    // shared daemon SharedWorker is genuinely shared.
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': 'cross-origin'
  }
}

async function tryFile (pathname) {
  // resolve within dist, prevent traversal
  const rel = normalize(decodeURIComponent(pathname)).replace(/^(\.\.[/\\])+/, '')
  const file = join(dist, rel)
  if (!file.startsWith(dist)) return null
  try {
    const s = await stat(file)
    if (s.isFile()) return file
  } catch { /* not a file */ }
  return null
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`)
    const hostKind = classifyHost(req.headers.host)

    // 1) static asset? (?gw-unregister is the escape hatch that removes the
    //    service worker, the same page the Caddy deploy serves.)
    let file = url.searchParams.has('gw-unregister')
      ? join(dist, '__gw__', 'unregister.html')
      : await tryFile(url.pathname)

    // 2) SPA fallback for navigations
    if (file == null) {
      if (hostKind === 'subdomain') file = join(dist, 'boot.html')
      else if (hostKind === 'root') file = join(dist, 'index.html')
      else {
        res.writeHead(404, { 'content-type': 'text/plain', ...isolationHeaders() })
        res.end('Unknown host. Use bzz.localhost:' + PORT + ' or <cid>.bzz.localhost:' + PORT)
        return
      }
    }

    const body = await readFile(file)
    const headers = {
      'content-type': MIME[extname(file)] ?? 'application/octet-stream',
      'cache-control': 'no-cache',
      ...isolationHeaders()
    }
    // the service worker must be allowed to control the whole origin
    if (file.endsWith('/sw.js') || file.endsWith('\\sw.js')) {
      headers['Service-Worker-Allowed'] = '/'
    }
    res.writeHead(200, headers)
    res.end(body)
  } catch (err) {
    res.writeHead(500, { 'content-type': 'text/plain' })
    res.end('server error: ' + (err?.message ?? err))
  }
})

server.listen(PORT, () => {
  console.log(`\n  hoverfly Swarm gateway`)
  console.log(`  ─ root / daemon : http://${INFIX}.localhost:${PORT}`)
  console.log(`  ─ content       : http://<cid>.${INFIX}.localhost:${PORT}`)
  console.log(`\n  (Chrome resolves *.localhost to 127.0.0.1 automatically.)\n`)
})
