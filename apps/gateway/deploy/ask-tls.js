// On-demand TLS gatekeeper for the Swarm gateway.
//
// Caddy calls GET /check?domain=<host> before issuing a certificate for a
// hostname seen on-demand. We return 200 only for hostnames that legitimately
// belong to this gateway, so a stranger pointing some unrelated DNS name at
// this IP can't make caddy mint certs (and burn Let's Encrypt rate limits).
//
// Allowed:
//   browserbzz.link                         (apex / gateway root)
//   <label>.browserbzz.link                 (a single-label <cid> content sub)
// Rejected: deeper nesting, foreign domains, empty domain.
//
// Bound to 127.0.0.1 only — never exposed publicly.

import { createServer } from 'node:http'

const ZONE = process.env.GW_ZONE ?? 'browserbzz.link'
const PORT = Number(process.env.GW_ASK_PORT ?? 9111)

function allowed (host) {
  if (!host) return false
  host = host.toLowerCase().split(':')[0]
  if (host === ZONE) return true
  if (!host.endsWith('.' + ZONE)) return false
  const label = host.slice(0, -(ZONE.length + 1))
  // exactly one label, no further dots (CIDv1 base32 is a single DNS label)
  return label.length > 0 && !label.includes('.')
}

createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost')
  if (url.pathname !== '/check') { res.writeHead(404).end(); return }
  const domain = url.searchParams.get('domain') ?? ''
  if (allowed(domain)) { res.writeHead(200).end('ok') }
  else { res.writeHead(403).end('denied') }
}).listen(PORT, '127.0.0.1', () => {
  console.log(`bzz-ask-tls listening on 127.0.0.1:${PORT} for *.${ZONE}`)
})
