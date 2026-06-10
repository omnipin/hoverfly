// Host / request classification for the subdomain gateway.
//
// Two deployment schemes are supported:
//
//   Infix scheme (local dev):
//     root      bzz.<base>          e.g. bzz.localhost:3000
//     content   <cid>.bzz.<base>    e.g. <cid>.bzz.localhost:3000
//
//   Infix-less scheme (production apex):
//     root      <base>              e.g. browserbzz.link
//     content   <cid>.<base>        e.g. <cid>.browserbzz.link
//
// The infix is matched first when present; otherwise a host is classified as a
// content origin iff its leftmost label is a Swarm CID, and as the root
// otherwise. (A CID label is unambiguous — see looksLikeCid.)

import { GATEWAY_INFIX } from './config.ts'
import { looksLikeCid } from './swarm-cid.ts'

export interface GatewayHost {
  kind: 'root' | 'subdomain' | 'other'
  /** subdomain label (the swarm CID) when kind === 'subdomain'. */
  id?: string
  /** The gateway root host (incl. port), e.g. `bzz.localhost:3000`. */
  rootHost: string
}

function splitHostPort (host: string): [string, string | undefined] {
  const i = host.lastIndexOf(':')
  // Guard against IPv6 (we don't support it as a gateway origin anyway).
  if (i === -1 || host.indexOf(':') !== i) return [host, undefined]
  return [host.slice(0, i), host.slice(i + 1)]
}

/** Classify a Host header / location.host string. */
export function parseHost (host: string): GatewayHost {
  const [hostname, port] = splitHostPort(host)
  const labels = hostname.split('.')
  const withPort = (rootHostname: string): string =>
    port != null ? `${rootHostname}:${port}` : rootHostname

  // 1) Infix scheme: an explicit `bzz` label marks the root boundary.
  const idx = labels.indexOf(GATEWAY_INFIX)
  if (idx !== -1) {
    const rootHost = withPort(labels.slice(idx).join('.'))
    if (idx === 0) return { kind: 'root', rootHost }
    if (idx === 1) return { kind: 'subdomain', id: labels[0], rootHost }
    // Deeper nesting (e.g. a.b.bzz.host) — not a single-label content host.
    return { kind: 'other', rootHost }
  }

  // 2) Infix-less scheme (production apex): a leading CID label => content
  //    origin; the root is everything after it. Without a CID label the host
  //    IS the root (gateway landing + shared daemon origin).
  const first = labels[0]
  if (labels.length >= 2 && looksLikeCid(first)) {
    return { kind: 'subdomain', id: first, rootHost: withPort(labels.slice(1).join('.')) }
  }
  return { kind: 'root', rootHost: withPort(hostname) }
}

/** Minimal shape shared by `Location`, `WorkerLocation` and `URL`. */
export interface LocationLike { protocol: string, host: string }

/** Origin (scheme + host) of the shared daemon for the current location. */
export function daemonOrigin (loc: LocationLike): string {
  const parsed = parseHost(loc.host)
  return `${loc.protocol}//${parsed.rootHost}`
}

/** Build the content-origin URL for a given swarm CID, preserving path/query. */
export function subdomainUrl (cid: string, loc: LocationLike, pathname = '/', search = ''): string {
  const parsed = parseHost(loc.host)
  return `${loc.protocol}//${cid}.${parsed.rootHost}${pathname}${search}`
}
