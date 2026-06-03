// Host / request classification for the subdomain gateway.
//
// Root gateway + shared daemon:  bzz.<base>            (e.g. bzz.localhost:3000)
// Content origin (per site):     <cid>.bzz.<base>      (e.g. <cid>.bzz.localhost:3000)

import { GATEWAY_INFIX } from './config.ts'

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
  const idx = labels.indexOf(GATEWAY_INFIX)
  if (idx === -1) return { kind: 'other', rootHost: host }

  const rootHostname = labels.slice(idx).join('.')
  const rootHost = port != null ? `${rootHostname}:${port}` : rootHostname

  if (idx === 0) return { kind: 'root', rootHost }
  if (idx === 1) return { kind: 'subdomain', id: labels[0], rootHost }
  // Deeper nesting (e.g. a.b.bzz.host) — not a valid single-label content host.
  return { kind: 'other', rootHost }
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
