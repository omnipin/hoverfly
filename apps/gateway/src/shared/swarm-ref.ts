// Normalize user-entered Swarm references into a {cid, refHex} pair.
//
// Accepts:
//   - 64-char hex (optionally 0x-prefixed)  -> the raw 32-byte reference
//   - a swarm multibase CID (b...)          -> decoded to its reference
//   - bzz://<ref|cid>/...  or  /bzz/<ref|cid>/...  (prefix stripped)

import { cidToReference, looksLikeCid, referenceToCid } from './swarm-cid.ts'

export interface SwarmRef {
  /** DNS-safe base32 CIDv1 label for the subdomain. */
  cid: string
  /** 64-char lowercase hex of the 32-byte reference. */
  refHex: string
}

const HEX64 = /^[0-9a-fA-F]{64}$/

export function normalizeRef (input: string): SwarmRef {
  let s = input.trim()
  // strip scheme / gateway path prefixes
  s = s.replace(/^bzz:\/\//i, '').replace(/^https?:\/\/[^/]+\//i, '').replace(/^\/?bzz\//i, '')
  // keep only the first path segment / drop query+hash
  s = s.split(/[/?#]/)[0]

  if (looksLikeCid(s)) {
    const { refHex } = cidToReference(s)
    return { cid: s, refHex }
  }

  const hex = s.startsWith('0x') ? s.slice(2) : s
  if (HEX64.test(hex)) {
    const refHex = hex.toLowerCase()
    return { cid: referenceToCid(refHex), refHex }
  }

  throw new Error('Enter a 64-char hex Swarm reference or a swarm CID (starts with "b").')
}

export function isValidRef (input: string): boolean {
  try {
    normalizeRef(input)
    return true
  } catch {
    return false
  }
}
