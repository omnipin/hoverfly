// Swarm reference <-> CIDv1 multibase encoding.
//
// Port of isheika `src/cid.rs`. A raw 32-byte Swarm reference is 64 hex
// chars, which exceeds the 63-char DNS label limit, so it cannot be used
// directly as a subdomain. Instead we encode it the same way bzz.limo / the
// ENS `contenthash` spec do:
//
//   CID  = varint(1) || varint(0xfa) || varint(0x1b) || varint(32) || ref32
//   text = "b" + base32_lower_no_pad(CID)
//
//   0x01 -> CIDv1
//   0xfa -> swarm-manifest codec
//   0x1b -> keccak-256 multihash code
//   "b"  -> multibase prefix for RFC 4648 lowercase base32, no padding
//
// The resulting label is ~61 chars, which fits in a DNS label and is
// case-insensitive (origin-safe).

import { hexToBytes, bytesToHex } from './bytes.ts'

const CID_V1 = 1
export const SWARM_MANIFEST_CODEC = 0xfa
export const SWARM_FEED_CODEC = 0xfb
const KECCAK_256_MULTIHASH = 0x1b
const REF_LEN = 32

const B32_ALPHABET = 'abcdefghijklmnopqrstuvwxyz234567'
const B32_REVERSE: Record<string, number> = (() => {
  const m: Record<string, number> = {}
  for (let i = 0; i < B32_ALPHABET.length; i++) m[B32_ALPHABET[i]] = i
  return m
})()

function pushVarint (out: number[], value: number): void {
  // values here are small (<= 0xfa), but keep a correct unsigned LEB128.
  while (value >= 0x80) {
    out.push((value & 0x7f) | 0x80)
    value >>>= 7
  }
  out.push(value)
}

function readVarint (bytes: Uint8Array, offset: number): { value: number, next: number } {
  let value = 0
  let shift = 0
  let pos = offset
  for (;;) {
    if (pos >= bytes.length) throw new Error('varint: unexpected end')
    const byte = bytes[pos++]
    value |= (byte & 0x7f) << shift
    if ((byte & 0x80) === 0) break
    shift += 7
    if (shift > 35) throw new Error('varint: too long')
  }
  return { value: value >>> 0, next: pos }
}

export function base32LowerEncode (input: Uint8Array): string {
  let out = ''
  let buffer = 0
  let bits = 0
  for (let i = 0; i < input.length; i++) {
    buffer = (buffer << 8) | input[i]
    bits += 8
    while (bits >= 5) {
      bits -= 5
      out += B32_ALPHABET[(buffer >>> bits) & 0x1f]
    }
  }
  if (bits > 0) {
    out += B32_ALPHABET[(buffer << (5 - bits)) & 0x1f]
  }
  return out
}

export function base32LowerDecode (input: string): Uint8Array {
  const out: number[] = []
  let buffer = 0
  let bits = 0
  for (let i = 0; i < input.length; i++) {
    const ch = input[i].toLowerCase()
    const val = B32_REVERSE[ch]
    if (val === undefined) throw new Error(`base32: invalid char "${input[i]}"`)
    buffer = (buffer << 5) | val
    bits += 5
    if (bits >= 8) {
      bits -= 8
      out.push((buffer >>> bits) & 0xff)
    }
  }
  return Uint8Array.from(out)
}

/** Encode a 32-byte Swarm reference (hex) as a CIDv1 multibase string. */
export function referenceToCid (refHex: string, codec: number = SWARM_MANIFEST_CODEC): string {
  const ref = hexToBytes(refHex)
  if (ref.length !== REF_LEN) {
    throw new Error(`swarm reference must be ${REF_LEN} bytes, got ${ref.length}`)
  }
  const header: number[] = []
  pushVarint(header, CID_V1)
  pushVarint(header, codec)
  pushVarint(header, KECCAK_256_MULTIHASH)
  pushVarint(header, REF_LEN)
  const bytes = new Uint8Array(header.length + REF_LEN)
  bytes.set(header, 0)
  bytes.set(ref, header.length)
  return 'b' + base32LowerEncode(bytes)
}

/** Decode a swarm CIDv1 multibase string back into the 32-byte reference hex. */
export function cidToReference (cid: string): { refHex: string, codec: number } {
  if (cid.length === 0 || cid[0] !== 'b') {
    throw new Error('only multibase "b" (base32 lower) CIDs are supported')
  }
  const bytes = base32LowerDecode(cid.slice(1))
  let r = readVarint(bytes, 0)
  if (r.value !== CID_V1) throw new Error(`unexpected CID version ${r.value}`)
  r = readVarint(bytes, r.next)
  const codec = r.value
  if (codec !== SWARM_MANIFEST_CODEC && codec !== SWARM_FEED_CODEC) {
    throw new Error(`unexpected codec 0x${codec.toString(16)} (not a swarm CID)`)
  }
  r = readVarint(bytes, r.next)
  if (r.value !== KECCAK_256_MULTIHASH) {
    throw new Error(`unexpected multihash code 0x${r.value.toString(16)} (expected keccak-256)`)
  }
  r = readVarint(bytes, r.next)
  if (r.value !== REF_LEN) throw new Error(`unexpected digest length ${r.value}`)
  const digest = bytes.slice(r.next, r.next + REF_LEN)
  if (digest.length !== REF_LEN) throw new Error('truncated CID digest')
  return { refHex: bytesToHex(digest), codec }
}

/** Heuristic: does this look like a multibase base32 swarm CID? */
export function looksLikeCid (s: string): boolean {
  return /^b[a-z2-7]{58,}$/.test(s)
}
