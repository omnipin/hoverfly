// Small byte helpers shared across the gateway (browser + node test).

export function hexToBytes (hex: string): Uint8Array {
  const clean = hex.startsWith('0x') ? hex.slice(2) : hex
  if (clean.length % 2 !== 0) {
    throw new Error(`hex length must be even, got ${clean.length}`)
  }
  const out = new Uint8Array(clean.length / 2)
  for (let i = 0; i < out.length; i++) {
    const byte = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16)
    if (Number.isNaN(byte)) {
      throw new Error(`invalid hex at offset ${i * 2}`)
    }
    out[i] = byte
  }
  return out
}

export function bytesToHex (bytes: Uint8Array): string {
  let out = ''
  for (let i = 0; i < bytes.length; i++) {
    out += bytes[i].toString(16).padStart(2, '0')
  }
  return out
}

export function isZero (bytes: Uint8Array): boolean {
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] !== 0) return false
  }
  return true
}

export function startsWith (haystack: Uint8Array, needle: Uint8Array): boolean {
  if (needle.length > haystack.length) return false
  for (let i = 0; i < needle.length; i++) {
    if (haystack[i] !== needle[i]) return false
  }
  return true
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

export function utf8 (s: string): Uint8Array {
  return textEncoder.encode(s)
}

export function fromUtf8 (b: Uint8Array): string {
  return textDecoder.decode(b)
}
