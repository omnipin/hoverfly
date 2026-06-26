// Session key: a secp256k1 key that owns the postage batch and signs every
// chunk's postage stamp, so uploads need ZERO per-chunk wallet prompts.
//
// ## Why a session key at all
//
// A bee postage stamp's signature is
//
//   secp256k1_sign( eip191_hash_message(
//     keccak256(chunkAddr || batchID || index || timestamp) ) )
//
// i.e. exactly `personal_sign` of a 32-byte digest. A MetaMask EOA *can* produce
// that — but bee needs ONE stamp signature PER CHUNK (~2500 for a 10 MB file).
// Prompting the wallet 2500 times is a non-starter. So we mint a separate key,
// set it as the batch `owner` in `createBatch`, and let it stamp every chunk
// locally with no prompts. (`createBatch(owner, …)` takes an explicit owner;
// bee validates each stamp against it.)
//
// ## Deterministic derivation from a wallet signature
//
// Rather than a random key (which only that browser's localStorage remembers),
// we DERIVE the session key from a deterministic wallet signature:
//
//   sessionPrivKey = keccak256( wallet.personal_sign(DERIVATION_MESSAGE) )
//
// ECDSA `personal_sign` is deterministic per (key, message) for EOAs (RFC 6979),
// so the SAME wallet reproduces the SAME session key — hence the same batch
// owner — on any device, with no storage. That makes batches recoverable by
// design: reconnect the same wallet anywhere and you own the same batches.
//
// ### Why this isn't a security hole
//
//  - It's a ONE-WAY derivation: the session key is `keccak256(signature)`; you
//    cannot recover the wallet key from it.
//  - It's SCOPED: the session key's only power is stamping chunks for batches it
//    owns. Worst case if it leaks = an attacker burns that batch's prepaid
//    storage. It can never touch the wallet, funds, or anything else. (Same
//    blast radius as the old random key — derivation doesn't widen it.)
//  - DOMAIN SEPARATION: the signed message is a fixed, human-readable, app- and
//    version-specific string (below), so this signature can't collide with a
//    transaction or another dapp's signature, and the user sees exactly what
//    they're approving (no blind hash-signing).
//
// Smart-contract wallets / some hardware signers may not return a stable
// signature; the derived key is cached per wallet address in localStorage, so a
// returning session reuses it without re-prompting, and rotate/import remain as
// escape hatches.

import { generatePrivateKey, privateKeyToAccount } from 'viem/accounts'
import { keccak256, type Address, type Hex, type WalletClient } from 'viem'
import { LS_SESSION_KEY } from './config.ts'

/**
 * The exact message the wallet signs to derive the session key. Keep this
 * STABLE — changing it changes everyone's derived key (and thus which batches
 * they own). The version suffix lets us intentionally migrate later if needed.
 */
const DERIVATION_MESSAGE =
  'Hoverfly Swarm upload — derive postage-stamping session key.\n\n' +
  'Signing this creates a key that can stamp chunks for batches you buy here. ' +
  'It cannot move funds or act outside this app. Only sign on the Hoverfly upload dApp.\n\n' +
  'Version: 1'

export interface SessionKey {
  /** 0x-prefixed 32-byte private key. Lives only in this browser. */
  readonly privateKeyHex: Hex
  /** The key WITHOUT the 0x prefix — the form hoverfly wasm expects. */
  readonly bareKeyHex: string
  /** Ethereum address derived from the key; becomes the batch owner. */
  readonly address: Address
}

function fromHex (privateKeyHex: Hex): SessionKey {
  const account = privateKeyToAccount(privateKeyHex)
  return {
    privateKeyHex,
    bareKeyHex: privateKeyHex.slice(2),
    address: account.address
  }
}

/** localStorage cache key, scoped per connected wallet address. */
function cacheKey (wallet: Address): string {
  return `${LS_SESSION_KEY}:${wallet.toLowerCase()}`
}

/** Return the cached session key for `account` if one exists, else null. Never
 *  prompts — used by eager connect to wire up silently when possible. */
export function cachedSessionKey (account: Address): SessionKey | null {
  const cached = localStorage.getItem(cacheKey(account))
  return cached != null && /^0x[0-9a-fA-F]{64}$/.test(cached) ? fromHex(cached as Hex) : null
}

/**
 * Derive (or load the cached) session key for `account`. On first use this
 * prompts the wallet to `personal_sign` the derivation message; subsequent
 * sessions reuse the cached key for that wallet without a prompt.
 *
 * `onSign` (optional) is called right before the signature prompt, so the UI
 * can explain why a signature is being requested.
 */
export async function deriveSessionKey (
  wallet: WalletClient,
  account: Address,
  onSign?: () => void
): Promise<SessionKey> {
  const cached = localStorage.getItem(cacheKey(account))
  if (cached != null && /^0x[0-9a-fA-F]{64}$/.test(cached)) {
    return fromHex(cached as Hex)
  }
  onSign?.()
  const signature = await wallet.signMessage({ account, message: DERIVATION_MESSAGE })
  // keccak256 of the 65-byte signature → 32-byte private key (one-way).
  const pk = keccak256(signature)
  localStorage.setItem(cacheKey(account), pk)
  return fromHex(pk)
}

/**
 * Rotate to a fresh RANDOM session key for `account`, overriding the derived
 * one. Use when you want a clean owner unrelated to the wallet (e.g. the
 * derived key was somehow exposed). Existing batches owned by the previous key
 * can no longer receive uploads. Returns the new key.
 */
export function rotateSessionKey (account: Address): SessionKey {
  const pk = generatePrivateKey()
  localStorage.setItem(cacheKey(account), pk)
  return fromHex(pk)
}
