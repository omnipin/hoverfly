// Batch discovery.
//
// There is no public "list all postage batches for owner X" API for the stock
// Swarm PostageStamp contract — its `BatchCreated` event doesn't index `owner`,
// and `batches(id)` is a per-id getter with no by-owner index. (Beeport only
// lists batches because it routes creation through its OWN registry contract
// that records `ownerBatches`; we call the stock contract directly.) A full
// `eth_getLogs` history scan from the contract's start block is impractical in
// the browser.
//
// So discovery here is: this browser remembers the batch IDs it created
// (localStorage), and we VERIFY each against the chain — confirming it exists
// and is owned by the current session key, and reading its live depth /
// balance. Because the batch owner IS the session key (unique per browser),
// that local list is effectively complete for this key; the only gap is
// "cleared storage / different browser", covered by manual import-by-ID.

import type { Address, Hex } from 'viem'
import { LS_BATCHES } from './config.ts'
import type { OnChainBatch, WalletConn } from './wallet.ts'
import { readBatch } from './wallet.ts'
import { discoverBatchesByOwner } from './swarmscan.ts'

export interface SavedBatch {
  batchId: Hex
  depth: number
  owner: Address
  createdAt: number
  /** Approx bytes the batch was sized for (display only). */
  sizeBytes?: number
}

/** A saved batch confirmed against the chain, with live on-chain state. */
export interface VerifiedBatch extends SavedBatch {
  onChain: OnChainBatch
  /** Usable = exists, owned by the session key, and not expired. */
  usable: boolean
}

export function loadBatches (): SavedBatch[] {
  try {
    const raw = localStorage.getItem(LS_BATCHES)
    if (raw == null) return []
    const parsed = JSON.parse(raw) as SavedBatch[]
    return Array.isArray(parsed) ? parsed : []
  } catch { return [] }
}

export function saveBatch (b: SavedBatch): void {
  const all = loadBatches().filter(x => x.batchId.toLowerCase() !== b.batchId.toLowerCase())
  all.unshift(b)
  localStorage.setItem(LS_BATCHES, JSON.stringify(all.slice(0, 50)))
}

function writeAll (batches: SavedBatch[]): void {
  localStorage.setItem(LS_BATCHES, JSON.stringify(batches.slice(0, 50)))
}

/**
 * Verify every saved batch owned by `sessionAddress` against the chain. Returns
 * the verified list (with live on-chain depth/balance and a `usable` flag).
 *
 * Side effect: prunes entries that no longer exist on-chain or whose owner
 * isn't the session key (stale/foreign records) from localStorage, so the list
 * stays trustworthy. Reads run concurrently. A read failure (RPC hiccup) keeps
 * the entry (marked not-usable) rather than dropping it.
 */
export async function verifyBatches (conn: WalletConn, sessionAddress: Address): Promise<VerifiedBatch[]> {
  // Auto-discover via Swarmscan first (recovers batches created in another
  // browser / after cleared storage) and merge into the local set. Best-effort;
  // on-chain verification below is the source of truth either way.
  try {
    for (const d of await discoverBatchesByOwner(sessionAddress)) {
      const known = loadBatches().some(b => b.batchId.toLowerCase() === d.batchId.toLowerCase())
      if (!known) saveBatch(d)
    }
  } catch { /* best effort */ }

  const mine = loadBatches().filter(b => b.owner.toLowerCase() === sessionAddress.toLowerCase())
  const results = await Promise.allSettled(mine.map(b => readBatch(conn, b.batchId)))

  const verified: VerifiedBatch[] = []
  const keep: SavedBatch[] = []
  // Preserve any saved batches NOT owned by this session key untouched.
  for (const b of loadBatches()) {
    if (b.owner.toLowerCase() !== sessionAddress.toLowerCase()) keep.push(b)
  }

  mine.forEach((b, i) => {
    const r = results[i]
    if (r.status !== 'fulfilled') {
      // RPC failed — don't drop the record, just can't confirm it now.
      keep.push(b)
      verified.push({ ...b, onChain: { owner: b.owner, depth: b.depth, immutable: false, normalisedBalance: 0n, notFound: false }, usable: false })
      return
    }
    const oc = r.value
    const ownedByUs = oc.owner.toLowerCase() === sessionAddress.toLowerCase()
    if (oc.notFound || !ownedByUs) return // prune: gone, or never ours
    keep.push({ ...b, depth: oc.depth }) // refresh depth from chain
    verified.push({ ...b, depth: oc.depth, onChain: oc, usable: oc.normalisedBalance > 0n })
  })

  writeAll(keep)
  verified.sort((a, b) => b.createdAt - a.createdAt)
  return verified
}

/**
 * Import a batch by ID: confirm it exists on-chain and is owned by the session
 * key, then persist it. Returns the verified batch. Throws with a clear reason
 * if it doesn't exist or isn't owned by this key (so it can't be stamped).
 */
export async function importBatch (conn: WalletConn, sessionAddress: Address, batchId: Hex): Promise<VerifiedBatch> {
  if (!/^0x[0-9a-fA-F]{64}$/.test(batchId)) throw new Error('Batch ID must be a 0x-prefixed 32-byte hex string')
  const oc = await readBatch(conn, batchId)
  if (oc.notFound) throw new Error('No such batch on-chain')
  if (oc.owner.toLowerCase() !== sessionAddress.toLowerCase()) {
    throw new Error(`Batch is owned by ${oc.owner}, not this session key (${sessionAddress}). Only the owner can stamp chunks for it.`)
  }
  const saved: SavedBatch = { batchId, depth: oc.depth, owner: sessionAddress, createdAt: Date.now() }
  saveBatch(saved)
  return { ...saved, onChain: oc, usable: oc.normalisedBalance > 0n }
}
