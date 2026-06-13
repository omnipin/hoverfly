// Auto-discover postage batches owned by an address via Swarmscan.
//
// Swarmscan (https://api.swarmscan.io) indexes PostageStamp events — it's the
// data source batch-explorer.github.io uses. There's no owner-filtered query
// (the contract's BatchCreated event doesn't index `owner`), but the
// `batch-created` feed is reverse-chronological and includes `data.owner` per
// event. Since our batch owner is a freshly-minted session key, its batches sit
// near the top of the feed, so scanning a few recent pages and filtering
// client-side is enough to recover batches created in another browser — without
// the user pasting an ID. CORS is open, so we call it directly.
//
// This is best-effort discovery: on-chain verification (batches.ts) remains the
// source of truth for whether a discovered batch is real, owned by us, and
// usable.

import type { Address, Hex } from 'viem'
import { SWARMSCAN_BATCH_CREATED } from './config.ts'

interface SwarmscanBatchCreated {
  data: {
    batchId: Hex
    owner: Address
    depth: number
    bucketDepth: number
    immutableFlag: boolean
  }
  blockTime?: string
}
interface SwarmscanPage {
  events: SwarmscanBatchCreated[]
}

export interface DiscoveredBatch {
  batchId: Hex
  depth: number
  owner: Address
  createdAt: number
}

/**
 * Fetch the recent batch-created feed and return the batches whose `owner`
 * matches `sessionAddress`. Best-effort: returns `[]` on any fetch error.
 *
 * NOTE: Swarmscan's public feed exposes only the latest ~100 batch-created
 * events — its `next` cursor does not actually advance (every page returns the
 * same window), so there's no point looping. That's fine for our model: the
 * batch owner is a freshly-minted session key, so its batches are recent and
 * land in this window. Older batches that have scrolled out are recovered via
 * import-by-ID instead.
 */
export async function discoverBatchesByOwner (sessionAddress: Address): Promise<DiscoveredBatch[]> {
  const target = sessionAddress.toLowerCase()
  let resp: Response
  try {
    const ctrl = new AbortController()
    const t = setTimeout(() => ctrl.abort(), 8_000)
    resp = await fetch(SWARMSCAN_BATCH_CREATED, { signal: ctrl.signal })
    clearTimeout(t)
  } catch { return [] }
  if (!resp.ok) return []

  let body: SwarmscanPage
  try { body = await resp.json() as SwarmscanPage } catch { return [] }

  const found: DiscoveredBatch[] = []
  for (const ev of body.events ?? []) {
    const d = ev.data
    if (d?.owner?.toLowerCase() === target && d.batchId != null) {
      found.push({
        batchId: d.batchId,
        depth: Number(d.depth),
        owner: sessionAddress,
        createdAt: ev.blockTime != null ? Date.parse(ev.blockTime) : Date.now()
      })
    }
  }
  return found
}
