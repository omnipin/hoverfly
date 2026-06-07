// ENS resolution: <name>.eth -> Swarm reference.
//
// ENS contenthash records (EIP-1577) store a multicodec-prefixed value. For a
// Swarm site that codec is `swarm-ns` (0xe4); @ensdomains/content-hash decodes
// it back to the 64-hex Swarm reference. We resolve the contenthash via viem
// (read the name's resolver, then its `contenthash(node)` record) and decode
// it here. Only `swarm` contenthashes are accepted — an ENS name pointing at
// IPFS/IPNS/etc. can't be served by this Swarm gateway.

import { createPublicClient, fallback, http, namehash } from 'viem'
import { mainnet } from 'viem/chains'
import { getEnsResolver } from 'viem/ens'
import { decode as decodeContentHash, getCodec } from '@ensdomains/content-hash'

/** Minimal resolver ABI: just the EIP-1577 contenthash getter. */
const CONTENTHASH_ABI = [
  {
    name: 'contenthash',
    type: 'function',
    stateMutability: 'view',
    inputs: [{ name: 'node', type: 'bytes32' }],
    outputs: [{ name: '', type: 'bytes' }]
  }
] as const

// A public mainnet client. ENS lives on mainnet regardless of the Swarm network
// the gateway fetches from. viem's bare `http()` default picks one public RPC
// that can be slow/flaky (observed timeouts on eth.merkle.io); use an explicit
// fallback list of reliable public endpoints so resolution degrades gracefully
// rather than hanging on a single bad node.
const client = createPublicClient({
  chain: mainnet,
  transport: fallback([
    http('https://ethereum-rpc.publicnode.com'),
    http('https://eth.llamarpc.com'),
    http('https://cloudflare-eth.com'),
    http() // viem default as a last resort
  ])
})

/** True if `s` looks like an ENS name we should try to resolve (has a dot and
 *  a non-numeric TLD-ish suffix; `.eth` and DNS-imported names like `.box`). */
export function looksLikeEnsName (s: string): boolean {
  const t = s.trim().toLowerCase()
  // must contain a dot, no slashes/spaces, and not be a bare hex/CID
  if (!t.includes('.') || /\s/.test(t)) return false
  // exclude things that are clearly an IP:port or URL host we stripped already
  return /^[a-z0-9-]+(\.[a-z0-9-]+)+$/.test(t)
}

export interface EnsSwarmResult {
  /** 64-char lowercase hex Swarm reference from the name's contenthash. */
  refHex: string
}

/**
 * Resolve an ENS name to a Swarm reference via its contenthash record.
 * Throws if the name has no resolver, no contenthash, or a non-Swarm codec.
 */
export async function resolveEnsToSwarm (name: string): Promise<EnsSwarmResult> {
  const normalized = name.trim().toLowerCase()

  const resolverAddress = await getEnsResolver(client, { name: normalized })
  if (resolverAddress == null) {
    throw new Error(`No ENS resolver set for ${normalized}`)
  }

  const raw = (await client.readContract({
    address: resolverAddress,
    abi: CONTENTHASH_ABI,
    functionName: 'contenthash',
    args: [namehash(normalized)]
  })) as `0x${string}`

  if (raw == null || raw === '0x') {
    throw new Error(`${normalized} has no contenthash record`)
  }

  // content-hash works on the hex string without the 0x prefix.
  const hex = raw.startsWith('0x') ? raw.slice(2) : raw
  const codec = getCodec(hex)
  if (codec !== 'swarm') {
    throw new Error(
      `${normalized} points at "${codec}", not Swarm — this gateway only serves bzz:// contenthashes`
    )
  }
  const refHex = decodeContentHash(hex).toLowerCase()
  if (!/^[0-9a-f]{64}$/.test(refHex)) {
    throw new Error(`${normalized} decoded to an unexpected Swarm reference: ${refHex}`)
  }
  return { refHex }
}
