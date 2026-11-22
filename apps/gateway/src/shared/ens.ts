// ENS resolution: <name>.eth -> Swarm reference.
//
// ENS contenthash records (EIP-1577) store a multicodec-prefixed value. For a
// Swarm site that codec is `swarm-ns` (0xe4) followed by the CIDv1 bytes, which
// swarm-cid.ts decodes back to the 64-hex Swarm reference. The record is read
// through the ENS Universal Resolver, so names whose records live offchain
// (ENSIP-10 wildcard resolvers answering via CCIP-read) resolve too. Only Swarm
// contenthashes are accepted — an ENS name pointing at IPFS/IPNS/etc. can't be
// served by this Swarm gateway.

import {
  BaseError, ContractFunctionRevertedError, createPublicClient, decodeFunctionResult,
  encodeFunctionData, fallback, http, toHex
} from 'viem'
import { mainnet } from 'viem/chains'
import { namehash, normalize, packetToBytes } from 'viem/ens'
import { decodeSwarmContenthash } from './swarm-cid.ts'

/** ENS Universal Resolver on mainnet. */
export const ENS_UNIVERSAL_RESOLVER = '0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe'

/** viem's in-client batch gateway: CCIP-read lookups are resolved locally
 *  instead of through a third-party batch gateway server. */
const LOCAL_BATCH_GATEWAY = 'x-batch-gateway:true'

const UNIVERSAL_RESOLVER_ABI = [
  { name: 'HttpError', type: 'error', inputs: [{ name: 'status', type: 'uint16' }, { name: 'message', type: 'string' }] },
  { name: 'ResolverError', type: 'error', inputs: [{ name: 'errorData', type: 'bytes' }] },
  { name: 'ResolverNotContract', type: 'error', inputs: [{ name: 'name', type: 'bytes' }, { name: 'resolver', type: 'address' }] },
  { name: 'ResolverNotFound', type: 'error', inputs: [{ name: 'name', type: 'bytes' }] },
  { name: 'UnsupportedResolverProfile', type: 'error', inputs: [{ name: 'selector', type: 'bytes4' }] },
  {
    name: 'resolveWithGateways',
    type: 'function',
    stateMutability: 'view',
    inputs: [{ name: 'name', type: 'bytes' }, { name: 'data', type: 'bytes' }, { name: 'gateways', type: 'string[]' }],
    outputs: [{ name: '', type: 'bytes' }, { name: 'address', type: 'address' }]
  }
] as const

/** EIP-1577 resolver profile: just the contenthash getter. */
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
// that can be slow/flaky, so try a short list of reliable public endpoints
// first and keep the default as a last resort.
const client = createPublicClient({
  chain: mainnet,
  transport: fallback([
    http('https://ethereum-rpc.publicnode.com'),
    http('https://cloudflare-eth.com'),
    http()
  ])
})

/** True if `s` looks like an ENS name we should try to resolve: it has a dot,
 *  no whitespace or slashes, and survives ENSIP-15 normalization. */
export function looksLikeEnsName (s: string): boolean {
  const t = s.trim()
  if (!t.includes('.') || /[\s/]/.test(t)) return false
  try {
    normalize(t)
    return true
  } catch {
    return false
  }
}

export interface EnsSwarmResult {
  /** 64-char lowercase hex Swarm reference from the name's contenthash. */
  refHex: string
}

/** Readable message for a Universal Resolver revert, keyed by error name. */
function resolverErrorMessage (name: string, errorName: string | undefined): string | undefined {
  switch (errorName) {
    case 'ResolverNotFound': return `No ENS resolver set for ${name}`
    case 'ResolverNotContract': return `${name}'s resolver is not a contract`
    case 'UnsupportedResolverProfile': return `${name}'s resolver doesn't support contenthash records`
    case 'ResolverError': return `${name}'s resolver reverted while reading its contenthash`
    case 'HttpError': return `The offchain gateway for ${name} failed`
    default: return undefined
  }
}

/**
 * Resolve an ENS name to a Swarm reference via its contenthash record.
 * Throws if the name has no resolver, no contenthash, or a non-Swarm codec.
 */
export async function resolveEnsToSwarm (input: string): Promise<EnsSwarmResult> {
  const name = normalize(input.trim())

  let result: `0x${string}`
  try {
    [result] = await client.readContract({
      address: ENS_UNIVERSAL_RESOLVER,
      abi: UNIVERSAL_RESOLVER_ABI,
      functionName: 'resolveWithGateways',
      args: [
        toHex(packetToBytes(name)),
        encodeFunctionData({ abi: CONTENTHASH_ABI, functionName: 'contenthash', args: [namehash(name)] }),
        [LOCAL_BATCH_GATEWAY]
      ]
    })
  } catch (err) {
    if (err instanceof BaseError) {
      const revert = err.walk((e) => e instanceof ContractFunctionRevertedError)
      if (revert instanceof ContractFunctionRevertedError) {
        const msg = resolverErrorMessage(name, revert.data?.errorName)
        if (msg != null) throw new Error(msg)
      }
    }
    throw err
  }

  const contenthash = result === '0x'
    ? '0x'
    : decodeFunctionResult({ abi: CONTENTHASH_ABI, functionName: 'contenthash', data: result })
  if (contenthash === '0x') {
    throw new Error(`${name} has no contenthash record`)
  }

  try {
    return { refHex: decodeSwarmContenthash(contenthash).refHex }
  } catch (e) {
    const msg = (e as Error).message
    if (msg.startsWith('points at ')) {
      throw new Error(`${name} ${msg} — this gateway only serves bzz:// contenthashes`)
    }
    throw new Error(`${name} has an unreadable contenthash (${msg})`)
  }
}
