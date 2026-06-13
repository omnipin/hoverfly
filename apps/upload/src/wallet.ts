// Wallet + on-chain batch purchase.
//
// Connects an injected EIP-1193 wallet (MetaMask et al.), ensures it's on
// Gnosis (Swarm mainnet's settlement chain), and runs the bee-canonical
// `createBatch` flow — approve BZZ, then `PostageStamp.createBatch` — with the
// batch OWNER set to the in-browser session key (see session-key.ts). That's
// the only place the wallet signs: two transactions (an ERC-20 approve and the
// createBatch), or just one when the allowance already covers it.
//
// The amount/depth math mirrors src/batch.rs (the native `hoverfly batch
// create`) so the dApp buys identically-shaped batches.

import {
  createPublicClient, createWalletClient, custom, http,
  encodeFunctionData, parseAbi, parseAbiItem, decodeEventLog,
  type Address, type Hex, type PublicClient, type WalletClient
} from 'viem'
import { gnosis } from 'viem/chains'
import {
  BUCKET_DEPTH, BZZ_TOKEN, GNOSIS_BLOCK_TIME_SECS, GNOSIS_CHAIN_ID,
  GNOSIS_RPC, POSTAGE_STAMP
} from './config.ts'

// ---- ABIs (only what we touch) ----
const POSTAGE_ABI = parseAbi([
  'function createBatch(address owner, uint256 initialBalancePerChunk, uint8 depth, uint8 bucketDepth, bytes32 nonce, bool immutable_) returns (bytes32)',
  'function lastPrice() view returns (uint64)',
  'function minimumValidityBlocks() view returns (uint64)',
  'function batches(bytes32 id) view returns (address owner, uint8 depth, uint8 bucketDepth, bool immutableFlag, uint256 normalisedBalance, uint256 lastUpdatedBlockNumber)'
])
const ERC20_ABI = parseAbi([
  'function balanceOf(address account) view returns (uint256)',
  'function allowance(address owner, address spender) view returns (uint256)',
  'function approve(address spender, uint256 amount) returns (bool)'
])
const BATCH_CREATED = parseAbiItem(
  'event BatchCreated(bytes32 indexed batchId, uint256 totalAmount, uint256 normalisedBalance, address owner, uint8 depth, uint8 bucketDepth, bool immutableFlag)'
)

// Effective volume in binary GiB for each depth 17..=41 — copied verbatim from
// src/batch.rs (which copies the official bee-docs stamp calculator). Used to
// pick the smallest depth that covers a file's size.
const EFFECTIVE_VOLUME_GIB: Array<[number, number]> = [
  [17, 0.000043], [18, 0.006504], [19, 0.109434], [20, 0.671504], [21, 2.60],
  [22, 7.73], [23, 19.94], [24, 47.06], [25, 105.51], [26, 227.98], [27, 476.68],
  [28, 993.65], [29, 2088.96], [30, 4270.08], [31, 8652.80], [32, 17479.68],
  [33, 35184.64], [34, 70696.96], [35, 141864.96], [36, 284385.28],
  [37, 569702.40], [38, 1163919.36], [39, 2338324.48], [40, 4676648.96],
  [41, 9363783.68]
]

/** Smallest batch depth whose effective volume covers `bytes`. */
export function depthForSize (bytes: number): number {
  const gib = bytes / (1024 ** 3)
  for (const [depth, eff] of EFFECTIVE_VOLUME_GIB) if (eff >= gib) return depth
  return 41
}

/** Per-chunk amount (PLUR) for ~`durationSecs` of storage at `lastPrice`. */
export function amountForDuration (lastPrice: bigint, durationSecs: number): bigint {
  const blocks = BigInt(Math.ceil(durationSecs / GNOSIS_BLOCK_TIME_SECS))
  return blocks * lastPrice + 10n // +10 PLUR buffer (matches batch.rs)
}

export interface WalletConn {
  account: Address
  chainId: number
  wallet: WalletClient
  /** Read client pinned to Gnosis (uses the wallet transport when already on
   *  Gnosis, else a public RPC) so reads work even before a chain switch. */
  read: PublicClient
}

interface Eip1193 {
  request: (args: { method: string, params?: unknown[] }) => Promise<unknown>
  on?: (event: string, handler: (...a: unknown[]) => void) => void
}

function injected (): Eip1193 {
  const eth = (globalThis as { ethereum?: Eip1193 }).ethereum
  if (eth == null) throw new Error('No injected wallet found. Install MetaMask (or another EIP-1193 wallet).')
  return eth
}

/** Prompt the wallet to connect and ensure it's on Gnosis (chain 100). */
export async function connectWallet (): Promise<WalletConn> {
  const eth = injected()
  const wallet = createWalletClient({ chain: gnosis, transport: custom(eth) })
  const [account] = await wallet.requestAddresses()
  if (account == null) throw new Error('Wallet returned no account')

  // Ensure Gnosis. If the wallet doesn't know it, add it.
  let chainId = await wallet.getChainId()
  if (chainId !== GNOSIS_CHAIN_ID) {
    try {
      await wallet.switchChain({ id: GNOSIS_CHAIN_ID })
    } catch {
      await wallet.addChain({ chain: gnosis })
      await wallet.switchChain({ id: GNOSIS_CHAIN_ID })
    }
    chainId = await wallet.getChainId()
  }

  const read = chainId === GNOSIS_CHAIN_ID
    ? createPublicClient({ chain: gnosis, transport: custom(eth) })
    : createPublicClient({ chain: gnosis, transport: http(GNOSIS_RPC) })

  return { account, chainId, wallet, read }
}

export interface BatchQuote {
  depth: number
  /** Per-chunk PLUR amount. */
  amountPerChunk: bigint
  /** Total BZZ pulled = amountPerChunk * 2^depth (in PLUR). */
  totalPlur: bigint
  /** Minimum per-chunk amount the contract requires (lastPrice * minValidity). */
  minPerChunk: bigint
  lastPrice: bigint
  balancePlur: bigint
  enoughBalance: boolean
}

/** Compute a batch quote (depth, amount, total cost) for a file size + TTL. */
export async function quoteBatch (
  conn: WalletConn, sizeBytes: number, durationSecs: number
): Promise<BatchQuote> {
  const [lastPrice, minValidity, balancePlur] = await Promise.all([
    conn.read.readContract({ address: POSTAGE_STAMP, abi: POSTAGE_ABI, functionName: 'lastPrice' }),
    conn.read.readContract({ address: POSTAGE_STAMP, abi: POSTAGE_ABI, functionName: 'minimumValidityBlocks' }),
    conn.read.readContract({ address: BZZ_TOKEN, abi: ERC20_ABI, functionName: 'balanceOf', args: [conn.account] })
  ])
  const depth = depthForSize(sizeBytes)
  const minPerChunk = BigInt(lastPrice) * BigInt(minValidity)
  let amountPerChunk = amountForDuration(BigInt(lastPrice), durationSecs)
  // The contract requires amount > minInitialBalancePerChunk STRICTLY; if the
  // requested duration is below the contract minimum, bump to min + buffer.
  if (amountPerChunk <= minPerChunk) amountPerChunk = minPerChunk + 10n
  const totalPlur = amountPerChunk * (1n << BigInt(depth))
  return {
    depth, amountPerChunk, totalPlur, minPerChunk,
    lastPrice: BigInt(lastPrice), balancePlur,
    enoughBalance: balancePlur >= totalPlur
  }
}

export interface CreatedBatch {
  batchId: Hex
  depth: number
  owner: Address
  approveTx?: Hex
  createTx: Hex
}

/**
 * Run the approve + createBatch flow with `owner` set to the session key.
 *
 * Wallet prompts: one `approve` (skipped if the allowance already covers the
 * total) and one `createBatch`. After this, the session key alone can stamp
 * every chunk for the batch — no further wallet interaction during upload.
 */
export async function createBatch (
  conn: WalletConn, owner: Address, quote: BatchQuote,
  immutable: boolean, onStep: (msg: string) => void
): Promise<CreatedBatch> {
  const { wallet, read, account } = conn
  if (!quote.enoughBalance) {
    throw new Error(`Insufficient BZZ: need ${formatBzz(quote.totalPlur)} BZZ, have ${formatBzz(quote.balancePlur)} BZZ`)
  }

  // 1. Approve (idempotent — skip when allowance already covers the total).
  let approveTx: Hex | undefined
  const allowance = await read.readContract({
    address: BZZ_TOKEN, abi: ERC20_ABI, functionName: 'allowance',
    args: [account, POSTAGE_STAMP]
  })
  if (allowance < quote.totalPlur) {
    onStep('Approve BZZ spend in your wallet…')
    approveTx = await wallet.writeContract({
      chain: gnosis, account, address: BZZ_TOKEN, abi: ERC20_ABI,
      functionName: 'approve', args: [POSTAGE_STAMP, quote.totalPlur]
    })
    onStep('Waiting for approve confirmation…')
    await read.waitForTransactionReceipt({ hash: approveTx })
  }

  // 2. createBatch(owner = session key) with a random 32-byte nonce.
  const nonce = randomBytes32()
  onStep('Confirm createBatch in your wallet…')
  const createTx = await wallet.writeContract({
    chain: gnosis, account, address: POSTAGE_STAMP, abi: POSTAGE_ABI,
    functionName: 'createBatch',
    args: [owner, quote.amountPerChunk, quote.depth, BUCKET_DEPTH, nonce, immutable]
  })
  onStep('Waiting for createBatch confirmation…')
  const receipt = await read.waitForTransactionReceipt({ hash: createTx })

  // 3. Parse the BatchCreated event for the canonical batch id.
  for (const log of receipt.logs) {
    if (log.address.toLowerCase() !== POSTAGE_STAMP.toLowerCase()) continue
    try {
      const decoded = decodeEventLog({ abi: [BATCH_CREATED], data: log.data, topics: log.topics })
      if (decoded.eventName === 'BatchCreated') {
        return {
          batchId: decoded.args.batchId,
          depth: Number(decoded.args.depth),
          owner: decoded.args.owner,
          approveTx, createTx
        }
      }
    } catch { /* not our event */ }
  }
  throw new Error('createBatch succeeded but BatchCreated event not found in logs')
}

export interface OnChainBatch {
  owner: Address
  depth: number
  immutable: boolean
  /** Normalised balance (PLUR/chunk cumulative). 0 once the batch is expired. */
  normalisedBalance: bigint
  /** True when the getter returned an all-zero struct (batch never existed). */
  notFound: boolean
}

/** Read a batch's on-chain state (to verify owner/depth before re-using it). */
export async function readBatch (conn: WalletConn, batchId: Hex): Promise<OnChainBatch> {
  const [owner, depth, , immutableFlag, normalisedBalance] = await conn.read.readContract({
    address: POSTAGE_STAMP, abi: POSTAGE_ABI, functionName: 'batches', args: [batchId]
  })
  const notFound = owner === '0x0000000000000000000000000000000000000000' && depth === 0
  return { owner, depth: Number(depth), immutable: immutableFlag, normalisedBalance, notFound }
}

// ---- helpers ----
function randomBytes32 (): Hex {
  const b = new Uint8Array(32)
  crypto.getRandomValues(b)
  return ('0x' + Array.from(b, x => x.toString(16).padStart(2, '0')).join('')) as Hex
}

/** Format PLUR (1 BZZ = 1e16 PLUR) to a short BZZ string. */
export function formatBzz (plur: bigint): string {
  const BZZ = 10n ** 16n
  const whole = plur / BZZ
  const frac = (plur % BZZ) * 10000n / BZZ // 4 dp
  return `${whole}.${frac.toString().padStart(4, '0')}`
}

export { encodeFunctionData }
