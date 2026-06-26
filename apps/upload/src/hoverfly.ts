// Main-thread RPC client for the hoverfly node Worker.
//
// The actual node (wasm, libp2p, rayon, tracing) lives in worker.ts so its
// dialing/hashing/logging never blocks the UI. This module just spawns the
// Worker, forwards calls over postMessage, and surfaces log/status events.

import { WORKER_JS } from './config.ts'
import { toTransferFiles, type Req, type ReqBody, type Res } from './worker-protocol.ts'

/** One entry passed to `uploadCollection`. */
export interface CollectionFile {
  path: string
  data: Uint8Array
  contentType?: string
}

export interface UploadSession {
  /** Number of peers we hold a live push session to (best effort). */
  connected: () => Promise<number>
  /** Upload a single file as a one-entry manifest. Returns the manifest root (hex). */
  uploadFile: (bytes: Uint8Array, path: string, contentType: string | undefined, batchIdHex: string, depth: number) => Promise<string>
  /** Upload a collection (tar/dir) as a multi-entry manifest. Returns the manifest root (hex). */
  uploadCollection: (files: CollectionFile[], indexDocument: string | undefined, errorDocument: string | undefined, batchIdHex: string, depth: number) => Promise<string>
}

const WORKER_URL = new URL(WORKER_JS, self.location.href).href

/**
 * Spawn the node worker, bring the node up (wasm + seed + discover + warm), and
 * return an RPC-backed UploadSession. `log` receives the worker's progress
 * lines; `onStatus` receives live connected-peer counts as the pool warms.
 */
export async function startUploadSession (
  sessionKeyHex: string,
  log: (m: string) => void,
  onStatus?: (connected: number) => void
): Promise<UploadSession> {
  const worker = new Worker(WORKER_URL, { type: 'module' })

  let nextId = 1
  const pending = new Map<number, { resolve: (v: unknown) => void, reject: (e: Error) => void }>()

  worker.onmessage = (e: MessageEvent<Res>) => {
    const msg = e.data
    if (msg.kind === 'log') { log(msg.message); return }
    if (msg.kind === 'status') { onStatus?.(msg.connected); return }
    if (msg.kind === 'result') {
      const p = pending.get(msg.id)
      if (p == null) return
      pending.delete(msg.id)
      if (msg.ok) p.resolve(msg.value)
      else p.reject(new Error(msg.error))
    }
  }
  worker.onerror = (e) => {
    const err = new Error(`worker error: ${e.message}`)
    for (const p of pending.values()) p.reject(err)
    pending.clear()
  }

  function call (req: ReqBody, transfer?: Transferable[]): Promise<unknown> {
    const id = nextId++
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject })
      worker.postMessage({ ...req, id } as Req, transfer ?? [])
    })
  }

  await call({ kind: 'start', sessionKeyHex })

  return {
    connected: async () => (await call({ kind: 'connected' })) as number,
    uploadFile: async (bytes, path, contentType, batchIdHex, depth) => {
      const buf = bytes.slice().buffer // detachable copy → transfer (zero-copy)
      return (await call(
        { kind: 'uploadFile', data: buf, path, contentType, batchIdHex, depth },
        [buf]
      )) as string
    },
    uploadCollection: async (files, indexDocument, errorDocument, batchIdHex, depth) => {
      const { files: tf, transfer } = toTransferFiles(files)
      return (await call(
        { kind: 'uploadCollection', files: tf, indexDocument, errorDocument, batchIdHex, depth },
        transfer
      )) as string
    }
  }
}
