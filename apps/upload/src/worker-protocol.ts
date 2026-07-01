// postMessage protocol between the main thread (hoverfly.ts) and the hoverfly
// node Worker (worker.ts). The node — libp2p driver, dial churn, rayon hashing,
// verbose tracing — runs entirely in the Worker so none of it janks the UI.

import type { CollectionFile } from './hoverfly.ts'
// (type-only import — no runtime cycle)

// ---- requests (main → worker) ----
/** Request bodies without the correlation `id` (added by the RPC layer). */
export type ReqBody =
  | { kind: 'start', sessionKeyHex: string }
  | { kind: 'connected' }
  | { kind: 'uploadFile', data: ArrayBuffer, path: string, contentType: string | undefined, batchIdHex: string, depth: number, immutable: boolean }
  | {
      kind: 'uploadCollection', batchIdHex: string, depth: number, immutable: boolean,
      indexDocument: string | undefined, errorDocument: string | undefined,
      // file bytes travel as ArrayBuffers (transferable) keyed alongside paths
      files: Array<{ path: string, data: ArrayBuffer, contentType?: string }>
    }
export type Req = ReqBody & { id: number }

// ---- responses + events (worker → main) ----
export type Res =
  | { kind: 'result', id: number, ok: true, value: unknown }
  | { kind: 'result', id: number, ok: false, error: string }
  | { kind: 'log', message: string }
  | { kind: 'status', connected: number }
  // Live upload progress: `done`/`total` chunks pushed. Emitted by the worker
  // on a timer while an upload is in flight (polling the wasm client's
  // `uploadProgress()`), so the UI bar tracks real chunk pushes.
  | { kind: 'progress', done: number, total: number }

/** Convert main-thread CollectionFile[] (Uint8Array) to transferable form. */
export function toTransferFiles (files: CollectionFile[]): {
  files: Array<{ path: string, data: ArrayBuffer, contentType?: string }>
  transfer: ArrayBuffer[]
} {
  const out = files.map(f => {
    // Copy out of any shared/over-allocated buffer into a tight ArrayBuffer so
    // it can be transferred (zero-copy) and detached safely.
    const buf = f.data.byteOffset === 0 && f.data.byteLength === f.data.buffer.byteLength
      ? f.data.buffer
      : f.data.slice().buffer
    return { path: f.path, data: buf as ArrayBuffer, contentType: f.contentType }
  })
  return { files: out, transfer: out.map(f => f.data) }
}
