// Bridge protocol between the shared hoverfly daemon (SharedWorker) and its
// clients (landing page, content-host document, service worker).
//
// The daemon serves RPC on any MessagePort. A port can also carry a special
// `{type:'attach'}` control message transferring a *new* MessagePort, which
// the daemon adopts as another RPC channel. This is how a cross-origin
// content origin reaches the single same-origin daemon: the broker iframe
// (same origin as the daemon) attaches the embedder's port, and the document
// can in turn mint a further port for its service worker.

export interface DaemonStatus {
  ready: boolean
  warming: boolean
  peerCount: number
  /** peers with a browser-dialable (/ws or /wss) underlay. */
  dialable: number
  /** peers we currently hold a live retrieval session (open connection) to —
   *  the warm forwarder set. Distinct from `dialable` (peers that merely
   *  advertise a /ws[s] underlay): this counts connections actually
   *  established. This is the count surfaced to users as "connected peers". */
  connected: number
  network: number
  bootstrap: string
  lastError?: string
  /** Coarse warm()/runtime phase, surfaced so clients (SW/page) can see daemon
   *  progress without opening the SharedWorker console. */
  phase?: string
  /** The Swarm reference (hex) the current `phase` belongs to, when the phase
   *  describes a specific per-CID fetch (e.g. "fetching index.html …"). The
   *  daemon is SHARED across every content origin, so its `phase` is global —
   *  without this, one CID's boot shell would display another CID's in-flight
   *  file progress. A content shell shows `phase` only when this is null (a
   *  daemon-lifecycle phase: warming/ready) or equals its OWN reference.
   *  Undefined for lifecycle phases. */
  phaseRef?: string
}

interface WithId { id: number }

export type DaemonRequestBody =
  | { kind: 'status' }
  | { kind: 'discover', bootstrap?: string, waitSecs?: number }
  | { kind: 'fetchPath', refHex: string, path: string }

export type DaemonRequest = DaemonRequestBody & WithId

export interface StatusResponse extends WithId { kind: 'status', status: DaemonStatus }
export interface DiscoverResponse extends WithId { kind: 'discover', ok: boolean, status: DaemonStatus, error?: string }
export interface FetchPathResponse extends WithId {
  kind: 'fetchPath'
  ok: boolean
  httpStatus: number
  contentType?: string
  body?: ArrayBuffer
  error?: string
  /** True iff the reference resolved through a feed manifest (mutable content).
   *  The SW must not cache such responses as immutable — see sw.ts. */
  mutable?: boolean
}

export type DaemonResponse = StatusResponse | DiscoverResponse | FetchPathResponse
export type DaemonEvent = { kind: 'event', event: 'status', status: DaemonStatus }

export const ATTACH = 'attach'

type StatusListener = (s: DaemonStatus) => void

/** Client-side RPC wrapper around a MessagePort connected to the daemon. */
export class DaemonRpc {
  private seq = 0
  private readonly pending = new Map<number, { resolve: (v: any) => void, reject: (e: any) => void }>()
  private readonly statusListeners = new Set<StatusListener>()
  lastStatus: DaemonStatus | undefined

  constructor (private readonly port: MessagePort) {
    port.onmessage = (e: MessageEvent) => this.onMessage(e.data)
    port.start?.()
  }

  onStatus (fn: StatusListener): () => void {
    this.statusListeners.add(fn)
    if (this.lastStatus != null) fn(this.lastStatus)
    return () => this.statusListeners.delete(fn)
  }

  private onMessage (msg: any): void {
    if (msg == null) return
    if (msg.kind === 'event') {
      if (msg.event === 'status') {
        this.lastStatus = msg.status
        this.statusListeners.forEach(fn => fn(msg.status))
      }
      return
    }
    const p = this.pending.get(msg.id)
    if (p != null) {
      this.pending.delete(msg.id)
      p.resolve(msg)
    }
  }

  private send<T> (req: DaemonRequestBody): Promise<T> {
    const id = ++this.seq
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      this.port.postMessage({ ...req, id })
    })
  }

  status (): Promise<StatusResponse> { return this.send({ kind: 'status' }) }
  discover (bootstrap?: string, waitSecs?: number): Promise<DiscoverResponse> { return this.send({ kind: 'discover', bootstrap, waitSecs }) }
  fetchPath (refHex: string, path: string): Promise<FetchPathResponse> { return this.send({ kind: 'fetchPath', refHex, path }) }
}

/**
 * Given a MessagePort already connected to the daemon, mint a *new* RPC port
 * to the same daemon by transferring one end via an `attach` control message.
 * Returns the local end to hand to another consumer (e.g. a service worker).
 */
export function mintDaemonPort (existing: MessagePort): MessagePort {
  const channel = new MessageChannel()
  existing.postMessage({ type: ATTACH }, [channel.port2])
  return channel.port1
}
