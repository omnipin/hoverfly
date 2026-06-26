// Unpack an uploaded tar archive into a Swarm collection, using nanotar.
//
// We parse tar in JS (not wasm) because hoverfly's `tar` crate is gated behind
// the `cli` feature and isn't in the wasm build; the wasm `uploadCollection`
// binding takes the unpacked entry list. Selection semantics match the CLI's
// `read_tar_files` (src/bin/hoverfly.rs) / bee's `pkg/api/dirs.go`: regular
// files only, `./` prefix stripped, empty/`.` paths skipped.

import { parseTar, parseTarGzip } from 'nanotar'

export interface CollectionEntry {
  path: string
  data: Uint8Array
  contentType?: string
}

function toEntries (items: Array<{ name: string, type?: string, data?: Uint8Array }>): CollectionEntry[] {
  const out: CollectionEntry[] = []
  for (const it of items) {
    if (it.type !== undefined && it.type !== 'file' && it.type !== 'contiguousFile') continue
    if (it.data == null) continue
    const path = it.name.replace(/^\.\//, '')
    if (path === '' || path === '.') continue
    out.push({ path, data: it.data, contentType: guessContentType(path) })
  }
  return out
}

/** Parse a (optionally gzipped) tar archive into regular-file entries. */
export async function readTar (bytes: Uint8Array, gzipped: boolean): Promise<CollectionEntry[]> {
  const items = gzipped ? await parseTarGzip(bytes) : parseTar(bytes)
  const entries = toEntries(items)
  if (entries.length === 0) throw new Error('archive contained no regular files')
  return entries
}

const MIME: Record<string, string> = {
  html: 'text/html', htm: 'text/html', css: 'text/css', js: 'text/javascript',
  mjs: 'text/javascript', json: 'application/json', svg: 'image/svg+xml',
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif',
  webp: 'image/webp', avif: 'image/avif', ico: 'image/x-icon', txt: 'text/plain',
  xml: 'application/xml', pdf: 'application/pdf', wasm: 'application/wasm',
  woff: 'font/woff', woff2: 'font/woff2', ttf: 'font/ttf', otf: 'font/otf',
  mp4: 'video/mp4', webm: 'video/webm', mp3: 'audio/mpeg', wav: 'audio/wav',
  md: 'text/markdown'
}

/** Guess a content-type from a filename extension. */
export function guessContentType (path: string): string | undefined {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  return MIME[ext]
}

/** Classify an uploaded file: plain tar, gzipped tar, or a single file. */
export function classifyArchive (name: string, type: string): 'tar' | 'tgz' | 'file' {
  if (type === 'application/x-tar' || type === 'application/tar' || /\.tar$/i.test(name)) return 'tar'
  if (type === 'application/gzip' || /\.tar\.gz$/i.test(name) || /\.tgz$/i.test(name)) return 'tgz'
  return 'file'
}
