// Build the upload dApp: bundle the TS entry with esbuild, copy static files
// from public/, and vendor the hoverfly wasm package from the repo's pkg/
// (built via `cargo build --target wasm32-... && wasm-bindgen`).
//
// Unlike the gateway, the upload dApp runs hoverfly entirely in the FOREGROUND
// (the page itself, not a SharedWorker) — uploading is a one-shot interactive
// action, so there's no need for a shared long-lived daemon. The page must
// still be cross-origin isolated (the wasm uses shared memory for the rayon
// hashing pool); serve.js sets the COOP/COEP headers.

import * as esbuild from 'esbuild'
import { copyFileSync, cpSync, existsSync, mkdirSync, rmSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const dist = resolve(here, 'dist')
const assets = resolve(dist, '__up__')
// The upload dApp uses its OWN wasm build — the no-shared-memory variant built
// by build-wasm.sh into apps/upload/pkg/ (NOT the repo-root ../../pkg, which is
// the gateway's threaded/shared-memory build). This is what lets the dApp run
// on the eth.limo ENS gateway, which can't set COOP/COEP.
const pkg = resolve(here, 'pkg')
const peersSeed = resolve(here, '../../peers.ws.json')
const watch = process.argv.includes('--watch')

const entryPoints = {
  app: resolve(here, 'src/app.ts'),
  // The hoverfly node Worker (own bundle so it can be `new Worker(worker.js)`).
  worker: resolve(here, 'src/worker.ts')
}

function copyStatic () {
  cpSync(resolve(here, 'public'), dist, { recursive: true })
  // Vendor the cold-start peer seed (browser-dialable /ws[s] peers harvested
  // from mainnet; the repo's refresh-peers workflow keeps it fresh). Bundled as
  // a fallback for when the CDN copy is unreachable — see config.PEERS_SEED_URL.
  if (existsSync(peersSeed)) {
    copyFileSync(peersSeed, resolve(assets, 'peers.ws.json'))
  } else {
    console.warn(`\n  ⚠  ${peersSeed} not found — upload will discover peers cold.\n`)
  }
  if (existsSync(pkg)) {
    cpSync(pkg, resolve(assets, 'hoverfly'), { recursive: true })
  } else {
    console.warn(`\n  ⚠  ${pkg} not found — build the no-shared-memory wasm first:\n` +
      '     ./build-wasm.sh   (from apps/upload/)\n')
  }
}

/** @type {import('esbuild').BuildOptions} */
const options = {
  entryPoints,
  outdir: assets,
  entryNames: '[name]',
  bundle: true,
  format: 'esm',
  splitting: false,
  sourcemap: true,
  target: ['chrome120'],
  logLevel: 'info',
  define: {
    'process.env.NODE_ENV': '"production"'
  }
}

rmSync(dist, { recursive: true, force: true })
mkdirSync(assets, { recursive: true })
copyStatic()

if (watch) {
  const ctx = await esbuild.context(options)
  await ctx.watch()
  console.log('esbuild watching for changes…')
} else {
  await esbuild.build(options)
  console.log('built →', dist)
}
