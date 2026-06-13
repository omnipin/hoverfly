# @hoverfly/upload

Prototype in-browser Swarm upload dApp. Connect a wallet, buy a postage batch,
pick a file, upload it via an embedded hoverfly wasm node. Foreground-only (no
SharedWorker), unlike `apps/gateway`.

```
pnpm install
pnpm build:wasm          # build the no-shared-memory hoverfly wasm → apps/upload/pkg/
pnpm start               # → http://localhost:3100
```

Needs a wallet on Gnosis with xBZZ (storage) and xDAI (gas).

## No shared memory (runs on the ENS gateway)

The dApp builds hoverfly **threadless** (`build-wasm.sh`: `--no-default-features`,
no `wasm-threads`, empty `RUSTFLAGS` to drop the repo's atomics/`--shared-memory`
flags). The result uses a plain non-shared linear memory, so it needs **no
`SharedArrayBuffer` and no COOP/COEP cross-origin isolation** — which is what
lets it be hosted on the **eth.limo / eth.link ENS gateway** (those only send
`Cross-Origin-Resource-Policy`, never COOP/COEP, so a shared-memory wasm can't
load there). Hashing is single-threaded, but still runs off the main thread in a
Worker, so the UI stays responsive.

This relies on the omnipin **nectar fork**, which gates `wasm-bindgen-rayon`
behind a default-off `wasm-threads` feature (the gateway opts back in). The
upload wasm lives in `apps/upload/pkg/` (separate from the gateway's threaded
`pkg/`).

## Flow

1. Connect wallet (EIP-1193); switch to Gnosis.
2. Choose a file → quote a batch (depth + cost; math mirrors `src/batch.rs`).
3. Buy: `BZZ.approve` + `createBatch`. Only wallet interaction (1–2 sigs).
4. Upload: wasm splits/stamps/pushes, returns a `bzz` reference.

## Session key (why uploading needs no wallet popups)

A bee stamp signature is `personal_sign` of a 32-byte digest, needed once per
chunk — thousands per file. Instead of prompting the wallet per chunk, the dApp
uses a separate **session key** as the batch `owner` in `createBatch` (which
takes an explicit owner); bee validates each stamp against that owner, so the
session key signs all stamps locally with no prompts. It holds no funds and only
stamps chunks for batches it owns.

The session key is **derived from a one-time wallet signature**
(`keccak256(wallet.personal_sign(fixed message))`). ECDSA `personal_sign` is
deterministic per key+message, so the same wallet reproduces the same session
key — and therefore owns the same batches — on **any device**, making batches
recoverable just by reconnecting (still cached per wallet to avoid re-prompting).
It's one-way (no wallet key recoverable from it), scoped (a leak only burns that
batch's storage), and signs a fixed human-readable message for domain
separation. A `Rotate` action swaps to a random key if ever needed.

This is deliberately the no-account-abstraction route. The signer is isolated in
`session-key.ts` + `wallet.ts` if a 7702/7579/4337 variant is wanted later.

## Layout

| file | role |
|------|------|
| `src/app.ts` | UI + orchestration |
| `src/wallet.ts` | viem: connect, quote, approve + `createBatch(owner)` |
| `src/session-key.ts` | in-browser batch-owner key |
| `src/hoverfly.ts` | load wasm, construct client, discover, `upload()` |
| `src/batches.ts` | localStorage record of bought batches |
| `build.js` / `serve.js` | esbuild + vendored `pkg/` wasm; serve with COOP/COEP |

The build vendors the wasm from `pkg/` — build it first (see `apps/gateway`).

## Manifests

Uploads are wrapped in a mantaray manifest so the `bzz` reference is usable:

- A single file → one-entry manifest with its filename + content-type.
- A `.tar` / `.tar.gz` → a Swarm collection (multi-entry manifest), unpacked in
  the browser with [`nanotar`](https://github.com/unjs/nanotar). A top-level
  `index.html` is set as the website index document, so the root serves as a
  browsable site.

These use the wasm `uploadFile` / `uploadCollection` bindings added to
`src/wasm.rs` (wrapping `client::upload_file_with_manifest_ex` /
`upload_collection`).

## Batch discovery

The stock Swarm PostageStamp contract has no "list batches for owner X" getter
(`BatchCreated` doesn't index `owner`, and `batches(id)` is per-id). Discovery
here is three layers:

1. **localStorage** — this browser remembers the batch IDs it created.
2. **Swarmscan auto-discovery** (`src/swarmscan.ts`) — Swarmscan indexes
   PostageStamp events (it's the data source `batch-explorer.github.io` uses).
   There's no owner-filtered query, but the `batch-created` feed is
   reverse-chronological and includes `data.owner`, so we scan a few recent
   pages and keep the ones owned by the session key. Since the session key is
   freshly minted, its batches sit near the top of the feed — this recovers
   batches created in another browser / after cleared storage, with no manual
   step.
3. **Import-by-ID** — fallback for older batches beyond the scan window: paste
   an ID; accepted only if the chain says the session key owns it.

Whatever the source, every candidate is then **verified on-chain**
(`batches(id)`: exists, owned by the session key, live balance) before it's
offered for reuse; stale/expired/foreign entries are pruned.

(Beeport lists batches differently — it routes creation through its *own*
registry contract that records `ownerBatches`. This dApp calls the stock
contract directly, so it relies on the indexer + on-chain verification instead.)

## Limitations

- Browser peers are `/ws[s]`-only (most mainnet bees are TCP), so the dialable
  set is sparse and uploads can need retries.
