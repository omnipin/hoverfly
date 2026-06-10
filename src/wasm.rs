//! WASM bindings — exposes a single `HoverflyClient` class to JavaScript.
//!
//! Keep the API symmetric with the CLI: `discover()`, `fetch()`, `upload()`,
//! all returning Promises. The class holds a `PeerStore` in memory across calls
//! and lets the caller import/export it as JSON.

#![cfg(target_arch = "wasm32")]

use core::time::Duration;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use futures_timer::Delay;
use js_sys::Uint8Array;
use libp2p::Multiaddr;
use wasm_bindgen::prelude::*;

use crate::DEFAULT_DOH_URL;
use crate::client::{
    RetrievalCache, discover, fetch_bytes_cached_ex, fetch_manifest_path_cached_ex,
    list_manifest_ex, upload_bytes,
};
use crate::doh::Doh;
use crate::peers::PeerStore;
use crate::signer::SwarmSigner;
use crate::transport::{Transport, TransportConfig};

/// Per-chunk peer racing for browser fetches. Mirrors the CLI default
/// (`client::DEFAULT_FETCH_CONCURRENCY`).
const FETCH_CONCURRENCY: usize = 5;

#[wasm_bindgen(start)]
pub fn _wasm_init() {
    console_error_panic_hook::set_once();
    // Route `tracing` events to the browser console. The default config maps
    // some levels to `console.debug` (hidden by Chrome's default log level),
    // and disables span timing noise. Pin a config that:
    //   - emits each event at its own console level (warn -> console.warn etc.)
    //     so retrieval `warn!`/`info!` are visible without enabling Verbose, and
    //   - keeps the `target` (e.g. `hoverfly::fetch`) so events are filterable.
    let mut builder = tracing_wasm::WASMLayerConfigBuilder::new();
    builder.set_max_level(tracing::Level::INFO);
    builder.set_report_logs_in_timings(false);
    tracing_wasm::set_as_global_default_with_config(builder.build());
}

#[wasm_bindgen]
pub struct HoverflyClient {
    /// libp2p transport, shared (`Arc`) with the background daemon loop so
    /// foreground fetches and background discovery dial through the same
    /// identity + per-peer dial cooldown.
    transport: Arc<Transport>,
    /// Candidate peer list behind shared interior mutability. The background
    /// daemon loop (see [`HoverflyClient::start`]) merges freshly-discovered
    /// peers in; every fetch snapshots it under a short borrow — never held
    /// across an `.await` — and retrieves against that snapshot. `Rc<RefCell>`
    /// is sound because wasm is single-threaded.
    peers: Rc<RefCell<PeerStore>>,
    doh: Arc<Doh>,
    signer_key: Option<String>,
    network_id: u64,
    /// Shared session cache + peer scoreboard. A long-lived (daemon-style)
    /// client reuses this across every fetch so warm sessions and learned
    /// peer scores carry over — the first request pays discovery, later ones
    /// reuse live forwarders.
    cache: RetrievalCache,
    /// `true` once the background daemon loop has been spawned. Guards
    /// against spawning more than one loop on repeated `start()` calls.
    running: Rc<RefCell<bool>>,
    /// Count of foreground fetches currently in flight. The background
    /// discovery loop skips its round while this is non-zero: on the browser's
    /// single ws+yamux connection driver, a discovery round's dial/substream
    /// churn resets in-flight retrieval substreams (observed as
    /// `retrieval: unexpected end of file` / `ConnectionReset: Canceled`),
    /// stalling exactly the cold-load burst (e.g. a 40-module site) it collides
    /// with. Pausing discovery while fetches run keeps the connections quiet for
    /// retrieval; discovery resumes once the burst drains. `Rc<Cell>` is sound
    /// on single-threaded wasm.
    inflight_fetches: Rc<Cell<usize>>,
}

/// RAII guard: increments the shared in-flight fetch counter on creation and
/// decrements on drop, so the background discovery loop can tell when any
/// foreground fetch is active (and pause). Drop runs on every exit path —
/// success, error, or early return — so the count can't leak.
struct FetchGuard(Rc<Cell<usize>>);
impl FetchGuard {
    fn new(c: &Rc<Cell<usize>>) -> Self {
        c.set(c.get() + 1);
        FetchGuard(c.clone())
    }
}
impl Drop for FetchGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

#[wasm_bindgen]
impl HoverflyClient {
    /// Construct a client. `private_key_hex` is optional — provide it only for upload.
    /// `network_id` defaults to `1` (mainnet). `doh_url` defaults to Cloudflare.
    ///
    /// `nonce_hex` is the optional 32-byte overlay nonce. The Swarm overlay is
    /// `keccak256(eth_addr || network_id || nonce)`, so to keep a *stable*
    /// overlay across restarts a caller must persist and replay BOTH the key and
    /// the nonce — persisting the key alone still yields a fresh random overlay
    /// each launch (see `SwarmSigner::from_hex`), which defeats peers' kademlia
    /// memory of this node. A long-lived browser daemon passes both to reuse one
    /// identity across page loads.
    #[wasm_bindgen(constructor)]
    pub fn new(
        private_key_hex: Option<String>,
        network_id: Option<u64>,
        doh_url: Option<String>,
        timeout_secs: Option<u32>,
        nonce_hex: Option<String>,
    ) -> Result<HoverflyClient, JsError> {
        let network_id = network_id.unwrap_or(1);
        let doh_url = doh_url.unwrap_or_else(|| DEFAULT_DOH_URL.to_string());
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(30) as u64);

        // key + nonce -> stable overlay (persisted, reusable daemon identity)
        // key only    -> stable eth identity, fresh random overlay nonce
        // neither      -> fully ephemeral (random key + nonce)
        let signer = match (&private_key_hex, &nonce_hex) {
            (Some(key), Some(nonce)) => {
                SwarmSigner::from_hex_with_nonce(key, nonce, network_id).map_err(into_js_err)?
            }
            (Some(key), None) => SwarmSigner::from_hex(key, network_id).map_err(into_js_err)?,
            (None, _) => SwarmSigner::random(network_id),
        };
        let cfg = TransportConfig {
            timeout,
            // Browsers need a far larger dial budget than native: a single dial
            // covers DNS + the browser's own TLS + WS upgrade to the AutoTLS
            // `wss://<sni>.libp2p.direct` endpoint, then Noise + Yamux + identify
            // + bee's handshake/pricing dance — all over a ~100ms+ RTT WebSocket.
            // The native default (3s, tuned for raw TCP) expires mid-chain in the
            // browser and surfaces as `peer failed: timeout` for every peer even
            // though the endpoint is reachable and its cert is valid.
            dial_timeout: Duration::from_secs(20),
            network_id,
            advertise: None,
            max_concurrent_substream_upgrades:
                crate::protocols::stream_pool::DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES,
        };

        Ok(Self {
            transport: Arc::new(Transport::new(signer, cfg)),
            peers: Rc::new(RefCell::new(PeerStore::new())),
            doh: Arc::new(Doh::with_url(doh_url)),
            signer_key: private_key_hex,
            network_id,
            cache: RetrievalCache::new(),
            running: Rc::new(RefCell::new(false)),
            inflight_fetches: Rc::new(Cell::new(0)),
        })
    }

    /// Replace the in-memory peer store from a peers.json string.
    #[wasm_bindgen(js_name = "loadPeers")]
    pub fn load_peers(&self, peers_json: &str) -> Result<(), JsError> {
        let store: PeerStore = serde_json::from_str(peers_json).map_err(into_js_err)?;
        *self.peers.borrow_mut() = store;
        Ok(())
    }

    /// Export the current peer store as a JSON string.
    #[wasm_bindgen(js_name = "exportPeers")]
    pub fn export_peers(&self) -> Result<String, JsError> {
        serde_json::to_string_pretty(&*self.peers.borrow()).map_err(into_js_err)
    }

    /// Number of peers currently held in memory.
    #[wasm_bindgen(js_name = "peerCount")]
    pub fn peer_count(&self) -> usize {
        self.peers.borrow().len()
    }

    /// Start the in-browser daemon: a long-lived background task that keeps
    /// the peer set warm. It runs one discovery round immediately — so the
    /// first fetch already has fresh, browser-dialable peers to race — then
    /// re-discovers every `interval_secs` for as long as the client lives.
    ///
    /// This is the in-browser analogue of the native unix-socket daemon's
    /// eager pool-fill + maintenance loop (`src/daemon.rs`): rather than the
    /// caller orchestrating discrete `discover()` then `fetch()` steps, the
    /// node maintains its own connectivity and `fetchManifestPath` / `fetch`
    /// simply talk to the running daemon. Because every method takes `&self`,
    /// fetches run concurrently with the background loop with no locking.
    ///
    /// Idempotent: a second call refreshes peers but does not spawn a second
    /// loop. Returns the peer count after the initial round.
    pub async fn start(
        &self,
        bootstrap: String,
        interval_secs: u32,
        wait_secs: u32,
    ) -> Result<usize, JsError> {
        let bootstrap_ma: Multiaddr = bootstrap
            .parse()
            .map_err(|e: libp2p::multiaddr::Error| into_js_err(e))?;

        // Eager initial fill — the first fetch should not start cold.
        merge_discovered(
            &self.transport,
            &self.doh,
            &self.peers,
            &bootstrap_ma,
            Duration::from_secs(wait_secs as u64),
        )
        .await;

        // Spawn the maintenance loop exactly once.
        let already_running = *self.running.borrow();
        if !already_running {
            *self.running.borrow_mut() = true;
            let transport = self.transport.clone();
            let doh = self.doh.clone();
            let peers = self.peers.clone();
            let running = self.running.clone();
            let inflight = self.inflight_fetches.clone();
            let bootstrap_ma = bootstrap_ma.clone();
            let interval = Duration::from_secs(interval_secs.max(1) as u64);
            let wait = Duration::from_secs(wait_secs as u64);
            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    // Sleep first: `start` already did the eager round above,
                    // so there's no need to discover again immediately.
                    Delay::new(interval).await;
                    let keep_running = *running.borrow();
                    if !keep_running {
                        break;
                    }
                    // Don't discover while a foreground fetch is in flight: on the
                    // browser's single ws+yamux driver, a discovery round resets
                    // in-flight retrieval substreams and stalls the fetch. If a
                    // fetch (or burst) is active, defer this round — re-check on a
                    // short cadence and run only once the connections go quiet, so
                    // discovery still happens between page loads but never mid-load.
                    let mut deferrals = 0u32;
                    while inflight.get() > 0 {
                        // Cap the wait so a wedged in-flight counter (shouldn't
                        // happen — FetchGuard's Drop always decrements) can't
                        // starve discovery forever; after ~interval of deferral
                        // proceed anyway.
                        if deferrals >= 10 {
                            break;
                        }
                        deferrals += 1;
                        Delay::new(Duration::from_secs(2)).await;
                    }
                    if !*running.borrow() {
                        break;
                    }
                    merge_discovered(&transport, &doh, &peers, &bootstrap_ma, wait).await;
                }
            });
        }

        Ok(self.peers.borrow().len())
    }

    /// Stop the background daemon loop. It exits after its current sleep.
    pub fn stop(&self) {
        *self.running.borrow_mut() = false;
    }

    /// Enable the persistent IndexedDB chunk cache (L2). Once enabled, every
    /// retrieved chunk is written back to IndexedDB and future fetches (this
    /// session or later) reuse stored chunks instead of hitting the network —
    /// immutable + content-addressed, so it's safe to keep indefinitely. This
    /// sits on top of the per-fetch in-memory cache. `db_name` is the
    /// IndexedDB database name to use.
    #[wasm_bindgen(js_name = "enableChunkStore")]
    pub async fn enable_chunk_store(&self, db_name: String) -> Result<(), JsError> {
        // Open once here to verify the database is usable (so a storage error
        // surfaces to the caller now, not silently on the first fetch). The
        // opened handle is bound to THIS thread; the retrieval paths run on
        // rayon worker threads and open their own per-thread handles lazily —
        // see `idb_chunk_store`'s threading note. So we only record the name.
        let _verify = crate::idb_chunk_store::IdbChunkStore::open(&db_name)
            .await
            .map_err(into_js_err)?;
        crate::idb_chunk_store::set_store_name(db_name);
        Ok(())
    }

    /// Number of chunks served from the persistent L2 (IndexedDB) cache since
    /// load. Non-zero with a cold in-memory cache means fetches are being
    /// served from IndexedDB rather than the network.
    #[wasm_bindgen(js_name = "chunkStoreHits")]
    pub fn chunk_store_hits(&self) -> u32 {
        crate::idb_chunk_store::hits()
    }

    /// Run a single discovery round now and merge the results into the
    /// in-memory peer store. The daemon's background loop does this
    /// automatically once [`HoverflyClient::start`] has been called; this is
    /// exposed for an explicit "refresh peers" affordance. Returns the new
    /// peer count.
    pub async fn discover(&self, bootstrap: String, wait_secs: u32) -> Result<usize, JsError> {
        let bootstrap_ma: Multiaddr = bootstrap
            .parse()
            .map_err(|e: libp2p::multiaddr::Error| into_js_err(e))?;
        merge_discovered(
            &self.transport,
            &self.doh,
            &self.peers,
            &bootstrap_ma,
            Duration::from_secs(wait_secs as u64),
        )
        .await;
        Ok(self.peers.borrow().len())
    }

    /// Fetch content. `root_hex` is a 32-byte content address in hex.
    /// Reassembles the BMT tree into the full byte stream.
    pub async fn fetch(&self, root_hex: String, max_retries: usize) -> Result<Uint8Array, JsError> {
        let _guard = FetchGuard::new(&self.inflight_fetches);
        let peers = self.peers.borrow().clone();
        let bytes = fetch_bytes_cached_ex(
            &self.transport,
            &peers,
            &root_hex,
            max_retries,
            FETCH_CONCURRENCY,
            &self.cache,
        )
        .await
        .map_err(into_js_err)?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    /// Resolve `path` through the mantaray manifest rooted at `root_hex` and
    /// fetch the resulting file's bytes + `Content-Type`. This is the gateway
    /// entry point: `fetchManifestPath("<root>", "index.html", 3)`. An empty
    /// `path` returns the manifest's root entry. Uses the shared retrieval
    /// cache so warm sessions/peer scores persist across requests.
    #[wasm_bindgen(js_name = "fetchManifestPath")]
    pub async fn fetch_manifest_path(
        &self,
        root_hex: String,
        path: String,
        max_retries: usize,
    ) -> Result<ManifestFetch, JsError> {
        let _guard = FetchGuard::new(&self.inflight_fetches);
        let peers = self.peers.borrow().clone();
        let (bytes, content_type) = fetch_manifest_path_cached_ex(
            &self.transport,
            &peers,
            &root_hex,
            &path,
            max_retries,
            FETCH_CONCURRENCY,
            &self.cache,
        )
        .await
        .map_err(into_js_err)?;
        Ok(ManifestFetch {
            bytes,
            content_type,
        })
    }

    /// List every entry in the mantaray manifest at `root_hex` as a JSON
    /// string: `[{"path","reference","contentType"}]`. Useful for directory
    /// index pages.
    #[wasm_bindgen(js_name = "listManifest")]
    pub async fn list_manifest(
        &self,
        root_hex: String,
        max_retries: usize,
    ) -> Result<String, JsError> {
        let peers = self.peers.borrow().clone();
        let entries = list_manifest_ex(
            &self.transport,
            &peers,
            &root_hex,
            max_retries,
            FETCH_CONCURRENCY,
        )
        .await
        .map_err(into_js_err)?;
        let json: Vec<serde_json::Value> = entries
            .iter()
            .map(|e| {
                serde_json::json!({
                    "path": e.path,
                    "reference": e.reference,
                    "contentType": e.content_type,
                })
            })
            .collect();
        serde_json::to_string(&json).map_err(into_js_err)
    }

    /// Upload bytes with an existing postage batch. Returns the root hash hex.
    pub async fn upload(
        &self,
        data: Uint8Array,
        batch_id_hex: String,
        depth: u8,
        max_retries: usize,
    ) -> Result<String, JsError> {
        let key = self
            .signer_key
            .as_deref()
            .ok_or_else(|| JsError::new("client constructed without a private key"))?;
        let signer = SwarmSigner::from_hex(key, self.network_id).map_err(into_js_err)?;
        let buf = data.to_vec();
        let peers = self.peers.borrow().clone();
        let root = upload_bytes(
            &self.transport,
            &peers,
            &signer,
            &batch_id_hex,
            depth,
            &buf,
            max_retries,
        )
        .await
        .map_err(into_js_err)?;
        Ok(hex::encode(root.as_bytes()))
    }
}

/// Result of a manifest path fetch: file bytes plus the optional
/// `Content-Type` recorded in the manifest entry's metadata.
#[wasm_bindgen]
pub struct ManifestFetch {
    bytes: Vec<u8>,
    content_type: Option<String>,
}

#[wasm_bindgen]
impl ManifestFetch {
    /// The resolved file's content bytes.
    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> Uint8Array {
        Uint8Array::from(self.bytes.as_slice())
    }

    /// The `Content-Type` from the manifest entry's metadata, if present.
    #[wasm_bindgen(getter, js_name = "contentType")]
    pub fn content_type(&self) -> Option<String> {
        self.content_type.clone()
    }
}

fn into_js_err<E: core::fmt::Display>(e: E) -> JsError {
    JsError::new(&e.to_string())
}

/// Run one discovery round against `bootstrap` and merge the freshly-found
/// peers into `peers`. Shared by the eager initial fill, the background
/// maintenance loop, and the manual `discover()` affordance.
///
/// Errors are swallowed (logged) — a failed round just means the next one
/// retries; the daemon must stay alive. The peer-store borrow is taken only
/// after the `discover` await resolves and is never held across an `.await`,
/// so it can never clash with a concurrent fetch's snapshot borrow.
async fn merge_discovered(
    transport: &Transport,
    doh: &Doh,
    peers: &Rc<RefCell<PeerStore>>,
    bootstrap: &Multiaddr,
    wait: Duration,
) {
    match discover(transport, doh, bootstrap, wait).await {
        Ok(found) => {
            let n = found.len();
            let mut store = peers.borrow_mut();
            for p in found {
                store.upsert(p);
            }
            tracing::info!(target: "hoverfly::wasm",
                "[daemon] discover round ok: found {} peer(s), store now {}", n, store.len());
        }
        Err(e) => {
            tracing::warn!(target: "hoverfly::wasm", "[daemon] discover round failed: {e}");
        }
    }
}
