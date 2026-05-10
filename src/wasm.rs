//! WASM bindings — exposes a single `IsheikaClient` class to JavaScript.
//!
//! Keep the API symmetric with the CLI: `discover()`, `fetch()`, `upload()`,
//! all returning Promises. The class holds a `PeerStore` in memory across calls
//! and lets the caller import/export it as JSON.

#![cfg(target_arch = "wasm32")]

use core::time::Duration;
use js_sys::Uint8Array;
use libp2p::Multiaddr;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;

use crate::client::{discover, fetch_bytes, upload_bytes};
use crate::doh::Doh;
use crate::peers::PeerStore;
use crate::signer::SwarmSigner;
use crate::transport::{Transport, TransportConfig};
use crate::DEFAULT_DOH_URL;

#[wasm_bindgen(start)]
pub fn _wasm_init() {
    console_error_panic_hook::set_once();
    let _ = tracing_wasm::try_set_as_global_default();
}

#[wasm_bindgen]
pub struct IsheikaClient {
    transport: Transport,
    peers: PeerStore,
    doh: Doh,
    signer_key: Option<String>,
    network_id: u64,
}

#[wasm_bindgen]
impl IsheikaClient {
    /// Construct a client. `private_key_hex` is optional — provide it only for upload.
    /// `network_id` defaults to `1` (mainnet). `doh_url` defaults to Cloudflare.
    #[wasm_bindgen(constructor)]
    pub fn new(
        private_key_hex: Option<String>,
        network_id: Option<u64>,
        doh_url: Option<String>,
        timeout_secs: Option<u32>,
    ) -> Result<IsheikaClient, JsError> {
        let network_id = network_id.unwrap_or(1);
        let doh_url = doh_url.unwrap_or_else(|| DEFAULT_DOH_URL.to_string());
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(30) as u64);

        let signer = match &private_key_hex {
            Some(hex) => SwarmSigner::from_hex(hex, network_id).map_err(into_js_err)?,
            None => SwarmSigner::random(network_id),
        };
        let cfg = TransportConfig { timeout, network_id };

        Ok(Self {
            transport: Transport::new(signer, cfg),
            peers: PeerStore::new(),
            doh: Doh::with_url(doh_url),
            signer_key: private_key_hex,
            network_id,
        })
    }

    /// Replace the in-memory peer store from a peers.json string.
    #[wasm_bindgen(js_name = "loadPeers")]
    pub fn load_peers(&mut self, peers_json: &str) -> Result<(), JsError> {
        let store: PeerStore = serde_json::from_str(peers_json).map_err(into_js_err)?;
        self.peers = store;
        Ok(())
    }

    /// Export the current peer store as a JSON string.
    #[wasm_bindgen(js_name = "exportPeers")]
    pub fn export_peers(&self) -> Result<String, JsError> {
        serde_json::to_string_pretty(&self.peers).map_err(into_js_err)
    }

    /// Number of peers currently held in memory.
    #[wasm_bindgen(js_name = "peerCount")]
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Discover peers from a bootstrap multiaddr (or `/dnsaddr/...`).
    /// Wait `wait_secs` for hive announcements per dialed underlay.
    /// Discovered peers are merged into the in-memory peer store and the
    /// new total count is returned.
    pub async fn discover(&mut self, bootstrap: String, wait_secs: u32) -> Result<usize, JsError> {
        let bootstrap_ma: Multiaddr = bootstrap.parse().map_err(|e: libp2p::multiaddr::Error| into_js_err(e))?;
        let discovered = discover(
            &self.transport,
            &self.doh,
            &bootstrap_ma,
            Duration::from_secs(wait_secs as u64),
        )
        .await
        .map_err(into_js_err)?;
        for p in discovered {
            self.peers.upsert(p);
        }
        Ok(self.peers.len())
    }

    /// Fetch content. `root_hex` is a 32-byte content address in hex.
    pub async fn fetch(&self, root_hex: String, max_retries: usize) -> Result<Uint8Array, JsError> {
        let bytes = fetch_bytes(&self.transport, &self.peers, &root_hex, max_retries)
            .await
            .map_err(into_js_err)?;
        Ok(Uint8Array::from(bytes.as_slice()))
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
        let root = upload_bytes(
            &self.transport,
            &self.peers,
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

fn into_js_err<E: core::fmt::Display>(e: E) -> JsError {
    JsError::new(&e.to_string())
}
