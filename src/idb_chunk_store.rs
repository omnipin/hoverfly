//! Persistent, content-addressed chunk cache backed by IndexedDB (browser only).
//!
//! Swarm chunks are immutable — addressed by the BMT hash of their content — so
//! once retrieved they can be cached forever and shared across fetches, sites
//! and sessions. isheika's in-memory chunk cache ([`crate::client::NetworkedStore`])
//! is per-fetch by design (so a long-lived daemon's RAM doesn't grow without
//! bound); this module adds an L2 that survives reloads.
//!
//! The store is held in a `thread_local` rather than threaded through the
//! retrieval structs: wasm is single-threaded, so it acts as a process-global
//! cache, which is exactly right for content-addressed data (a chunk is the
//! same chunk no matter which fetch or which `IsheikaClient` asked for it). It
//! also keeps `NetworkedStore`/`RetrievalCache` unchanged, so nectar's
//! `ChunkGet` bounds are unaffected.
//!
//! We use the `indexed-db` crate specifically because it is the only IndexedDB
//! binding that behaves under wasm-bindgen's multi-threaded futures executor,
//! which this `+atomics` / wasm-bindgen-rayon build uses.

#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use indexed_db::{Database, Factory};
use wasm_bindgen::{JsCast, JsValue};

/// Object store name inside the IndexedDB database.
const STORE: &str = "chunks";

/// User error type flowing through `indexed-db` generics. We never raise our
/// own errors inside transactions, so the concrete type only needs to satisfy
/// the crate's bounds; `std::io::Error` matches the crate's own examples.
type IdbErr = std::io::Error;

/// A handle to the IndexedDB-backed chunk store. Cheap to clone (an `Rc` bump);
/// the underlying `IDBDatabase` stays open for the session.
#[derive(Clone)]
pub struct IdbChunkStore {
    db: Rc<Database<IdbErr>>,
}

impl IdbChunkStore {
    /// Open (creating if needed) the chunk database `name`, ensuring the
    /// `chunks` object store exists.
    pub async fn open(name: &str) -> Result<Self, String> {
        let factory = Factory::<IdbErr>::get().map_err(|e| format!("idb factory: {e}"))?;
        let db = factory
            .open(name, 1, |evt| async move {
                let db = evt.database();
                // Out-of-line keys: we supply the chunk address as the key on
                // every put/get, so no key path or auto-increment.
                db.build_object_store(STORE).create()?;
                Ok(())
            })
            .await
            .map_err(|e| format!("idb open: {e}"))?;
        Ok(Self { db: Rc::new(db) })
    }

    /// Fetch the stored bytes for `key_hex` (a chunk address in hex), if present.
    pub async fn get(&self, key_hex: String) -> Option<Vec<u8>> {
        let res = self
            .db
            .transaction(&[STORE])
            .run(move |t| async move {
                let store = t.object_store(STORE)?;
                store.get(&JsValue::from_str(&key_hex)).await
            })
            .await;
        match res {
            Ok(Some(js)) => js_to_vec(&js),
            _ => None,
        }
    }

    /// Store `bytes` under `key_hex`. Overwrites any existing value (chunks are
    /// immutable, so a repeat write is a harmless no-op in content terms).
    pub async fn put(&self, key_hex: String, bytes: Vec<u8>) {
        let _ = self
            .db
            .transaction(&[STORE])
            .rw()
            .run(move |t| async move {
                let store = t.object_store(STORE)?;
                // Copy into a fresh, non-shared JS buffer. Under the shared-
                // memory wasm build a `Uint8Array` view over wasm memory is
                // SharedArrayBuffer-backed; copying sidesteps any structured-
                // clone quirks with shared views (same reasoning as src/wsws).
                let arr = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
                arr.copy_from(&bytes);
                store
                    .put_kv(&JsValue::from_str(&key_hex), arr.as_ref())
                    .await
            })
            .await;
    }
}

/// Convert a stored IndexedDB value (a `Uint8Array`, or an `ArrayBuffer` on
/// engines that hand one back) into bytes.
fn js_to_vec(js: &JsValue) -> Option<Vec<u8>> {
    if let Some(arr) = js.dyn_ref::<js_sys::Uint8Array>() {
        return Some(arr.to_vec());
    }
    if let Some(buf) = js.dyn_ref::<js_sys::ArrayBuffer>() {
        return Some(js_sys::Uint8Array::new(buf).to_vec());
    }
    None
}

thread_local! {
    /// Process-global (single-threaded wasm) handle to the persistent chunk
    /// store. `None` until [`set_store`] is called.
    static CHUNK_STORE: RefCell<Option<IdbChunkStore>> = RefCell::new(None);
}

/// Install the persistent chunk store. Called once after opening the database.
pub fn set_store(store: IdbChunkStore) {
    CHUNK_STORE.with(|c| *c.borrow_mut() = Some(store));
}

/// Clone the installed chunk store, if any. The borrow is released before the
/// clone is returned, so callers may freely `.await` on the result.
pub fn get_store() -> Option<IdbChunkStore> {
    CHUNK_STORE.with(|c| c.borrow().clone())
}

/// Count of chunks served from the persistent L2 cache (across all fetches).
/// Exposed via `IsheikaClient::chunkStoreHits` for diagnostics — a non-zero
/// value on a cold in-memory cache proves chunks are coming from IndexedDB.
static L2_HITS: core::sync::atomic::AtomicU32 = core::sync::atomic::AtomicU32::new(0);

/// Record one L2 cache hit.
pub fn note_hit() {
    L2_HITS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
}

/// Total L2 cache hits so far.
pub fn hits() -> u32 {
    L2_HITS.load(core::sync::atomic::Ordering::Relaxed)
}
