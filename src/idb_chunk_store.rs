//! Persistent, content-addressed chunk cache backed by IndexedDB (browser only).
//!
//! Swarm chunks are immutable — addressed by the BMT hash of their content — so
//! once retrieved they can be cached forever and shared across fetches, sites
//! and sessions. hoverfly's in-memory chunk cache ([`crate::client::NetworkedStore`])
//! is per-fetch by design (so a long-lived daemon's RAM doesn't grow without
//! bound); this module adds an L2 that survives reloads.
//!
//! ## Threading
//!
//! This build is NOT single-threaded: `nectar-primitives` pulls in
//! `wasm-bindgen-rayon` and the daemon calls `initThreadPool`, so retrieval
//! futures are polled across several Web Worker threads. The `indexed-db`
//! handle (`Rc<Database>`) is `!Send` and thread-affine — it can only be used
//! on the thread that opened it — so the store handle CANNOT be a single
//! process-global value shared across threads.
//!
//! An earlier version kept the handle in a `thread_local` and assumed wasm was
//! single-threaded ("process-global cache"). Under the real thread pool that
//! was a silent bug: `enableChunkStore` installed the handle on the main
//! worker thread, but the write-back/read paths run on rayon worker threads
//! where the `thread_local` was still empty — so every chunk write was skipped
//! and the L2 cache stayed empty forever.
//!
//! Fix: store only the database NAME in a process-global `static` (a `String`
//! is `Send + Sync`), and lazily open + cache a per-thread `Database` handle on
//! first use via a `thread_local`. IndexedDB permits multiple concurrent
//! connections to the same database, so each worker thread holding its own
//! handle to `hoverfly-gw-chunks` is fine — writes from any thread land in the
//! same on-disk store. This keeps `NetworkedStore`/`RetrievalCache` unchanged,
//! so nectar's `ChunkGet` bounds are unaffected.
//!
//! We use the `indexed-db` crate specifically because it is the only IndexedDB
//! binding that behaves under wasm-bindgen's multi-threaded futures executor,
//! which this `+atomics` / wasm-bindgen-rayon build uses.

#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;

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
        // Self-heal a storeless DB. A previous bug let the landing page's
        // versionless `open()` commit an empty version-1 database before the
        // daemon ran its store-creating v1 upgrade; since both open at v1, the
        // upgrade never fired and the `chunks` store was permanently absent.
        // The landing page now aborts that accidental create, but an already-
        // poisoned DB won't fix itself: if the store is missing, delete the DB
        // and recreate it from scratch (it only ever held re-fetchable,
        // content-addressed chunks, so dropping it is harmless).
        if db.object_store_names().iter().any(|n| n == STORE) {
            return Ok(Self { db: Rc::new(db) });
        }
        db.close();
        factory
            .delete_database(name)
            .await
            .map_err(|e| format!("idb delete (storeless): {e}"))?;
        let db = factory
            .open(name, 1, |evt| async move {
                evt.database().build_object_store(STORE).create()?;
                Ok(())
            })
            .await
            .map_err(|e| format!("idb reopen: {e}"))?;
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

/// Process-global name of the configured chunk database. `String` is
/// `Send + Sync`, so unlike the thread-affine `Database` handle this can be a
/// real `static` shared across every rayon worker thread. `None` until
/// [`set_store_name`] is called by `enableChunkStore`.
static CHUNK_DB_NAME: Mutex<Option<String>> = Mutex::new(None);

thread_local! {
    /// Per-thread, lazily-opened handle to the chunk database named by
    /// [`CHUNK_DB_NAME`]. Each rayon worker thread opens its own connection on
    /// first use (IndexedDB allows concurrent connections to the same DB), so
    /// the thread-affine `Rc<Database>` is never shared across threads.
    static CHUNK_STORE: RefCell<Option<IdbChunkStore>> = const { RefCell::new(None) };
}

/// Record the configured chunk database name. Called once by
/// `HoverflyClient::enableChunkStore` after it has verified the DB opens. The
/// actual per-thread handles are opened lazily by [`get_store`].
pub fn set_store_name(name: String) {
    if let Ok(mut g) = CHUNK_DB_NAME.lock() {
        *g = Some(name);
    }
}

/// Get this thread's chunk-store handle, opening (and caching) it on first use.
///
/// Returns `None` if no database has been configured ([`set_store_name`] not
/// called) or if opening the per-thread connection fails. Safe to call from any
/// worker thread: the returned handle is owned by — and only used on — the
/// calling thread. The borrow on the `thread_local` is released before any
/// `.await`, so callers may freely await the returned store's methods.
pub async fn get_store() -> Option<IdbChunkStore> {
    // Fast path: this thread already has an open handle.
    if let Some(store) = CHUNK_STORE.with(|c| c.borrow().clone()) {
        return Some(store);
    }
    // No handle yet on this thread — is a DB even configured?
    let name = CHUNK_DB_NAME.lock().ok()?.clone()?;
    // Open a fresh connection for this thread. Awaited with the borrow already
    // released (we only re-borrow to store the result below).
    let store = IdbChunkStore::open(&name).await.ok()?;
    CHUNK_STORE.with(|c| *c.borrow_mut() = Some(store.clone()));
    Some(store)
}

/// Count of chunks served from the persistent L2 cache (across all fetches).
/// Exposed via `HoverflyClient::chunkStoreHits` for diagnostics — a non-zero
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
