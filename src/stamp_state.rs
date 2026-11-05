//! Per-batch postage slot state, persisted across uploads.
//!
//! A stamp names a slot `(bucket, index)` inside its batch. Bee's reserve keeps
//! one chunk per `(batch, slot)`: a chunk arriving with a slot that is already
//! taken and a newer stamp timestamp *replaces* the stored chunk, whatever the
//! batch type (`pkg/storer/internal/reserve/reserve.go`, `Reserve.Put`). Chunks
//! in one bucket share their top 16 address bits, so they land on the same
//! storers. An issuer that starts every upload at index 0 therefore evicts the
//! previous upload's chunks from every bucket both uploads touch.
//!
//! Bee avoids this by persisting its issuer (`pkg/postage/stampissuer.go`).
//! Here each upload *reserves* its slots before stamping: under an exclusive
//! file lock it reads the batch's per-bucket counters, adds the upload's
//! per-bucket chunk counts, checks capacity, and writes the result back. The
//! issuer then starts from the counters as they were before the reservation.
//! A crash after the reservation wastes slots but never reuses them, and two
//! processes uploading to one batch get disjoint ranges.
//!
//! Layout: one file per batch, `<dir>/<batch-hex>.slots`, holding a short
//! header and `2^bucket_depth` little-endian `u32` counters (256 KiB at the
//! standard bucket depth of 16).

use nectar_postage::BatchId;
use nectar_primitives::chunk::ChunkAddress;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const MAGIC: &[u8; 4] = b"HFSS";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum StampStateError {
    #[error("stamp state {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("stamp state {0}: corrupt or unsupported file")]
    Corrupt(PathBuf),
    #[error(
        "stamp state {path}: recorded at depth {recorded}, but --depth is {requested}; \
         pass the batch's real depth"
    )]
    DepthDecrease {
        path: PathBuf,
        recorded: u8,
        requested: u8,
    },
    #[error(
        "batch is full: bucket {bucket} has {used} of {capacity} slots used and this upload \
         needs {needed} more (dilute or top up the batch, or use another one)"
    )]
    BucketFull {
        bucket: u32,
        used: u32,
        needed: u32,
        capacity: u32,
    },
    #[error("invalid batch geometry: depth {depth}, bucket depth {bucket_depth}")]
    Geometry { depth: u8, bucket_depth: u8 },
}

static DIR: OnceLock<PathBuf> = OnceLock::new();

/// Override where slot state lives. First call wins; later calls are ignored
/// so a library caller can't move state out from under an upload in flight.
pub fn set_dir(dir: PathBuf) {
    let _ = DIR.set(dir);
}

/// The slot-state directory: [`set_dir`], else `$HOVERFLY_STAMP_DIR`, else
/// `$XDG_STATE_HOME/hoverfly/stamps`, else `~/.local/state/hoverfly/stamps`.
///
/// Deliberately not relative to the working directory: an upload run from a
/// different directory must still see the batch's earlier reservations.
pub fn dir() -> PathBuf {
    DIR.get_or_init(|| {
        // Unit tests stamp real uploads; keep them out of the user's state.
        if cfg!(test) {
            return std::env::temp_dir()
                .join(format!("hoverfly-test-stamps-{}", std::process::id()));
        }
        if let Some(d) = std::env::var_os("HOVERFLY_STAMP_DIR") {
            return PathBuf::from(d);
        }
        if let Some(d) = std::env::var_os("XDG_STATE_HOME") {
            return PathBuf::from(d).join("hoverfly").join("stamps");
        }
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home)
                .join(".local")
                .join("state")
                .join("hoverfly")
                .join("stamps");
        }
        PathBuf::from("hoverfly-stamps")
    })
    .clone()
}

/// Reserve slots for `addrs` in the default directory. See [`reserve_in`].
pub fn reserve<'a>(
    batch_id: &BatchId,
    depth: u8,
    bucket_depth: u8,
    ring: bool,
    addrs: impl IntoIterator<Item = &'a ChunkAddress>,
) -> Result<Vec<u32>, StampStateError> {
    reserve_in(&dir(), batch_id, depth, bucket_depth, ring, addrs)
}

/// Reserve one slot per address and return the per-bucket counters as they
/// were *before* the reservation, i.e. where this upload's issuer starts.
///
/// `ring` selects mutable-batch semantics: counters are cursors that wrap at
/// the bucket capacity instead of failing. A single upload may still not wrap
/// a bucket onto itself, because its own chunks would share one timestamp and
/// bee refuses an overwrite that isn't strictly newer.
pub fn reserve_in<'a>(
    dir: &Path,
    batch_id: &BatchId,
    depth: u8,
    bucket_depth: u8,
    ring: bool,
    addrs: impl IntoIterator<Item = &'a ChunkAddress>,
) -> Result<Vec<u32>, StampStateError> {
    if bucket_depth == 0 || bucket_depth > 24 || depth < bucket_depth || depth - bucket_depth > 31 {
        return Err(StampStateError::Geometry {
            depth,
            bucket_depth,
        });
    }
    let buckets = 1usize << bucket_depth;
    let capacity = 1u32 << (depth - bucket_depth);

    let mut needed = vec![0u32; buckets];
    for addr in addrs {
        let b = bucket_of(addr.as_bytes(), bucket_depth);
        needed[b] = needed[b].saturating_add(1);
    }

    let io = |path: &Path| {
        let path = path.to_path_buf();
        move |source| StampStateError::Io { path, source }
    };
    fs::create_dir_all(dir).map_err(io(dir))?;
    let stem = hex::encode(batch_id.as_slice());
    let path = dir.join(format!("{stem}.slots"));
    let lock_path = dir.join(format!("{stem}.lock"));

    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(io(&lock_path))?;
    lock.lock().map_err(io(&lock_path))?;

    let start = match read_counts(&path, bucket_depth)? {
        Some((recorded, counts)) => {
            if recorded > depth {
                return Err(StampStateError::DepthDecrease {
                    path,
                    recorded,
                    requested: depth,
                });
            }
            counts
        }
        None => vec![0u32; buckets],
    };

    let mut next = start.clone();
    for (bucket, (&have, &want)) in start.iter().zip(&needed).enumerate() {
        if want == 0 {
            continue;
        }
        let full = || StampStateError::BucketFull {
            bucket: bucket as u32,
            used: have.min(capacity),
            needed: want,
            capacity,
        };
        next[bucket] = if ring {
            if want > capacity {
                return Err(full());
            }
            let from = if have >= capacity { 0 } else { have };
            (from + want - 1) % capacity + 1
        } else {
            match have.checked_add(want) {
                Some(n) if n <= capacity => n,
                _ => return Err(full()),
            }
        };
    }

    write_counts(&path, depth, bucket_depth, &next)?;
    drop(lock);
    Ok(start)
}

/// Bucket = the top `bucket_depth` bits of the address (bee
/// `postage.ToBucket`).
fn bucket_of(addr: &[u8], bucket_depth: u8) -> usize {
    let top = u32::from_be_bytes([addr[0], addr[1], addr[2], addr[3]]);
    (top >> (32 - u32::from(bucket_depth))) as usize
}

fn read_counts(path: &Path, bucket_depth: u8) -> Result<Option<(u8, Vec<u32>)>, StampStateError> {
    let mut buf = Vec::new();
    match File::open(path) {
        Ok(mut f) => f
            .read_to_end(&mut buf)
            .map_err(|source| StampStateError::Io {
                path: path.to_path_buf(),
                source,
            })?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(StampStateError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let buckets = 1usize << bucket_depth;
    if buf.len() != HEADER_LEN + 4 * buckets
        || &buf[..4] != MAGIC
        || buf[4] != VERSION
        || buf[6] != bucket_depth
    {
        return Err(StampStateError::Corrupt(path.to_path_buf()));
    }
    let depth = buf[5];
    let counts = buf[HEADER_LEN..]
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    Ok(Some((depth, counts)))
}

fn write_counts(
    path: &Path,
    depth: u8,
    bucket_depth: u8,
    counts: &[u32],
) -> Result<(), StampStateError> {
    let io = |p: &Path| {
        let p = p.to_path_buf();
        move |source| StampStateError::Io { path: p, source }
    };
    let mut buf = Vec::with_capacity(HEADER_LEN + 4 * counts.len());
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&[VERSION, depth, bucket_depth, 0]);
    for c in counts {
        buf.extend_from_slice(&c.to_le_bytes());
    }
    let tmp = path.with_extension(format!("slots.tmp.{}", std::process::id()));
    {
        let mut f = File::create(&tmp).map_err(io(&tmp))?;
        f.write_all(&buf).map_err(io(&tmp))?;
        f.sync_all().map_err(io(&tmp))?;
    }
    fs::rename(&tmp, path).map_err(io(path))?;
    if let Some(parent) = path.parent() {
        // Persist the rename itself; best effort where directories can't be
        // opened for sync.
        if let Ok(d) = File::open(parent) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "hoverfly-stamp-state-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn addr(top: u16, salt: u8) -> ChunkAddress {
        let mut b = [salt; 32];
        b[..2].copy_from_slice(&top.to_be_bytes());
        ChunkAddress::new(b)
    }

    const BATCH: [u8; 32] = [7u8; 32];

    #[test]
    fn second_upload_starts_after_the_first() {
        let dir = tmpdir("seq");
        let batch = BatchId::from(BATCH);
        let first = [addr(5, 1), addr(5, 2), addr(9, 3)];
        let second = [addr(5, 4), addr(9, 5), addr(9, 6)];

        let s1 = reserve_in(&dir, &batch, 20, 16, false, first.iter()).unwrap();
        assert_eq!((s1[5], s1[9]), (0, 0));
        let s2 = reserve_in(&dir, &batch, 20, 16, false, second.iter()).unwrap();
        assert_eq!((s2[5], s2[9]), (2, 1));
        let s3 = reserve_in(&dir, &batch, 20, 16, false, std::iter::empty()).unwrap();
        assert_eq!((s3[5], s3[9]), (3, 3));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn full_bucket_fails_before_anything_is_stamped() {
        let dir = tmpdir("full");
        let batch = BatchId::from(BATCH);
        // depth 17 / bucket depth 16 => 2 slots per bucket.
        let three = [addr(1, 1), addr(1, 2), addr(1, 3)];
        let err = reserve_in(&dir, &batch, 17, 16, false, three.iter()).unwrap_err();
        assert!(
            matches!(err, StampStateError::BucketFull { bucket: 1, .. }),
            "{err}"
        );
        // The failed reservation must not have consumed anything.
        let two = [addr(1, 1), addr(1, 2)];
        assert_eq!(
            reserve_in(&dir, &batch, 17, 16, false, two.iter()).unwrap()[1],
            0
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ring_wraps_across_uploads_but_not_within_one() {
        let dir = tmpdir("ring");
        let batch = BatchId::from(BATCH);
        let two = [addr(3, 1), addr(3, 2)];
        assert_eq!(
            reserve_in(&dir, &batch, 17, 16, true, two.iter()).unwrap()[3],
            0
        );
        // Cursor is now at capacity (2); the next upload wraps to slot 0.
        assert_eq!(
            reserve_in(&dir, &batch, 17, 16, true, two[..1].iter()).unwrap()[3],
            2
        );
        let three = [addr(3, 1), addr(3, 2), addr(3, 3)];
        assert!(reserve_in(&dir, &batch, 17, 16, true, three.iter()).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dilution_keeps_counters_and_shrinking_depth_is_refused() {
        let dir = tmpdir("dilute");
        let batch = BatchId::from(BATCH);
        let a = [addr(2, 1)];
        reserve_in(&dir, &batch, 18, 16, false, a.iter()).unwrap();
        assert_eq!(
            reserve_in(&dir, &batch, 20, 16, false, a.iter()).unwrap()[2],
            1
        );
        assert!(matches!(
            reserve_in(&dir, &batch, 18, 16, false, a.iter()),
            Err(StampStateError::DepthDecrease { .. })
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_reservations_get_disjoint_ranges() {
        let dir = tmpdir("concurrent");
        let batch = BatchId::from(BATCH);
        let starts: Vec<u32> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8u8)
                .map(|i| {
                    let dir = dir.clone();
                    s.spawn(move || {
                        let addrs: Vec<ChunkAddress> =
                            (0..5).map(|j| addr(4, i * 10 + j)).collect();
                        reserve_in(&dir, &batch, 22, 16, false, addrs.iter()).unwrap()[4]
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut sorted = starts.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..8).map(|i| i * 5).collect::<Vec<u32>>(),
            "{starts:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bucket_matches_nectar() {
        for top in [0u16, 1, 0x00ff, 0x8000, 0xffff] {
            let a = addr(top, 0xab);
            assert_eq!(
                bucket_of(a.as_bytes(), 16) as u32,
                nectar_postage::calculate_bucket(&a, 16)
            );
        }
    }
}
