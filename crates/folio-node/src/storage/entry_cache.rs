//! EntrySegmentCache and background flush loop.
//!
//! `EntrySegmentCache` is a two-tier write buffer:
//!
//!   hot  — new entries from `add_entry`, not yet written to the EntrySegment file.
//!   warm — entries written and synced to the active EntrySegment but not yet
//!          evicted.  They stay in memory until the segment seals so that reads
//!          never need to touch the active (still-growing) segment file.
//!
//! Read path (L0): `get` checks hot first, then warm.  As long as the segment is
//! active, every entry written in this session is in one of the two tiers.  Only
//! after `evict_warm()` (called atomically when the segment seals) do reads fall
//! through to the L1 entry_index + block cache against the now-immutable file.
//!
//! Flush cycle:
//!   1. Wait for hot to fill (flush_threshold bytes) or the periodic tick.
//!   2. `drain_hot_to_warm()` — atomically moves hot entries to warm and returns
//!      them.  The lock is held for this operation, so there is no window where
//!      an entry is in neither tier.
//!   3. Write the returned entries to the EntrySegment file (O_DIRECT, async).
//!   4. fdatasync.
//!   5. Insert each entry into entry_index; delete from wal_index (WAL GC).
//!   6. On segment seal: `evict_warm()` then open a new segment.
//!
//! Thread model: flush loop runs on its own OS thread with a dedicated
//! `tokio_uring` runtime.  The caller communicates via:
//!   - `Arc<Mutex<EntrySegmentCache>>` for inserts (add_entry hot path)
//!   - `watch::Sender<Option<SegmentId>>` receives the ID of each sealed segment
//!     (consumed by the BackgroundOffloader).

use crate::storage::entry_segment::{EntrySegmentWriter, entry_seg_path, next_entry_seg_id};
use crate::storage::fjall::{FjallIndex, SegmentRegistry};
use crate::storage::segment::{SegmentId, SegmentKind, SegmentMeta, SegmentStatus};
use bytes::Bytes;
use folio_core::error::Result;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::watch;

/// Default EntrySegment seal threshold (128 MB).
pub const DEFAULT_ENTRY_SEAL_THRESHOLD: u64 = 128 * 1024 * 1024;

// ── Cache ─────────────────────────────────────────────────────────────────

pub struct EntrySegmentCache {
    /// New entries not yet written to the active EntrySegment file.
    hot: HashMap<(u64, u64), Bytes>,
    /// Entries written and synced to the active EntrySegment, kept in memory
    /// until the segment seals so reads never hit the growing file.
    warm: HashMap<(u64, u64), Bytes>,
    hot_bytes: usize,
    flush_threshold: usize,
}

impl EntrySegmentCache {
    pub fn new(flush_threshold: usize) -> Self {
        Self {
            hot: HashMap::new(),
            warm: HashMap::new(),
            hot_bytes: 0,
            flush_threshold,
        }
    }

    pub fn insert(&mut self, ledger_id: u64, entry_id: u64, payload: Bytes) {
        // Duplicate detection in add_entry prevents re-insertion of warm entries,
        // but guard hot_bytes accounting defensively.
        if self.warm.contains_key(&(ledger_id, entry_id)) {
            return;
        }
        let prev = self.hot.insert((ledger_id, entry_id), payload.clone());
        if prev.is_none() {
            self.hot_bytes += payload.len();
        }
    }

    pub fn get(&self, ledger_id: u64, entry_id: u64) -> Option<&Bytes> {
        self.hot
            .get(&(ledger_id, entry_id))
            .or_else(|| self.warm.get(&(ledger_id, entry_id)))
    }

    pub fn contains(&self, ledger_id: u64, entry_id: u64) -> bool {
        self.hot.contains_key(&(ledger_id, entry_id))
            || self.warm.contains_key(&(ledger_id, entry_id))
    }

    pub fn is_full(&self) -> bool {
        self.hot_bytes >= self.flush_threshold
    }

    pub fn hot_byte_len(&self) -> usize {
        self.hot_bytes
    }

    /// Atomically move all hot entries into warm and return them for disk write.
    ///
    /// After this call every returned entry is immediately readable from warm,
    /// so L0 reads never miss while the async write + sync is in flight.
    pub fn drain_hot_to_warm(&mut self) -> Vec<(u64, u64, Bytes)> {
        self.hot_bytes = 0;
        let entries: Vec<(u64, u64, Bytes)> = self
            .hot
            .drain()
            .map(|((lid, eid), payload)| (lid, eid, payload))
            .collect();
        for (lid, eid, payload) in &entries {
            self.warm.insert((*lid, *eid), payload.clone());
        }
        entries
    }

    /// Drop all warm entries after the active segment has been sealed.
    ///
    /// Must be called only after `entry_index` has been updated for every warm
    /// entry, so that subsequent reads fall through to L1 (entry_index + block
    /// cache) against the now-immutable sealed file.
    pub fn evict_warm(&mut self) {
        self.warm.clear();
    }

    fn is_hot_empty(&self) -> bool {
        self.hot.is_empty()
    }
}

// ── Flush loop ────────────────────────────────────────────────────────────

/// Default periodic flush interval (ms).
pub const DEFAULT_FLUSH_TICK_MS: u64 = 500;

#[allow(clippy::too_many_arguments)]
pub fn spawn_flush_loop(
    cache: Arc<Mutex<EntrySegmentCache>>,
    entries_dir: PathBuf,
    entry_index: Arc<FjallIndex>,
    wal_index: Arc<FjallIndex>,
    entry_registry: Arc<SegmentRegistry>,
    entry_sealed_tx: watch::Sender<Option<SegmentId>>,
    seal_threshold: u64,
    flush_tick_ms: u64,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("entry-flush".into())
        .spawn(move || {
            tokio_uring::start(async move {
                if let Err(e) = flush_loop(
                    cache,
                    entries_dir,
                    entry_index,
                    wal_index,
                    entry_registry,
                    entry_sealed_tx,
                    seal_threshold,
                    flush_tick_ms,
                )
                .await
                {
                    tracing::error!("entry-flush: {e}");
                }
            });
        })
        .expect("spawn entry-flush thread")
}

#[allow(clippy::too_many_arguments)]
async fn flush_loop(
    cache: Arc<Mutex<EntrySegmentCache>>,
    entries_dir: PathBuf,
    entry_index: Arc<FjallIndex>,
    wal_index: Arc<FjallIndex>,
    entry_registry: Arc<SegmentRegistry>,
    entry_sealed_tx: watch::Sender<Option<SegmentId>>,
    seal_threshold: u64,
    flush_tick_ms: u64,
) -> Result<()> {
    tokio::fs::create_dir_all(&entries_dir).await?;

    // Seal any Active entry segments left from the previous run (partially
    // written; already indexed by CrashRecovery Phase 1).
    if let Ok(orphans) = entry_registry.list_by_status(SegmentStatus::Active) {
        for meta in orphans {
            if matches!(meta.kind, SegmentKind::Entry) {
                let _ =
                    entry_registry.cas_status(meta.id, SegmentStatus::Active, SegmentStatus::Local);
                let _ = entry_sealed_tx.send(Some(meta.id));
                tracing::info!(
                    "entry-flush: sealed orphaned active segment {} on startup",
                    meta.id
                );
            }
        }
    }
    // Reset Offloading segments to Local so the offloader retries the upload.
    // A segment is left in Offloading when the process crashes mid-upload; the
    // offloader only scans Local, so without this they would never be re-queued.
    if let Ok(orphans) = entry_registry.list_by_status(SegmentStatus::Offloading) {
        for meta in orphans {
            if matches!(meta.kind, SegmentKind::Entry) {
                let _ = entry_registry.cas_status(
                    meta.id,
                    SegmentStatus::Offloading,
                    SegmentStatus::Local,
                );
                let _ = entry_sealed_tx.send(Some(meta.id));
                tracing::info!(
                    "entry-flush: reset stuck offloading segment {} to Local on startup",
                    meta.id
                );
            }
        }
    }

    let mut next_seg_id = next_entry_seg_id(&entries_dir, &entry_registry)?;
    let mut writer = open_active_segment(&entries_dir, next_seg_id, &entry_registry)?;
    let mut ledger_sizes: HashMap<u64, u64> = HashMap::new();

    loop {
        // Wait until hot fills or the periodic tick fires.
        loop {
            if cache.lock().is_full() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(flush_tick_ms)).await;
            if !cache.lock().is_hot_empty() {
                break;
            }
        }

        // Move hot → warm atomically under the lock.  From this point forward
        // every returned entry is readable from L0 (warm), so reads never miss
        // while the async write + sync is in flight below.
        let entries = cache.lock().drain_hot_to_warm();
        if entries.is_empty() {
            continue;
        }

        let offsets = writer.write_batch(&entries).await?;
        writer.sync().await?;

        let seg_id = writer.id();

        for ((lid, eid, _), (offset, length)) in entries.iter().zip(offsets.iter()) {
            entry_index.insert_location(*lid, *eid, seg_id, *offset, *length)?;
            // Remove WAL index entry so WAL GC can reclaim the segment.
            // Silently ignored when wal_index has no entry (normal-operation path
            // where crash recovery never ran).
            let _ = wal_index.delete_location(*lid, *eid);
            *ledger_sizes.entry(*lid).or_insert(0) += *length as u64;
        }

        entry_registry.upsert(&SegmentMeta {
            id: seg_id,
            kind: SegmentKind::Entry,
            status: SegmentStatus::Active,
            local_path: Some(entry_seg_path(&entries_dir, seg_id)),
            s3_key: None,
            byte_len: writer.byte_len(),
            ledger_sizes: ledger_sizes.clone(),
        })?;

        tracing::debug!(
            "entry-flush: seg {} +{} entries ({} bytes total)",
            seg_id,
            entries.len(),
            writer.byte_len(),
        );

        if writer.byte_len() >= seal_threshold {
            seal_active_segment(seg_id, writer.byte_len(), &entry_registry, &entry_sealed_tx)?;
            // entry_index now has every warm entry's location in the sealed file.
            // Evict warm so subsequent reads go to L1 (entry_index + block cache).
            cache.lock().evict_warm();
            next_seg_id += 1;
            writer = open_active_segment(&entries_dir, next_seg_id, &entry_registry)?;
            ledger_sizes.clear();
        }
    }
}

fn open_active_segment(
    entries_dir: &Path,
    seg_id: SegmentId,
    entry_registry: &Arc<SegmentRegistry>,
) -> Result<EntrySegmentWriter> {
    let writer = EntrySegmentWriter::create(entries_dir, seg_id)?;
    entry_registry.upsert(&SegmentMeta {
        id: seg_id,
        kind: SegmentKind::Entry,
        status: SegmentStatus::Active,
        local_path: Some(writer.path().to_path_buf()),
        s3_key: None,
        byte_len: 0,
        ledger_sizes: HashMap::new(),
    })?;
    Ok(writer)
}

fn seal_active_segment(
    seg_id: SegmentId,
    byte_len: u64,
    entry_registry: &Arc<SegmentRegistry>,
    entry_sealed_tx: &watch::Sender<Option<SegmentId>>,
) -> Result<()> {
    entry_registry.cas_status(seg_id, SegmentStatus::Active, SegmentStatus::Local)?;
    let _ = entry_sealed_tx.send(Some(seg_id));
    tracing::info!(
        "entry-flush: sealed entry segment {} ({} bytes)",
        seg_id,
        byte_len
    );
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry_segment::ENTRY_FLUSH_SIZE;

    #[test]
    fn hot_to_warm_keeps_entries_readable() {
        let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);
        cache.insert(1, 0, Bytes::from_static(b"hello"));
        cache.insert(1, 1, Bytes::from_static(b"world"));
        assert!(cache.get(1, 0).is_some());

        let drained = cache.drain_hot_to_warm();
        assert_eq!(drained.len(), 2);
        assert_eq!(cache.hot_byte_len(), 0);
        // entries are still readable from warm
        assert!(cache.get(1, 0).is_some());
        assert!(cache.get(1, 1).is_some());
        assert!(cache.contains(1, 0));
    }

    #[test]
    fn evict_warm_clears_entries() {
        let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);
        cache.insert(1, 0, Bytes::from_static(b"hello"));
        cache.drain_hot_to_warm();
        assert!(cache.get(1, 0).is_some());

        cache.evict_warm();
        assert!(cache.get(1, 0).is_none());
        assert!(!cache.contains(1, 0));
    }

    #[test]
    fn hot_full_threshold() {
        let mut cache = EntrySegmentCache::new(10);
        cache.insert(1, 0, Bytes::from(vec![0u8; 9]));
        assert!(!cache.is_full());
        cache.insert(1, 1, Bytes::from(vec![0u8; 1]));
        assert!(cache.is_full());
    }

    #[test]
    fn duplicate_insert_no_double_count() {
        let mut cache = EntrySegmentCache::new(100);
        cache.insert(1, 0, Bytes::from(vec![0u8; 50]));
        cache.insert(1, 0, Bytes::from(vec![0u8; 50]));
        assert_eq!(cache.hot_byte_len(), 50);
    }

    #[test]
    fn warm_entry_not_re_inserted_to_hot() {
        let mut cache = EntrySegmentCache::new(100);
        cache.insert(1, 0, Bytes::from(vec![0u8; 50]));
        cache.drain_hot_to_warm();
        // Attempt to re-insert the same entry (e.g. from crash recovery)
        cache.insert(1, 0, Bytes::from(vec![0u8; 50]));
        // hot should still be empty — warm guards against double-counting
        assert_eq!(cache.hot_byte_len(), 0);
        assert!(cache.hot.is_empty());
    }

    #[test]
    fn new_hot_entries_after_drain_readable() {
        let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);
        cache.insert(1, 0, Bytes::from_static(b"first"));
        cache.drain_hot_to_warm();

        // New entry arrives while first is in warm
        cache.insert(1, 1, Bytes::from_static(b"second"));
        assert!(cache.get(1, 0).is_some()); // warm
        assert!(cache.get(1, 1).is_some()); // hot
    }
}
