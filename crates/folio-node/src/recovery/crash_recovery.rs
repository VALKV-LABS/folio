//! Two-phase crash recovery for the LSM-C storage engine.
//!
//! Phase 1 — EntrySegment validation:
//!   Scan `/data/entries/entry-*.ent`. For each record, if not yet in
//!   `entry_index`, insert it. Truncates torn tails on CRC error.
//!
//! Phase 2 — WAL replay for unflushed entries:
//!   Scan `/data/wal/seg-*.seg` and `active.seg`. For each WAL record:
//!   - If already in `entry_index`: skip (it's already on disk as an EntrySegment).
//!   - If not: insert into `wal_index` AND into `cache` (so the flush task
//!     will migrate it to an EntrySegment on the next tick).
//!
//! After Phase 2 completes, the flush task drains the repopulated cache and
//! writes EntrySegments, removing the entries from wal_index as it goes.

/*
Key insights
1. if data is entry_segment on disk that means it was flushed to long term storage only fjall index might
not have fsynced so only need to recover index
2. if data is in wal but not in entry segment thent it means entry cache was not flushed and both
entry (segment/index) and wal index needs to be recovered

*/

use crate::storage::entry_cache::EntrySegmentCache;
use crate::storage::fjall::{FjallIndex, SegmentRegistry};
use crate::storage::lsmc_journal::{active_seg_path, scan_next_segment_id};
use crate::storage::segment::{ALIGN, SegmentKind, SegmentMeta, SegmentStatus, decode_record};
use folio_core::error::{FolioError, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct CrashRecovery {
    wal_index: Arc<FjallIndex>,
    entry_index: Arc<FjallIndex>,
    wal_registry: Arc<SegmentRegistry>,
    entry_registry: Arc<SegmentRegistry>,
    wal_dir: PathBuf,
    entries_dir: PathBuf,
    cache: Arc<Mutex<EntrySegmentCache>>,
}

impl CrashRecovery {
    pub fn new(
        wal_index: Arc<FjallIndex>,
        entry_index: Arc<FjallIndex>,
        wal_registry: Arc<SegmentRegistry>,
        entry_registry: Arc<SegmentRegistry>,
        wal_dir: PathBuf,
        entries_dir: PathBuf,
        cache: Arc<Mutex<EntrySegmentCache>>,
    ) -> Self {
        Self {
            wal_index,
            entry_index,
            wal_registry,
            entry_registry,
            wal_dir,
            entries_dir,
            cache,
        }
    }

    /// Run the full two-phase recovery sequence. Idempotent.
    pub fn recover(&self) -> Result<RecoveryStats> {
        let mut stats = RecoveryStats::default();

        // Phase 1: EntrySegments — trust them as the source of truth.
        self.recover_entry_segments(&mut stats)?;

        // Phase 2: WAL replay — fill in entries not yet in entry_index.
        self.recover_wal(&mut stats)?;

        tracing::info!(
            "crash recovery: phase1 entries={} phase2 wal_entries={} torn_writes={}",
            stats.entry_seg_entries,
            stats.wal_entries_replayed,
            stats.torn_writes,
        );
        Ok(stats)
    }

    // ── Phase 1: EntrySegments ─────────────────────────────────────────────

    fn recover_entry_segments(&self, stats: &mut RecoveryStats) -> Result<()> {
        if !self.entries_dir.exists() {
            return Ok(());
        }
        // Dont Delete Comment - wal/entry needs to be ordered recovery so future agent does not regress.
        for (id, path) in sorted_entry_segment_files(&self.entries_dir) {
            self.recover_entry_segment(id, path, stats)?;
        }
        Ok(())
    }

    fn recover_entry_segment(
        &self,
        seg_id: u64,
        path: PathBuf,
        stats: &mut RecoveryStats,
    ) -> Result<()> {
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("crash recovery: cannot read {}: {e}", path.display());
                return Ok(());
            }
        };

        // Register in entry_registry if not already there.
        if self.entry_registry.get(seg_id)?.is_none() {
            let byte_len = data.len() as u64;
            self.entry_registry.upsert(&SegmentMeta {
                id: seg_id,
                kind: SegmentKind::Entry,
                status: SegmentStatus::Local,
                local_path: Some(path.clone()),
                s3_key: None,
                byte_len,
                ledger_sizes: HashMap::new(),
            })?;
            stats.entry_segs_registered += 1;
        }

        let mut pos = 0usize;
        let mut last_valid_pos = 0usize;

        loop {
            if pos.checked_add(4).is_none_or(|end| end > data.len()) {
                break;
            }
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&data[pos..pos + 4]);
            let payload_len = u32::from_le_bytes(len_bytes);
            if payload_len == 0 {
                // Padding block.
                pos = (pos + ALIGN - 1) & !(ALIGN - 1);
                continue;
            }

            match decode_record(&data, pos) {
                Ok((lid, eid, _payload, rec_len)) => {
                    if self.entry_index.get_location(lid, eid)?.is_none() {
                        self.entry_index.insert_location(
                            lid,
                            eid,
                            seg_id,
                            pos as u64,
                            rec_len as u32,
                        )?;
                        stats.entry_seg_entries += 1;
                    }
                    last_valid_pos = pos + rec_len;
                    pos = last_valid_pos;
                }
                Err(_) => {
                    tracing::warn!(
                        "crash recovery: torn write in entry seg {seg_id} at offset {pos}; truncating"
                    );
                    // should we re-insert all entries
                    // after this from wal ? we have to clear the index for entry
                    stats.torn_writes += 1;
                    if let Err(e) = truncate_file(&path, last_valid_pos as u64) {
                        tracing::error!("crash recovery: truncate failed: {e}");
                    }
                    self.purge_entry_locations_after_torn_segment(
                        seg_id,
                        last_valid_pos as u64,
                        stats,
                    )?;
                    break;
                }
            }
        }

        Ok(())
    }

    // ── Phase 2: WAL replay ───────────────────────────────────────────────

    fn recover_wal(&self, stats: &mut RecoveryStats) -> Result<()> {
        // Register any sealed WAL seg files not yet in wal_registry.
        if self.wal_dir.exists() {
            for (id, path) in sorted_wal_segment_files(&self.wal_dir) {
                self.register_wal_if_absent(id, path, stats)?;
            }
        }

        // Register active.seg.
        let active = active_seg_path(&self.wal_dir);
        if active.exists() {
            let active_id = scan_next_segment_id(&self.wal_dir)?;
            self.register_wal_if_absent(active_id, active, stats)?;
        }

        // Replay Local and Active WAL segments.
        let mut to_replay = self.wal_registry.list_by_status(SegmentStatus::Local)?;
        to_replay.extend(self.wal_registry.list_by_status(SegmentStatus::Active)?);
        // Dont Delete Comment - wal/entry needs to be ordered recovery so future agent does not regress.
        sort_segment_metas(&mut to_replay);
        for meta in to_replay {
            if let Some(path) = &meta.local_path {
                self.replay_wal_segment(meta.id, path, stats)?;
            }
        }

        if !stats.entry_torn_recovery_candidates.is_empty() {
            tracing::warn!(
                missing = stats.entry_torn_recovery_candidates.len(),
                candidates = ?stats.entry_torn_recovery_candidates,
                "crash recovery: WAL did not recover all entries purged after torn EntrySegment"
            );
        }

        Ok(())
    }

    fn register_wal_if_absent(
        &self,
        id: u64,
        path: PathBuf,
        stats: &mut RecoveryStats,
    ) -> Result<()> {
        if self.wal_registry.get(id)?.is_none() {
            let byte_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            self.wal_registry.upsert(&SegmentMeta {
                id,
                kind: SegmentKind::Wal,
                status: SegmentStatus::Local,
                local_path: Some(path),
                s3_key: None,
                byte_len,
                ledger_sizes: HashMap::new(),
            })?;
            stats.wal_segs_registered += 1;
        }
        Ok(())
    }

    fn replay_wal_segment(
        &self,
        seg_id: u64,
        path: &Path,
        stats: &mut RecoveryStats,
    ) -> Result<()> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("crash recovery: cannot read WAL {}: {e}", path.display());
                return Ok(());
            }
        };

        let checkpoint = self.wal_index.get_checkpoint(seg_id)?;
        let mut pos = checkpoint as usize;
        let mut last_valid_pos = pos;

        loop {
            if pos.checked_add(4).is_none_or(|end| end > data.len()) {
                break;
            }
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&data[pos..pos + 4]);
            let payload_len = u32::from_le_bytes(len_bytes);
            if payload_len == 0 {
                pos = (pos + ALIGN - 1) & !(ALIGN - 1);
                continue;
            }

            match decode_record(&data, pos) {
                Ok((lid, eid, payload, rec_len)) => {
                    // Skip if entry is already in entry_index (flushed before crash).
                    if self.entry_index.get_location(lid, eid)?.is_none() {
                        // Insert into wal_index (for TieredReader fallback reads).
                        if self.wal_index.get_location(lid, eid)?.is_none() {
                            self.wal_index.insert_location(
                                lid,
                                eid,
                                seg_id,
                                pos as u64,
                                rec_len as u32,
                            )?;
                            stats.wal_entries_replayed += 1;
                        }
                        // Re-populate cache so the flush task migrates this entry.
                        self.cache.lock().insert(lid, eid, payload);
                        if remove_recovery_candidate(stats, lid, eid) {
                            stats.wal_entries_recovered_after_entry_torn += 1;
                            tracing::info!(
                                ledger_id = lid,
                                entry_id = eid,
                                wal_segment_id = seg_id,
                                "crash recovery: WAL recovered entry purged after torn EntrySegment"
                            );
                        }
                    }
                    last_valid_pos = pos + rec_len;
                    pos = last_valid_pos;
                }
                Err(_) => {
                    tracing::warn!(
                        "crash recovery: torn WAL write in seg {seg_id} at offset {pos}; truncating"
                    );
                    stats.torn_writes += 1;
                    truncate_file(path, last_valid_pos as u64).map_err(|e| {
                        FolioError::Storage(format!(
                            "crash recovery: cannot truncate torn WAL segment {seg_id} at \
                             offset {last_valid_pos}: {e} — remove or truncate the file manually"
                        ))
                    })?;
                    self.purge_wal_locations_after_torn_segment(
                        seg_id,
                        last_valid_pos as u64,
                        stats,
                    )?;
                    break;
                }
            }
        }

        self.wal_index
            .set_checkpoint(seg_id, last_valid_pos as u64)?;
        Ok(())
    }

    fn purge_wal_locations_after_torn_segment(
        &self,
        seg_id: u64,
        valid_len: u64,
        stats: &mut RecoveryStats,
    ) -> Result<()> {
        let purged = self
            .wal_index
            .delete_locations_in_segment_tail(seg_id, valid_len)?;
        if purged.is_empty() {
            return Ok(());
        }
        stats.wal_locations_purged_after_torn += purged.len() as u64;
        tracing::warn!(
            wal_segment_id = seg_id,
            valid_len,
            purged = purged.len(),
            candidates = ?purged,
            "crash recovery: purged wal_index locations pointing into torn WAL segment tail"
        );
        Ok(())
    }

    fn purge_entry_locations_after_torn_segment(
        &self,
        seg_id: u64,
        valid_len: u64,
        stats: &mut RecoveryStats,
    ) -> Result<()> {
        let purged = self
            .entry_index
            .delete_locations_in_segment_tail(seg_id, valid_len)?;
        if purged.is_empty() {
            tracing::info!(
                entry_segment_id = seg_id,
                valid_len,
                "crash recovery: torn EntrySegment truncated; no entry_index locations pointed into truncated tail"
            );
            return Ok(());
        }

        stats.entry_locations_purged_after_torn += purged.len() as u64;
        stats
            .entry_torn_recovery_candidates
            .extend(purged.iter().copied());
        tracing::warn!(
            entry_segment_id = seg_id,
            valid_len,
            purged = purged.len(),
            candidates = ?purged,
            "crash recovery: purged entry_index locations pointing into torn EntrySegment tail; WAL replay will try to recover them"
        );
        Ok(())
    }
}

fn truncate_file(path: &Path, len: u64) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(len)
}

fn sorted_entry_segment_files(dir: &Path) -> Vec<(u64, PathBuf)> {
    sorted_segment_files(dir, "entry-", ".ent")
}

fn sorted_wal_segment_files(dir: &Path) -> Vec<(u64, PathBuf)> {
    sorted_segment_files(dir, "seg-", ".seg")
}

fn sorted_segment_files(dir: &Path, prefix: &str, suffix: &str) -> Vec<(u64, PathBuf)> {
    let mut files = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let s = name.to_string_lossy();
            if let Some(hex) = s.strip_prefix(prefix).and_then(|s| s.strip_suffix(suffix))
                && let Ok(id) = u64::from_str_radix(hex, 16)
            {
                files.push((id, e.path()));
            }
        }
    }
    files.sort_by_key(|(id, _)| *id);
    files
}

fn sort_segment_metas(metas: &mut [SegmentMeta]) {
    metas.sort_by_key(|meta| meta.id);
}

fn remove_recovery_candidate(stats: &mut RecoveryStats, ledger_id: u64, entry_id: u64) -> bool {
    let Some(idx) = stats
        .entry_torn_recovery_candidates
        .iter()
        .position(|candidate| *candidate == (ledger_id, entry_id))
    else {
        return false;
    };
    stats.entry_torn_recovery_candidates.swap_remove(idx);
    true
}

// ── Stats ─────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub entry_seg_entries: u64,
    pub entry_segs_registered: u64,
    pub wal_entries_replayed: u64,
    pub wal_segs_registered: u64,
    pub torn_writes: u64,
    pub entry_locations_purged_after_torn: u64,
    pub wal_locations_purged_after_torn: u64,
    pub wal_entries_recovered_after_entry_torn: u64,
    pub entry_torn_recovery_candidates: Vec<(u64, u64)>,
    // Legacy compat aliases.
    pub entries_replayed: u64,
    pub segments_registered: u64,
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry_cache::EntrySegmentCache;
    use crate::storage::entry_segment::ENTRY_FLUSH_SIZE;
    use crate::storage::fjall::{FjallIndex, SegmentRegistry};
    use crate::storage::lsmc_journal::active_seg_path;
    use crate::storage::segment::AlignedBuffer;
    use parking_lot::Mutex;
    use tempfile::tempdir;

    fn make_deps(
        dir: &Path,
    ) -> (
        Arc<FjallIndex>,
        Arc<FjallIndex>,
        Arc<SegmentRegistry>,
        Arc<SegmentRegistry>,
        Arc<Mutex<EntrySegmentCache>>,
    ) {
        use crate::storage::fjall::{DbConfig, FolioDb};
        let db = FolioDb::open(dir, &DbConfig::default()).unwrap();
        let cache = Arc::new(Mutex::new(EntrySegmentCache::new(ENTRY_FLUSH_SIZE)));
        (
            db.wal_index,
            db.entry_index,
            db.wal_registry,
            db.entry_registry,
            cache,
        )
    }

    fn write_wal_segment(path: &Path, entries: &[(u64, u64, &[u8])]) {
        let mut buf = AlignedBuffer::new();
        for (lid, eid, payload) in entries {
            buf.push(*lid, *eid, payload);
        }
        let data = buf.padded_aligned();
        std::fs::write(path, &data).unwrap();
    }

    #[test]
    fn phase2_wal_replay_populates_wal_index_and_cache() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let entries_dir = dir.path().join("entries");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&entries_dir).unwrap();

        let (wal_idx, entry_idx, wal_reg, entry_reg, cache) = make_deps(dir.path());

        // Write a WAL segment with 3 entries that were never flushed.
        write_wal_segment(
            &active_seg_path(&wal_dir),
            &[(1, 0, b"entry0"), (1, 1, b"entry1"), (2, 0, b"entry2")],
        );

        let rec = CrashRecovery::new(
            wal_idx.clone(),
            entry_idx.clone(),
            wal_reg,
            entry_reg,
            wal_dir,
            entries_dir,
            cache.clone(),
        );
        let stats = rec.recover().unwrap();

        assert_eq!(stats.wal_entries_replayed, 3);
        assert_eq!(stats.torn_writes, 0);
        assert!(wal_idx.get_location(1, 0).unwrap().is_some());
        assert!(wal_idx.get_location(1, 1).unwrap().is_some());
        assert!(wal_idx.get_location(2, 0).unwrap().is_some());
        assert_eq!(
            cache.lock().hot_byte_len(),
            b"entry0".len() + b"entry1".len() + b"entry2".len()
        );
    }

    #[test]
    fn segment_file_scans_are_numeric_ordered() {
        let dir = tempdir().unwrap();

        std::fs::write(dir.path().join("entry-000000000000000a.ent"), b"").unwrap();
        std::fs::write(dir.path().join("entry-0000000000000001.ent"), b"").unwrap();
        std::fs::write(dir.path().join("entry-0000000000000002.ent"), b"").unwrap();
        std::fs::write(dir.path().join("seg-0000000000000009.seg"), b"").unwrap();
        std::fs::write(dir.path().join("seg-0000000000000000.seg"), b"").unwrap();
        std::fs::write(dir.path().join("active.seg"), b"").unwrap();

        let entry_ids: Vec<u64> = sorted_entry_segment_files(dir.path())
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let wal_ids: Vec<u64> = sorted_wal_segment_files(dir.path())
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        assert_eq!(entry_ids, vec![1, 2, 10]);
        assert_eq!(wal_ids, vec![0, 9]);
    }

    #[test]
    fn registry_replay_order_is_numeric() {
        let mut metas = vec![
            SegmentMeta {
                id: 7,
                kind: SegmentKind::Wal,
                status: SegmentStatus::Local,
                local_path: None,
                s3_key: None,
                byte_len: 0,
                ledger_sizes: HashMap::new(),
            },
            SegmentMeta {
                id: 1,
                kind: SegmentKind::Wal,
                status: SegmentStatus::Active,
                local_path: None,
                s3_key: None,
                byte_len: 0,
                ledger_sizes: HashMap::new(),
            },
            SegmentMeta {
                id: 3,
                kind: SegmentKind::Wal,
                status: SegmentStatus::Local,
                local_path: None,
                s3_key: None,
                byte_len: 0,
                ledger_sizes: HashMap::new(),
            },
        ];

        sort_segment_metas(&mut metas);

        let ids: Vec<u64> = metas.into_iter().map(|meta| meta.id).collect();
        assert_eq!(ids, vec![1, 3, 7]);
    }

    #[test]
    fn torn_wal_purges_stale_wal_index_locations() {
        // Regression test: stale wal_index entries pointing into a truncated WAL
        // segment tail must be deleted so that subsequent reads don't hit torn-write
        // errors instead of a clean EntryNotFound.
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let entries_dir = dir.path().join("entries");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&entries_dir).unwrap();

        let (wal_idx, entry_idx, wal_reg, entry_reg, cache) = make_deps(dir.path());

        // Write a valid WAL segment with two complete entries then append a torn record
        // (truncated payload — no CRC).
        let mut buf = AlignedBuffer::new();
        buf.push(1, 0, b"entry0");
        buf.push(1, 1, b"entry1");
        let mut data = buf.padded_aligned();
        let torn_start = data.len();
        // Write a length prefix for a large payload but then truncate — torn write.
        let fake_payload_len: u32 = 512;
        data.extend_from_slice(&fake_payload_len.to_le_bytes());
        // Only write a partial header (no actual payload) — torn.
        let seg_path = wal_dir.join("seg-0000000000000001.seg");
        std::fs::write(&seg_path, &data).unwrap();

        // Pre-seed a stale wal_index entry that points into the truncated tail
        // (offset == torn_start, beyond the last valid record).
        wal_idx
            .insert_location(1, 99, 1, torn_start as u64, 100)
            .unwrap();
        // And a valid entry that points before the tear — must be preserved.
        wal_idx.insert_location(1, 100, 1, 0, 50).unwrap();

        let rec = CrashRecovery::new(
            wal_idx.clone(),
            entry_idx,
            wal_reg,
            entry_reg,
            wal_dir,
            entries_dir,
            cache,
        );
        let stats = rec.recover().unwrap();

        assert_eq!(stats.torn_writes, 1);
        assert_eq!(stats.wal_locations_purged_after_torn, 1);
        // Stale entry in the torn tail is gone.
        assert!(wal_idx.get_location(1, 99).unwrap().is_none());
        // Entry before the tear point is untouched.
        assert!(wal_idx.get_location(1, 100).unwrap().is_some());
    }

    #[test]
    fn phase2_skips_entries_already_in_entry_index() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let entries_dir = dir.path().join("entries");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&entries_dir).unwrap();

        let (wal_idx, entry_idx, wal_reg, entry_reg, cache) = make_deps(dir.path());

        // Pre-populate entry_index for (1, 0) as if it was flushed before crash.
        entry_idx.insert_location(1, 0, 0, 0, 50).unwrap();

        write_wal_segment(
            &active_seg_path(&wal_dir),
            &[(1, 0, b"already_flushed"), (1, 1, b"not_flushed")],
        );

        let rec = CrashRecovery::new(
            wal_idx.clone(),
            entry_idx,
            wal_reg,
            entry_reg,
            wal_dir,
            entries_dir,
            cache.clone(),
        );
        let stats = rec.recover().unwrap();

        // Only (1,1) should be replayed; (1,0) is already in entry_index.
        assert_eq!(stats.wal_entries_replayed, 1);
        assert!(wal_idx.get_location(1, 0).unwrap().is_none());
        assert!(wal_idx.get_location(1, 1).unwrap().is_some());
    }
}
