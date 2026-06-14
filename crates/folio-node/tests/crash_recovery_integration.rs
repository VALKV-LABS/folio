//! Integration test: crash recovery replays entries written to WAL segments.
//!
//! Writes WAL segment data directly via AlignedBuffer (same on-disk format as
//! LsmcJournal) so the test works in Docker without io_uring.
//!
//! After recovery, entries live in `wal_index`.  TieredReader's WAL tier (L2)
//! serves reads until the flush thread migrates them to EntrySegments (L1).
//!
//! Tests that require io_uring (LsmcJournal O_DIRECT writes) are in
//! lsmc_write_read.rs.

use folio_node::storage::segment::AlignedBuffer;
use folio_node::{
    BlockCache, CrashRecovery, DbConfig, ENTRY_FLUSH_SIZE, EntrySegmentCache, FjallIndex, FolioDb,
    SegmentRegistry, TieredReader,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tempfile::tempdir;

fn make_payload(i: u64) -> Vec<u8> {
    vec![i as u8; 64]
}

fn write_wal_segment(path: &std::path::Path, entries: &[(u64, u64, Vec<u8>)]) {
    let mut buf = AlignedBuffer::new();
    for (lid, eid, payload) in entries {
        buf.push(*lid, *eid, payload);
    }
    std::fs::write(path, buf.padded_aligned()).unwrap();
}

fn make_deps(
    dir: &std::path::Path,
) -> (
    Arc<FjallIndex>,
    Arc<FjallIndex>,
    Arc<SegmentRegistry>,
    Arc<SegmentRegistry>,
    Arc<Mutex<EntrySegmentCache>>,
) {
    let db = FolioDb::open(dir, &DbConfig::default()).unwrap();
    (
        db.wal_index,
        db.entry_index,
        db.wal_registry,
        db.entry_registry,
        Arc::new(Mutex::new(EntrySegmentCache::new(ENTRY_FLUSH_SIZE))),
    )
}

#[tokio::test]
async fn crash_then_recover_all_entries_readable() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&entries_dir).unwrap();

    const N: u64 = 15;

    // Simulate crash: write entries directly to wal/active.seg.
    let entries: Vec<(u64, u64, Vec<u8>)> = (0..N).map(|i| (1, i, make_payload(i))).collect();
    write_wal_segment(&wal_dir.join("active.seg"), &entries);

    let (wal_index, entry_index, wal_registry, entry_registry, cache) = make_deps(dir.path());

    let stats = CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        cache.clone(),
    )
    .recover()
    .unwrap();

    assert_eq!(
        stats.wal_entries_replayed, N,
        "all {N} entries must be in wal_index"
    );
    assert_eq!(stats.torn_writes, 0, "no torn writes on clean data");
    assert_eq!(
        stats.wal_segs_registered, 1,
        "active.seg must be registered"
    );

    for i in 0..N {
        assert!(
            wal_index.get_location(1, i).unwrap().is_some(),
            "entry {i} missing from wal_index after recovery"
        );
    }

    // TieredReader: entry_index miss → wal_index hit → WAL file read (L2).
    let block_cache = BlockCache::new(8 * 1024 * 1024);
    let reader = TieredReader::new(
        block_cache,
        entry_index.clone(),
        entry_registry.clone(),
        wal_index.clone(),
        wal_registry.clone(),
        None,
    );

    for i in 0..N {
        let bytes = reader.read_entry(1, i).await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            make_payload(i).as_slice(),
            "entry {i} payload mismatch"
        );
    }
}

#[tokio::test]
async fn crash_recovery_is_idempotent() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&entries_dir).unwrap();

    let entries: Vec<(u64, u64, Vec<u8>)> = (0..5u64).map(|i| (2, i, vec![i as u8; 16])).collect();
    write_wal_segment(&wal_dir.join("active.seg"), &entries);

    let (wal_index, entry_index, wal_registry, entry_registry, cache) = make_deps(dir.path());

    let stats1 = CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        cache.clone(),
    )
    .recover()
    .unwrap();
    assert_eq!(stats1.wal_entries_replayed, 5);

    // Second recovery: wal_index already has all locations — nothing new.
    let stats2 = CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        cache.clone(),
    )
    .recover()
    .unwrap();
    assert_eq!(
        stats2.wal_entries_replayed, 0,
        "second recovery must be a no-op"
    );
    assert_eq!(stats2.torn_writes, 0);
}

#[tokio::test]
async fn crash_recovery_handles_multiple_sealed_segments() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&entries_dir).unwrap();

    // Two sealed WAL segments + one active segment.
    write_wal_segment(
        &wal_dir.join("seg-0000000000000000.seg"),
        &[(1, 0, make_payload(0)), (1, 1, make_payload(1))],
    );
    write_wal_segment(
        &wal_dir.join("seg-0000000000000001.seg"),
        &[(1, 2, make_payload(2)), (1, 3, make_payload(3))],
    );
    write_wal_segment(&wal_dir.join("active.seg"), &[(1, 4, make_payload(4))]);

    let (wal_index, entry_index, wal_registry, entry_registry, cache) = make_deps(dir.path());

    let stats = CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        cache.clone(),
    )
    .recover()
    .unwrap();

    assert_eq!(
        stats.wal_entries_replayed, 5,
        "all 5 entries across 3 WAL segments"
    );
    assert_eq!(stats.wal_segs_registered, 3);
    for i in 0..5u64 {
        assert!(
            wal_index.get_location(1, i).unwrap().is_some(),
            "entry {i} missing"
        );
    }
}

#[tokio::test]
async fn entries_already_in_entry_index_are_skipped_during_wal_replay() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&entries_dir).unwrap();

    let (wal_index, entry_index, wal_registry, entry_registry, cache) = make_deps(dir.path());

    // Pre-populate entry_index for (1, 0) — as if it was flushed to an EntrySegment before crash.
    entry_index.insert_location(1, 0, 99, 0, 100).unwrap();

    write_wal_segment(
        &wal_dir.join("active.seg"),
        &[
            (1, 0, make_payload(0)), // already in entry_index — skip
            (1, 1, make_payload(1)), // not flushed — replay into wal_index
        ],
    );

    let stats = CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        cache.clone(),
    )
    .recover()
    .unwrap();

    assert_eq!(
        stats.wal_entries_replayed, 1,
        "only (1,1) should be replayed"
    );
    assert!(
        wal_index.get_location(1, 0).unwrap().is_none(),
        "(1,0) should not be in wal_index"
    );
    assert!(
        wal_index.get_location(1, 1).unwrap().is_some(),
        "(1,1) must be in wal_index"
    );
}
