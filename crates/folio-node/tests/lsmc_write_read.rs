//! Integration test: write/read roundtrip through the full LSM-C JournalService.
//!
//! Stack exercised:
//!   add_entry → WAL (O_DIRECT via io_uring) + EntrySegmentCache (L0)
//!   read_entry → L0 cache hit immediately after write
//!   seal → rotate WAL (Active → Local), reads remain served from L0
//!
//! Requires io_uring (Linux kernel ≥ 5.1).  Run with:
//!   docker run --rm --security-opt seccomp=unconfined folio-test \
//!       cargo test --test lsmc_write_read

use folio_core::protocol::Entry;
use folio_node::storage::entry_cache::spawn_flush_loop;
use folio_node::{
    BackgroundOffloader, BlockCache, CrashRecovery, DEFAULT_ENTRY_SEAL_THRESHOLD,
    DEFAULT_FLUSH_TICK_MS, DEFAULT_SEAL_THRESHOLD, DbConfig, ENTRY_FLUSH_SIZE, EntrySegmentCache,
    FolioDb, JournalService, LsmcJournal, OffloadPolicy, TieredReader,
};
use parking_lot::Mutex;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::watch;

fn make_entry(entry_id: u64) -> Entry {
    Entry::new(
        1,
        entry_id,
        entry_id.saturating_sub(1),
        vec![entry_id as u8; 64],
    )
}

async fn setup(dir: &Path) -> JournalService {
    let wal_dir = dir.join("wal");
    let entries_dir = dir.join("entries");
    tokio::fs::create_dir_all(&wal_dir).await.unwrap();
    tokio::fs::create_dir_all(&entries_dir).await.unwrap();

    let db = FolioDb::open(dir, &DbConfig::default()).unwrap();
    let wal_index = db.wal_index;
    let entry_index = db.entry_index;
    let wal_registry = db.wal_registry;
    let entry_registry = db.entry_registry;

    let journal = LsmcJournal::open(&wal_dir, DEFAULT_SEAL_THRESHOLD, wal_registry.clone())
        .await
        .unwrap();

    let entry_cache = Arc::new(Mutex::new(EntrySegmentCache::new(ENTRY_FLUSH_SIZE)));
    let (entry_sealed_tx, entry_sealed_rx) = watch::channel::<Option<u64>>(None);

    let flush_thread = spawn_flush_loop(
        entry_cache.clone(),
        entries_dir.clone(),
        entry_index.clone(),
        wal_index.clone(),
        entry_registry.clone(),
        entry_sealed_tx,
        DEFAULT_ENTRY_SEAL_THRESHOLD,
        DEFAULT_FLUSH_TICK_MS,
    );

    CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        entry_cache.clone(),
    )
    .recover()
    .unwrap();

    let block_cache = BlockCache::new(32 * 1024 * 1024);
    let reader = Arc::new(TieredReader::new(
        block_cache,
        entry_index.clone(),
        entry_registry.clone(),
        wal_index.clone(),
        wal_registry.clone(),
        None,
    ));

    let offload = BackgroundOffloader::spawn(
        entry_registry.clone(),
        wal_registry.clone(),
        wal_index.clone(),
        None,
        OffloadPolicy::default(),
        entry_sealed_rx,
    );

    JournalService::new_lsmc(
        "test-node",
        journal,
        wal_index.clone(),
        entry_index.clone(),
        entry_cache,
        reader,
        offload,
        flush_thread,
    )
}

#[tokio::test]
async fn lsmc_write_and_read_roundtrip() {
    let dir = tempdir().unwrap();
    let svc = setup(dir.path()).await;

    const N: u64 = 20;
    for i in 0..N {
        let ack = svc.add_entry(make_entry(i)).await.unwrap();
        assert_eq!(ack.entry_id, i);
        assert_eq!(ack.ledger_id, 1);
    }

    // Reads hit L0 (EntrySegmentCache) — entries were just written.
    for i in 0..N {
        let result = svc.read_entry(1, i).await.unwrap();
        assert_eq!(result.entry.entry_id, i, "entry_id mismatch at i={i}");
        assert_eq!(result.entry.ledger_id, 1);
        assert_eq!(result.entry.data[0], i as u8, "payload mismatch at i={i}");
    }
}

#[tokio::test]
async fn lsmc_seal_then_read() {
    let dir = tempdir().unwrap();
    let svc = setup(dir.path()).await;

    for i in 0u64..5 {
        svc.add_entry(make_entry(i)).await.unwrap();
    }

    // Seal the WAL segment — transitions Active → Local in wal_registry.
    svc.seal().await.unwrap();

    // Reads still served from L0 (EntrySegmentCache); flush has not drained yet.
    for i in 0u64..5 {
        let result = svc.read_entry(1, i).await.unwrap();
        assert_eq!(result.entry.entry_id, i);
        assert_eq!(result.entry.data[0], i as u8);
    }
}

#[tokio::test]
async fn lsmc_duplicate_entry_is_idempotent() {
    let dir = tempdir().unwrap();
    let svc = setup(dir.path()).await;

    let entry = make_entry(0);
    let ack1 = svc.add_entry(entry.clone()).await.unwrap();
    let ack2 = svc.add_entry(entry.clone()).await.unwrap();

    assert_eq!(ack1.entry_id, ack2.entry_id);
    assert_eq!(ack1.ledger_id, ack2.ledger_id);
}

#[tokio::test]
async fn lsmc_read_missing_entry_returns_not_found() {
    let dir = tempdir().unwrap();
    let svc = setup(dir.path()).await;

    let err = svc.read_entry(99, 0).await.unwrap_err();
    assert!(
        matches!(err, folio_core::error::FolioError::EntryNotFound { .. }),
        "expected EntryNotFound, got {err:?}"
    );
}

/// Verify the flush loop transfers hot entries into `entry_index` within a
/// bounded time.  Polls for up to 5 s so the test is not flaky on slow CI.
#[tokio::test]
async fn flush_loop_populates_entry_index() {
    use std::time::Duration;

    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let entries_dir = dir.path().join("entries");
    tokio::fs::create_dir_all(&wal_dir).await.unwrap();
    tokio::fs::create_dir_all(&entries_dir).await.unwrap();

    let db = FolioDb::open(dir.path(), &DbConfig::default()).unwrap();
    let wal_index = db.wal_index.clone();
    let entry_index = db.entry_index.clone();
    let wal_registry = db.wal_registry;
    let entry_registry = db.entry_registry;

    let journal = LsmcJournal::open(&wal_dir, DEFAULT_SEAL_THRESHOLD, wal_registry.clone())
        .await
        .unwrap();

    let entry_cache = Arc::new(Mutex::new(EntrySegmentCache::new(ENTRY_FLUSH_SIZE)));
    let (entry_sealed_tx, entry_sealed_rx) = watch::channel::<Option<u64>>(None);

    // Use a very short tick so the flush loop fires quickly in tests.
    let flush_thread = spawn_flush_loop(
        entry_cache.clone(),
        entries_dir.clone(),
        entry_index.clone(),
        wal_index.clone(),
        entry_registry.clone(),
        entry_sealed_tx,
        DEFAULT_ENTRY_SEAL_THRESHOLD,
        50, // 50 ms tick
    );

    CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        entry_cache.clone(),
    )
    .recover()
    .unwrap();

    let block_cache = BlockCache::new(32 * 1024 * 1024);
    let reader = Arc::new(TieredReader::new(
        block_cache,
        entry_index.clone(),
        entry_registry.clone(),
        wal_index.clone(),
        wal_registry.clone(),
        None,
    ));

    let offload = BackgroundOffloader::spawn(
        entry_registry.clone(),
        wal_registry.clone(),
        wal_index.clone(),
        None,
        OffloadPolicy::default(),
        entry_sealed_rx,
    );

    let svc = JournalService::new_lsmc(
        "test-node",
        journal,
        wal_index.clone(),
        entry_index.clone(),
        entry_cache,
        reader,
        offload,
        flush_thread,
    );

    const N: u64 = 5;
    for i in 0..N {
        svc.add_entry(make_entry(i)).await.unwrap();
    }

    // Poll until the flush loop indexes all entries into entry_index.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let all_indexed = (0..N).all(|i| entry_index.get_location(1, i).unwrap_or(None).is_some());
        if all_indexed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "flush loop did not index entries within 5s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Confirm every entry has a valid index location.
    for i in 0..N {
        let loc = entry_index.get_location(1, i).unwrap();
        assert!(
            loc.is_some(),
            "entry {i} missing from entry_index after flush"
        );
    }
}

/// After the flush loop has indexed entries, a freshly opened service with an
/// empty L0 cache must serve reads through L1 (entry_index + segment file).
#[tokio::test]
async fn reads_work_via_entry_index_after_fresh_open() {
    use std::time::Duration;

    let dir = tempdir().unwrap();

    // First session: write entries and wait for at least one flush tick.
    // DEFAULT_FLUSH_TICK_MS = 500ms; sleep 2 s = 4× flush period.
    // We drop svc (and its Fjall handles) completely before re-opening so that
    // there is never more than one Fjall writer on the same path.
    {
        let svc = setup(dir.path()).await;
        for i in 0u64..5 {
            svc.add_entry(make_entry(i)).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        // svc drops here — Fjall keyspace is released.
    }

    // Second session: fresh L0 cache (CrashRecovery skips entries already in
    // entry_index).  Reads must go to L1 (entry_index + segment file).
    let svc2 = setup(dir.path()).await;

    for i in 0u64..5 {
        let result = svc2.read_entry(1, i).await.unwrap();
        assert_eq!(
            result.entry.entry_id, i,
            "L1 read: entry_id mismatch at {i}"
        );
        assert_eq!(
            result.entry.data[0], i as u8,
            "L1 read: payload mismatch at {i}"
        );
    }
}
