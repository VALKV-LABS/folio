//! Throughput benchmark: concurrent writes across multiple ledgers trigger
//! adaptive batching in the LsmcJournal flush loop.
//!
//! 16 concurrent "client" tasks each write 625 × 1 KB entries to their own
//! ledger.  Because all tasks send to the flush channel simultaneously, the
//! flush loop's try_recv drain loop picks up multiple entries per O_DIRECT
//! write_at — the adaptive-batch path that a single-ledger sequential loop
//! never exercises.
//!
//! Requires io_uring — run with:
//!   docker run --rm --security-opt seccomp=unconfined folio-test \
//!       cargo test --test load_walrus

use folio_core::protocol::Entry;
use folio_node::storage::entry_cache::spawn_flush_loop;
use folio_node::{
    BackgroundOffloader, BlockCache, CrashRecovery, DEFAULT_ENTRY_SEAL_THRESHOLD,
    DEFAULT_FLUSH_TICK_MS, DEFAULT_SEAL_THRESHOLD, DbConfig, ENTRY_FLUSH_SIZE, EntrySegmentCache,
    FolioDb, JournalService, LsmcJournal, OffloadPolicy, TieredReader,
};
use futures_util::future::join_all;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;
use tokio::sync::watch;

fn make_entry(ledger_id: u64, entry_id: u64) -> Entry {
    Entry::new(
        ledger_id,
        entry_id,
        entry_id.saturating_sub(1),
        vec![0xCDu8; 1024],
    )
}

// 16 concurrent ledger writers × 625 entries = 10 000 entries, 10 MB total.
const LEDGERS: u64 = 16;
const ENTRIES_PER_LEDGER: u64 = 625;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput benchmark — run manually on real hardware, not in CI"]
async fn lsmc_10k_1kb_concurrent_ledgers_throughput() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&entries_dir).unwrap();

    let db = FolioDb::open(dir.path(), &DbConfig::default()).unwrap();
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
        wal_dir,
        entries_dir,
        entry_cache.clone(),
    )
    .recover()
    .unwrap();

    let block_cache = BlockCache::new(64 * 1024 * 1024);
    let reader = Arc::new(TieredReader::new(
        block_cache,
        entry_index.clone(),
        entry_registry.clone(),
        wal_index.clone(),
        wal_registry.clone(),
        None,
    ));
    let offload = BackgroundOffloader::spawn(
        entry_registry,
        wal_registry,
        wal_index.clone(),
        None,
        OffloadPolicy::default(),
        entry_sealed_rx,
    );
    let svc = Arc::new(JournalService::new_lsmc(
        "bench-node",
        journal,
        wal_index.clone(),
        entry_index.clone(),
        entry_cache,
        reader,
        offload,
        flush_thread,
    ));

    // ── Write: one tokio task per ledger, all concurrent ─────────────────────
    // With LEDGERS senders in-flight, the flush loop drains batches from the
    // channel instead of waking for every individual entry.
    let write_start = Instant::now();
    let write_handles: Vec<_> = (0..LEDGERS)
        .map(|lid| {
            let svc = svc.clone();
            tokio::spawn(async move {
                for i in 0..ENTRIES_PER_LEDGER {
                    let ack = svc
                        .add_entry(make_entry(lid, i))
                        .await
                        .unwrap_or_else(|e| panic!("ledger {lid} entry {i}: {e}"));
                    assert_eq!(ack.ledger_id, lid);
                    assert_eq!(ack.entry_id, i);
                }
            })
        })
        .collect();
    for h in join_all(write_handles).await {
        h.unwrap();
    }
    let write_elapsed = write_start.elapsed();

    // ── Read: concurrent per ledger, verify every entry ───────────────────────
    let read_start = Instant::now();
    let read_handles: Vec<_> = (0..LEDGERS)
        .map(|lid| {
            let svc = svc.clone();
            tokio::spawn(async move {
                for i in 0..ENTRIES_PER_LEDGER {
                    let r = svc
                        .read_entry(lid, i)
                        .await
                        .unwrap_or_else(|e| panic!("read ledger {lid} entry {i}: {e}"));
                    assert_eq!(r.entry.ledger_id, lid);
                    assert_eq!(r.entry.entry_id, i);
                    assert_eq!(r.entry.data[0], 0xCD);
                }
            })
        })
        .collect();
    for h in join_all(read_handles).await {
        h.unwrap();
    }
    let read_elapsed = read_start.elapsed();

    let total = LEDGERS * ENTRIES_PER_LEDGER;
    let bytes = total as f64 * 1024.0;
    let write_mbs = bytes / write_elapsed.as_secs_f64() / 1_048_576.0;
    let read_mbs = bytes / read_elapsed.as_secs_f64() / 1_048_576.0;

    match entry_index.scan_ledger_locations(0) {
        Ok(locations) => println!(
            "entry_index: scan_ledger_locations found {} entries for ledger 0",
            locations.len()
        ),
        Err(e) => panic!("scan_ledger_locations: {e}"),
    }

    println!(
        "write: {total} × 1 KB across {LEDGERS} ledgers in {write_elapsed:?} = {write_mbs:.1} MB/s"
    );
    println!("read:  {total} × 1 KB in {read_elapsed:?} = {read_mbs:.1} MB/s");

    if std::env::var("SKIP_THROUGHPUT_ASSERT").is_err() {
        assert!(
            write_mbs > 50.0,
            "write throughput {write_mbs:.1} MB/s below 50 MB/s floor"
        );
    }
}
