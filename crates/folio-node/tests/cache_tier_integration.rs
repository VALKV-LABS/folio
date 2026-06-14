//! Integration tests for the EntrySegmentCache hot/warm two-tier lifecycle.
//!
//! These tests do NOT require io_uring — they drive the cache manually without
//! running the real flush loop.  This lets them run in Docker environments that
//! do not have io_uring support.
//!
//! What is tested:
//!   1. No read miss during the flush window: after drain_hot_to_warm(), entries
//!      are readable from warm while the async disk write is "in flight".
//!   2. entry_index serves L1 reads after evict_warm(): once evicted from L0,
//!      the entry can be found via entry_index.get_location().
//!   3. New hot entries coexist with warm — both tiers are readable simultaneously.
//!   4. Two consecutive segment lifecycles: evict_warm followed by a fresh hot
//!      fill still works correctly.

use bytes::Bytes;
use folio_node::storage::segment::AlignedBuffer;
use folio_node::{
    BlockCache, DbConfig, ENTRY_FLUSH_SIZE, EntrySegmentCache, FjallIndex, FolioDb, SegmentKind,
    SegmentMeta, SegmentRegistry, SegmentStatus, TieredReader,
};
use std::sync::Arc;
use tempfile::tempdir;

fn make_payload(i: u64) -> Bytes {
    Bytes::from(vec![i as u8; 32])
}

fn open_db(
    dir: &std::path::Path,
) -> (
    Arc<FjallIndex>,      // wal_index
    Arc<FjallIndex>,      // entry_index
    Arc<SegmentRegistry>, // wal_registry
    Arc<SegmentRegistry>, // entry_registry
) {
    let db = FolioDb::open(dir, &DbConfig::default()).unwrap();
    (
        db.wal_index,
        db.entry_index,
        db.wal_registry,
        db.entry_registry,
    )
}

/// Write a fake EntrySegment file using the same AlignedBuffer record format so
/// TieredReader can decode it at a given offset.
///
/// Returns `(offset, length)` for each entry in `entries` order — suitable for
/// passing to `entry_index.insert_location`.
fn write_entry_segment(path: &std::path::Path, entries: &[(u64, u64, Bytes)]) -> Vec<(u64, u32)> {
    let mut buf = AlignedBuffer::new();
    let mut locs = Vec::with_capacity(entries.len());
    let base: u64 = 0;
    for (lid, eid, payload) in entries {
        let (off, len) = buf.push(*lid, *eid, payload);
        locs.push((base + off as u64, len));
    }
    std::fs::write(path, buf.padded_aligned()).unwrap();
    locs
}

// ── Test 1: no read miss during flush window ──────────────────────────────

/// After drain_hot_to_warm(), entries are immediately readable from warm even
/// before any disk write or entry_index update has occurred.
#[test]
fn no_read_miss_during_flush_window() {
    let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);

    // Populate hot.
    for i in 0u64..10 {
        cache.insert(1, i, make_payload(i));
    }

    // drain_hot_to_warm is the critical atomic step — simulates the flush loop.
    let drained = cache.drain_hot_to_warm();
    assert_eq!(drained.len(), 10);

    // Disk write + fdatasync would happen here (async, in flight).
    // Reads must succeed from warm right now, before that completes.
    for i in 0u64..10 {
        assert!(
            cache.get(1, i).is_some(),
            "entry {i} must be readable from warm during flush window"
        );
    }
    assert_eq!(cache.hot_byte_len(), 0, "hot is empty after drain");
}

// ── Test 2: L0 evicted after seal; entry_index has the location ───────────

/// After evict_warm(), entries are gone from L0 but entry_index holds their
/// on-disk location so L1 reads succeed.
#[tokio::test]
async fn l0_eviction_entry_index_serves_l1_reads() {
    let dir = tempdir().unwrap();
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&entries_dir).unwrap();

    let (wal_index, entry_index, wal_registry, entry_registry) = open_db(dir.path());

    // Build entries and write them to a fake segment file.
    let entries: Vec<(u64, u64, Bytes)> = (0u64..5).map(|i| (1, i, make_payload(i))).collect();

    let seg_path = entries_dir.join("entry-0000000000000000.ent");
    let locs = write_entry_segment(&seg_path, &entries);

    // Register the segment in entry_registry so TieredReader can find its path.
    entry_registry
        .upsert(&SegmentMeta {
            id: 0,
            kind: SegmentKind::Entry,
            status: SegmentStatus::Local,
            local_path: Some(seg_path.clone()),
            s3_key: None,
            byte_len: seg_path.metadata().unwrap().len(),
            ledger_sizes: Default::default(),
        })
        .unwrap();

    // Simulate flush: insert locations into entry_index.
    for (i, (offset, length)) in locs.iter().enumerate() {
        entry_index
            .insert_location(1, i as u64, 0, *offset, *length)
            .unwrap();
    }

    // Build a cache that has all entries in warm, then evict.
    let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);
    for (i, (_, _, payload)) in entries.iter().enumerate() {
        cache.insert(1, i as u64, payload.clone());
    }
    cache.drain_hot_to_warm();

    // Sanity: entries are in warm right now.
    for i in 0u64..5 {
        assert!(
            cache.get(1, i).is_some(),
            "entry {i} should be in warm before evict"
        );
    }

    // Seal: evict warm.
    cache.evict_warm();

    // L0 is now empty.
    for i in 0u64..5 {
        assert!(
            cache.get(1, i).is_none(),
            "entry {i} should be gone from L0 after evict"
        );
    }

    // L1 read path: TieredReader must find them via entry_index + local file.
    let reader = TieredReader::new(
        BlockCache::new(8 * 1024 * 1024),
        entry_index.clone(),
        entry_registry.clone(),
        wal_index,
        wal_registry,
        None,
    );

    for i in 0u64..5 {
        let bytes = reader.read_entry(1, i).await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            make_payload(i).as_ref(),
            "L1 payload mismatch at entry {i}"
        );
    }
}

// ── Test 3: new hot entries coexist with warm ─────────────────────────────

/// While some entries are warm (post-drain, pre-seal), new entries written to
/// hot are also immediately readable.
#[test]
fn new_hot_entries_coexist_with_warm() {
    let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);

    // First batch → hot.
    for i in 0u64..5 {
        cache.insert(1, i, make_payload(i));
    }
    cache.drain_hot_to_warm();

    // Second batch → hot (written while first batch is still warm).
    for i in 5u64..10 {
        cache.insert(1, i, make_payload(i));
    }

    // Both warm and hot entries must be readable.
    for i in 0u64..10 {
        assert!(
            cache.get(1, i).is_some(),
            "entry {i} not readable (warm=0..5, hot=5..10)"
        );
    }

    // hot_byte_len tracks only the hot tier.
    assert!(cache.hot_byte_len() > 0, "hot_byte_len must be non-zero");
}

// ── Test 4: two consecutive segment lifecycles ────────────────────────────

/// Seal segment 0 (evict warm), then fill segment 1 (new hot entries).
/// Entries from segment 0 are gone from L0; entries from segment 1 are in hot.
#[test]
fn two_consecutive_segment_lifecycles() {
    let mut cache = EntrySegmentCache::new(ENTRY_FLUSH_SIZE);

    // Segment 0.
    for i in 0u64..5 {
        cache.insert(1, i, make_payload(i));
    }
    cache.drain_hot_to_warm();
    cache.evict_warm(); // seal segment 0

    // Segment 0 entries gone from L0.
    for i in 0u64..5 {
        assert!(
            cache.get(1, i).is_none(),
            "seg-0 entry {i} should be evicted"
        );
    }

    // Segment 1: new hot entries.
    for i in 5u64..10 {
        cache.insert(1, i, make_payload(i));
    }
    for i in 5u64..10 {
        assert!(
            cache.get(1, i).is_some(),
            "seg-1 entry {i} must be readable from hot"
        );
    }

    // Drain seg-1 hot to warm; both tiers coexist within segment 1.
    cache.drain_hot_to_warm();
    for i in 5u64..10 {
        assert!(
            cache.get(1, i).is_some(),
            "seg-1 entry {i} must be readable from warm after drain"
        );
    }
}
