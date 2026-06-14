//! Space-efficiency test for the EntrySegment layer (M1).
//!
//! The LSM-C design eliminates the O_DIRECT alignment waste that plagued the
//! WAL-only path.  WAL entries are individually padded to 4096 bytes (O_DIRECT
//! requirement), so a 100-byte entry wastes 97.6%.  EntrySegments batch up to
//! ENTRY_FLUSH_SIZE (64 KB) before one aligned write, trimming waste to
//! ≤ 4095/65536 ≈ 6.25% of *file size* (≤ ~10% of *payload* when framing is
//! included).
//!
//! This test writes a realistic batch of small entries to a real EntrySegment
//! file and asserts:
//!   1. File size is within the padding bound (≤ payload + framing + ALIGN − 1).
//!   2. Payload retrieval round-trips correctly.
//!
//! Requires `tokio_uring::start` (available wherever tokio-uring is enabled).

use bytes::Bytes;
use folio_node::storage::entry_segment::{
    ENTRY_FLUSH_SIZE, EntrySegmentWriter, entry_seg_path, read_record_at,
};
use folio_node::storage::segment::RECORD_OVERHEAD;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

const PAYLOAD_SIZE: usize = 100; // 100-byte entries — small, realistic
const NUM_ENTRIES: usize = 200; // 200 × 100 = 20 KB payload, one flush window

#[test]
fn entry_segment_alignment_waste_within_bound() {
    let dir = tempdir().unwrap();
    let entries_dir = dir.path().join("entries");
    std::fs::create_dir_all(&entries_dir).unwrap();

    let batch: Vec<(u64, u64, Bytes)> = (0..NUM_ENTRIES as u64)
        .map(|i| (1u64, i, Bytes::from(vec![(i & 0xFF) as u8; PAYLOAD_SIZE])))
        .collect();

    let offsets_out: Arc<Mutex<Vec<(u64, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let file_size_out: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

    {
        let entries_dir = entries_dir.clone();
        let batch = batch.clone();
        let offsets_cap = offsets_out.clone();
        let size_cap = file_size_out.clone();
        tokio_uring::start(async move {
            let mut writer = EntrySegmentWriter::create(&entries_dir, 0).unwrap();
            let offs = writer.write_batch(&batch).await.unwrap();
            writer.sync().await.unwrap();
            *size_cap.lock().unwrap() = writer.byte_len();
            *offsets_cap.lock().unwrap() = offs;
        });
    }

    let offsets = Arc::try_unwrap(offsets_out).unwrap().into_inner().unwrap();
    let file_size = *file_size_out.lock().unwrap();

    let payload_total: usize = NUM_ENTRIES * PAYLOAD_SIZE;
    let framing_total: usize = NUM_ENTRIES * RECORD_OVERHEAD;
    let data_bytes: usize = payload_total + framing_total;

    // File must not exceed data rounded up to next 4 KB boundary.
    let aligned_bound = (data_bytes + 4095) & !4095;
    assert!(
        file_size as usize <= aligned_bound,
        "file size {file_size} > aligned bound {aligned_bound} \
         (data={data_bytes}, payload={payload_total}, framing={framing_total})"
    );

    let waste = file_size as usize - data_bytes;
    assert!(
        waste < 4096,
        "alignment waste {waste} B ≥ 4096 — records are not being packed within a batch"
    );

    // Compare with WAL-only: each entry padded independently to 4096 B.
    let wal_equivalent = NUM_ENTRIES * 4096;
    let space_saving = 1.0 - (file_size as f64 / wal_equivalent as f64);
    println!(
        "entry_segment_space: file={file_size} B payload={payload_total} B \
         framing={framing_total} B waste={waste} B \
         WAL-equiv={wal_equivalent} B saving={:.1}%",
        space_saving * 100.0
    );
    assert!(
        space_saving > 0.50,
        "expected >50% space saving vs WAL-only, got {:.1}%",
        space_saving * 100.0
    );

    // Round-trip every entry through the on-disk read path.
    let path = entry_seg_path(&entries_dir, 0);
    tokio_uring::start(async move {
        for (i, (offset, length)) in offsets.iter().enumerate() {
            let payload = read_record_at(&path, *offset, *length).await.unwrap();
            assert_eq!(
                payload.len(),
                PAYLOAD_SIZE,
                "entry {i}: wrong payload length"
            );
            assert_eq!(payload[0], (i & 0xFF) as u8, "entry {i}: wrong content");
        }
    });
}

#[test]
fn entry_flush_size_bound_keeps_waste_below_7_percent() {
    // Calculation test (no I/O): validates the ENTRY_FLUSH_SIZE constant keeps
    // alignment waste < 7% for 1 KB entries at the flush threshold.
    let payload_per_entry: usize = 1024;
    let record_per_entry: usize = payload_per_entry + RECORD_OVERHEAD;
    let n_entries: usize = ENTRY_FLUSH_SIZE / record_per_entry;
    let data_bytes: usize = n_entries * record_per_entry;
    let padding: usize = (4096 - (data_bytes % 4096)) % 4096;
    let file_size: usize = data_bytes + padding;

    let waste_pct = padding as f64 / file_size as f64 * 100.0;
    println!(
        "waste_bound: n={n_entries} entries data={data_bytes} B \
         padding={padding} B file={file_size} B waste={waste_pct:.2}%"
    );
    assert!(
        waste_pct < 7.0,
        "alignment waste {waste_pct:.2}% ≥ 7% — ENTRY_FLUSH_SIZE may be too small"
    );
}
