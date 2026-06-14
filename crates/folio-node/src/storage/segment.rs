//! Shared segment primitives: record framing, aligned write buffer, index value.
//!
//! Record layout on disk:
//!   [u32 LE payload_len][u64 BE ledger_id][u64 BE entry_id][payload bytes][u32 LE CRC32]
//!
//! CRC32 covers the four preceding fields (len + ledger_id + entry_id + payload).
//! A payload_len of zero signals an O_DIRECT padding block; crash recovery skips to
//! the next ALIGN boundary when it encounters one.

use bytes::Bytes;
use crc32fast::Hasher as CrcHasher;
use folio_core::error::{FolioError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

pub type SegmentId = u64;

pub const RECORD_HEADER: usize = 20; // 4 (len) + 8 (ledger_id) + 8 (entry_id)
pub const RECORD_FOOTER: usize = 4; // CRC32
pub const RECORD_OVERHEAD: usize = RECORD_HEADER + RECORD_FOOTER; // 24 total

/// O_DIRECT block alignment — buffer base, file offset, and write length must be multiples.
pub const ALIGN: usize = 4096;
/// Read-ahead block size for L2 local reads and L3 S3 range requests.
pub const BLOCK_SIZE: usize = 256 * 1024;

// ── SegmentMeta / SegmentStatus ────────────────────────────────────────────

/// Distinguishes WAL segments (durability layer, entries flushed then GC'd)
/// from EntrySegments (compacted storage layer, permanent until S3 offload).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentKind {
    /// Write-ahead log segment: written on every append, GC'd after EntrySegment flush.
    Wal,
    /// Compacted entry segment: written by the flush task, read by TieredReader.
    Entry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentStatus {
    /// Sealed segment file exists locally; not yet offloaded.
    Local,
    /// S3 upload in progress; local file still present.
    Offloading,
    /// Uploaded to S3; local file deleted.
    S3,
    /// Deleted everywhere (tombstone entry, kept for audit).
    Deleted,
    /// Currently being written to (`active.seg`). Never eligible for offload.
    /// Transitions to `Local` when the journal seals and renames the file.
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: SegmentId,
    pub kind: SegmentKind,
    pub status: SegmentStatus,
    pub local_path: Option<PathBuf>,
    pub s3_key: Option<String>,
    /// Byte count of the sealed segment (0 while still active).
    pub byte_len: u64,
    /// On-disk bytes attributed to each ledger (payload + record overhead).
    /// Accumulated incrementally during entry flush; used by GC to score
    /// segments for compaction once a ledger is deleted.
    /// Empty for WAL segments (short-lived; not worth tracking).
    #[serde(default)]
    pub ledger_sizes: HashMap<u64, u64>,
}

// ── IndexValue ────────────────────────────────────────────────────────────

/// Stored in Fjall for each `(ledger_id, entry_id)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexValue {
    pub segment_id: SegmentId,
    /// Byte offset of the record start within the segment file.
    pub offset: u64,
    /// Total record bytes = RECORD_OVERHEAD + payload.len().
    pub length: u32,
}

impl IndexValue {
    /// Returns the (start, end) byte range of the payload within a `data` slice
    /// that was read starting at `block_start` in the segment file.
    pub fn payload_range_in_block(&self, block_start: u64) -> (usize, usize) {
        let rec_start = (self.offset - block_start) as usize;
        let pay_start = rec_start + RECORD_HEADER;
        let pay_end = pay_start + (self.length as usize - RECORD_OVERHEAD);
        (pay_start, pay_end)
    }
}

// ── AlignedBuffer ─────────────────────────────────────────────────────────

/// In-memory accumulation buffer. Records are pushed here and flushed to disk
/// in ALIGN-padded blocks for O_DIRECT compatibility.
pub struct AlignedBuffer {
    inner: Vec<u8>,
}

impl Default for AlignedBuffer {
    fn default() -> Self {
        Self {
            inner: Vec::with_capacity(ALIGN * 8),
        }
    }
}

impl AlignedBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a record; returns `(offset_within_buffer, total_record_bytes)`.
    pub fn push(&mut self, ledger_id: u64, entry_id: u64, payload: &[u8]) -> (usize, u32) {
        let start = self.inner.len();
        let payload_len = payload.len() as u32;

        self.inner.extend_from_slice(&payload_len.to_le_bytes());
        self.inner.extend_from_slice(&ledger_id.to_be_bytes());
        self.inner.extend_from_slice(&entry_id.to_be_bytes());
        self.inner.extend_from_slice(payload);

        let mut h = CrcHasher::new();
        h.update(&payload_len.to_le_bytes());
        h.update(&ledger_id.to_be_bytes());
        h.update(&entry_id.to_be_bytes());
        h.update(payload);
        self.inner.extend_from_slice(&h.finalize().to_le_bytes());

        (start, (RECORD_OVERHEAD + payload.len()) as u32)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn clear(&mut self) {
        self.inner.clear();
    }
    /// Raw bytes without alignment padding.
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }

    /// Clone with zero-padding to the next ALIGN boundary (required for O_DIRECT writes).
    pub fn padded_aligned(&self) -> Vec<u8> {
        let aligned = (self.inner.len() + ALIGN - 1) & !(ALIGN - 1);
        let mut v = self.inner.clone();
        v.resize(aligned, 0);
        v
    }
}

// ── Record decoder ─────────────────────────────────────────────────────────

/// Decode one record from `data` at byte position `pos`.
///
/// Returns `(ledger_id, entry_id, payload, record_len)`.
/// Errors on truncation or CRC mismatch (torn write).
pub fn decode_record(data: &[u8], pos: usize) -> Result<(u64, u64, Bytes, usize)> {
    if pos.checked_add(4).is_none_or(|end| end > data.len()) {
        return Err(FolioError::Storage(
            "torn write: truncated length prefix".into(),
        ));
    }
    let len_bytes: [u8; 4] = data[pos..pos + 4]
        .try_into()
        .map_err(|_| FolioError::Storage("torn write: malformed length prefix".into()))?;
    let payload_len = u32::from_le_bytes(len_bytes) as usize;
    let total = RECORD_OVERHEAD
        .checked_add(payload_len)
        .ok_or_else(|| FolioError::Storage("torn write: record length overflow".into()))?;

    if pos.checked_add(total).is_none_or(|end| end > data.len()) {
        return Err(FolioError::Storage(format!(
            "torn write: record at {pos} needs {total} bytes, {} available",
            data.len() - pos,
        )));
    }

    let ledger_id = u64::from_be_bytes(
        data[pos + 4..pos + 12]
            .try_into()
            .map_err(|_| FolioError::Storage("torn write: malformed ledger id".into()))?,
    );
    let entry_id = u64::from_be_bytes(
        data[pos + 12..pos + 20]
            .try_into()
            .map_err(|_| FolioError::Storage("torn write: malformed entry id".into()))?,
    );
    let payload = &data[pos + 20..pos + 20 + payload_len];
    let stored_crc = u32::from_le_bytes(
        data[pos + 20 + payload_len..pos + total]
            .try_into()
            .map_err(|_| FolioError::Storage("torn write: malformed checksum".into()))?,
    );

    let mut h = CrcHasher::new();
    h.update(&(payload_len as u32).to_le_bytes());
    h.update(&ledger_id.to_be_bytes());
    h.update(&entry_id.to_be_bytes());
    h.update(payload);
    let computed = h.finalize();

    if stored_crc != computed {
        return Err(FolioError::Storage(format!(
            "torn write: CRC mismatch at {pos}: stored={stored_crc:#010x} computed={computed:#010x}",
        )));
    }

    Ok((ledger_id, entry_id, Bytes::copy_from_slice(payload), total))
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_buffer_round_trip() {
        let mut buf = AlignedBuffer::new();
        let mut offsets = Vec::new();
        for i in 0u64..10 {
            let (off, len) = buf.push(1, i, &vec![i as u8; 32]);
            offsets.push((off, len));
        }
        let data = buf.padded_aligned();
        for (i, (off, len)) in offsets.iter().enumerate() {
            let (lid, eid, payload, rec_len) = decode_record(&data, *off).unwrap();
            assert_eq!(lid, 1);
            assert_eq!(eid, i as u64);
            assert_eq!(&payload[..], &vec![i as u8; 32]);
            assert_eq!(rec_len, *len as usize);
        }
    }

    #[test]
    fn torn_write_truncated_length() {
        let mut buf = AlignedBuffer::new();
        buf.push(1, 0, b"hello");
        let mut data = buf.padded_aligned();
        data.truncate(2);
        assert!(decode_record(&data, 0).is_err());
    }

    #[test]
    fn torn_write_truncated_payload() {
        let mut buf = AlignedBuffer::new();
        buf.push(1, 0, b"hello world");
        let mut data = buf.padded_aligned();
        // Truncate inside the payload
        data.truncate(RECORD_HEADER + 3);
        assert!(decode_record(&data, 0).is_err());
    }

    #[test]
    fn torn_write_crc_mismatch() {
        let mut buf = AlignedBuffer::new();
        buf.push(1, 0, b"hello");
        let mut data = buf.padded_aligned();
        // Corrupt one payload byte
        data[RECORD_HEADER + 1] ^= 0xFF;
        assert!(decode_record(&data, 0).is_err());
    }

    #[test]
    fn multiple_records_at_sequential_offsets() {
        let mut buf = AlignedBuffer::new();
        let (off0, len0) = buf.push(10, 0, b"aaa");
        let (off1, len1) = buf.push(10, 1, b"bbbbb");
        assert_eq!(off1, off0 + len0 as usize);
        let data = buf.padded_aligned();
        let (_, eid0, p0, _) = decode_record(&data, off0).unwrap();
        let (_, eid1, p1, _) = decode_record(&data, off1).unwrap();
        assert_eq!((eid0, &p0[..]), (0, b"aaa".as_ref()));
        assert_eq!((eid1, &p1[..]), (1, b"bbbbb".as_ref()));
        assert_eq!(len1 as usize, RECORD_OVERHEAD + 5);
    }
}
