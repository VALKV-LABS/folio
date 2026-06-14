//! EntrySegment: the packed, long-term storage layer.
//!
//! Files live in `{data_dir}/entries/entry-{id:016x}.ent`.
//! Writes use O_DIRECT (tokio_uring) with the shared `AlignedBuffer` — identical
//! to the WAL write path, so alignment waste is ≤ 4095 / ENTRY_FLUSH_SIZE ≈ 6%
//! at the 64 KB flush threshold.  Reads use buffered pread (no O_DIRECT needed;
//! the block cache absorbs the re-read cost).

use crate::storage::segment::{ALIGN, AlignedBuffer, SegmentId, decode_record};
use bytes::Bytes;
use folio_core::error::{FolioError, Result};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub const ENTRY_FLUSH_SIZE: usize = 64 * 1024; // 64 KB flush threshold

// ── Path helpers ──────────────────────────────────────────────────────────

pub fn entry_seg_path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(format!("entry-{id:016x}.ent"))
}

/// Scan existing `.ent` files and return the next unused ID (max_seen + 1).
pub fn scan_next_entry_seg_id(dir: &Path) -> Result<SegmentId> {
    let mut max: SegmentId = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let s = name.to_string_lossy();
            if let Some(hex) = s
                .strip_prefix("entry-")
                .and_then(|s| s.strip_suffix(".ent"))
                && let Ok(id) = u64::from_str_radix(hex, 16)
            {
                max = max.max(id + 1);
            }
        }
    }
    Ok(max)
}

/// Returns the next entry segment ID that is safe to use.
///
/// Same logic as `next_wal_segment_id`: takes the higher of the disk scan and
/// the registry max so that IDs whose local files were deleted after S3 offload
/// are never reused.
pub fn next_entry_seg_id(
    dir: &Path,
    registry: &crate::storage::fjall::SegmentRegistry,
) -> Result<SegmentId> {
    let disk_max = scan_next_entry_seg_id(dir)?;
    let reg_max = registry.max_id()?.map(|id| id + 1).unwrap_or(0);
    Ok(disk_max.max(reg_max))
}

// ── Writer ────────────────────────────────────────────────────────────────

/// Writes a single EntrySegment file using O_DIRECT via `tokio_uring`.
/// Must be driven from within a `tokio_uring::start` context (the flush thread).
pub struct EntrySegmentWriter {
    id: SegmentId,
    path: PathBuf,
    file: tokio_uring::fs::File,
    file_offset: u64,
}

impl EntrySegmentWriter {
    /// Create a new EntrySegment file in `dir`.  Opens with O_DIRECT | O_CREAT.
    pub fn create(dir: &Path, id: SegmentId) -> Result<Self> {
        let path = entry_seg_path(dir, id);
        let std_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .map_err(|e| {
                FolioError::Storage(format!("entry segment create {}: {e}", path.display()))
            })?;
        let file = tokio_uring::fs::File::from_std(std_file);
        Ok(Self {
            id,
            path,
            file,
            file_offset: 0,
        })
    }

    /// Write a batch of `(ledger_id, entry_id, payload)` entries as one O_DIRECT write.
    /// Returns `(file_offset, record_len)` for each entry in the same order.
    pub async fn write_batch(&mut self, entries: &[(u64, u64, Bytes)]) -> Result<Vec<(u64, u32)>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let base_pos = self.file_offset;
        let mut buf = AlignedBuffer::new();
        let mut per_entry: Vec<(u64, u32)> = Vec::with_capacity(entries.len());

        for (lid, eid, payload) in entries {
            let (buf_off, rec_len) = buf.push(*lid, *eid, payload);
            per_entry.push((base_pos + buf_off as u64, rec_len));
        }

        let padded = buf.padded_aligned();
        let write_len = padded.len() as u64;
        let aligned = make_aligned(&padded);

        let (res, _) = self.file.write_at(aligned, base_pos).await;
        match res {
            Ok(n) if n < write_len as usize => {
                return Err(FolioError::Storage(format!(
                    "entry segment short write at offset {base_pos}: \
                     expected {write_len} bytes, wrote {n}"
                )));
            }
            Ok(_) => {}
            Err(e) => return Err(FolioError::Storage(format!("entry segment write_at: {e}"))),
        }

        self.file_offset += write_len;
        Ok(per_entry)
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.file
            .sync_all()
            .await
            .map_err(|e| FolioError::Storage(format!("entry segment sync: {e}")))
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn byte_len(&self) -> u64 {
        self.file_offset
    }
}

// ── Reader ────────────────────────────────────────────────────────────────

/// Read a single record from an EntrySegment at the given offset+length.
/// Returns only the entry payload (strips the record header and CRC).
pub async fn read_record_at(path: &Path, offset: u64, length: u32) -> Result<Bytes> {
    let path = path.to_path_buf();
    let data = tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        let file = std::fs::File::open(&path)
            .map_err(|e| FolioError::Storage(format!("open {}: {e}", path.display())))?;
        let mut buf = vec![0u8; length as usize];
        let n = file
            .read_at(&mut buf, offset)
            .map_err(|e| FolioError::Storage(format!("read_at {offset}: {e}")))?;
        if n < length as usize {
            return Err(FolioError::Storage(format!(
                "entry segment short read at {offset}: expected {length} got {n}"
            )));
        }
        Ok::<Vec<u8>, FolioError>(buf)
    })
    .await
    .map_err(|e| FolioError::Storage(format!("spawn_blocking: {e}")))??;

    let (_, _, payload, _) = decode_record(&data, 0)?;
    Ok(payload)
}

// ── Aligned allocation (mirrors lsmc_journal) ─────────────────────────────

fn make_aligned(data: &[u8]) -> Vec<u8> {
    let aligned_len = (data.len() + ALIGN - 1) & !(ALIGN - 1);
    let layout = std::alloc::Layout::from_size_align(aligned_len, ALIGN)
        .expect("ALIGN is a valid power-of-two");
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    let mut v = unsafe { Vec::from_raw_parts(ptr, aligned_len, aligned_len) };
    v[..data.len()].copy_from_slice(data);
    v
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::tempdir;

    fn run(f: impl std::future::Future<Output = ()>) {
        tokio_uring::start(async move { f.await });
    }

    #[test]
    fn write_batch_and_read_back() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("entries")).unwrap();
        let entries_dir = dir.path().join("entries");

        run(async {
            let mut writer = EntrySegmentWriter::create(&entries_dir, 0).unwrap();

            let batch: Vec<(u64, u64, Bytes)> = (0u64..10)
                .map(|i| (1u64, i, Bytes::from(vec![i as u8; 64])))
                .collect();

            let offsets = writer.write_batch(&batch).await.unwrap();
            writer.sync().await.unwrap();

            assert_eq!(offsets.len(), 10);

            let path = entry_seg_path(&entries_dir, 0);
            for (i, (offset, length)) in offsets.iter().enumerate() {
                let payload = read_record_at(&path, *offset, *length).await.unwrap();
                assert_eq!(payload.len(), 64);
                assert_eq!(payload[0], i as u8);
            }
        });
    }

    #[test]
    fn scan_next_entry_seg_id_empty_dir() {
        let dir = tempdir().unwrap();
        assert_eq!(scan_next_entry_seg_id(dir.path()).unwrap(), 0);
    }

    #[test]
    fn scan_next_entry_seg_id_existing_files() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("entry-0000000000000000.ent"), b"").unwrap();
        std::fs::write(dir.path().join("entry-0000000000000003.ent"), b"").unwrap();
        assert_eq!(scan_next_entry_seg_id(dir.path()).unwrap(), 4);
    }
}
