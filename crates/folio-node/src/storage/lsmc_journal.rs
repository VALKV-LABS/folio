//! LSM-C segment journal: interleaves writes from all ledgers into sequential
//! 64–128 MB segment files using O_DIRECT (tokio-uring).
//!
//! # Durability rule
//! An AppendAck is NEVER sent until both the O_DIRECT write_at AND the
//! subsequent fdatasync (sync_data) have returned successfully.  O_DIRECT
//! bypasses the kernel page cache but the drive's own write-back DRAM still
//! requires an explicit flush.  Under heavy load the adaptive batcher coalesces
//! many entries into one write+fdatasync pair; under light load a single entry
//! is written and synced immediately.  Seal (segment rotation) is a separate
//! concern — it renames the file and fsyncs metadata, not individual entries.
//!
//! Adaptive batching: after the first request arrives, drain the channel with
//! try_recv (no waiting) up to MAX_BATCH_BYTES. All entries in the batch share
//! one write_at call; ACKs fire for all of them only after that write returns.
//!
//! # Segment lifecycle
//! Active segment: `{data_dir}/active.seg` (open, O_DIRECT).
//! Sealed segment: `{data_dir}/seg-{id:016x}.seg` (immutable).
//! A tokio watch channel notifies the BackgroundOffloader of each new seal.

use crate::storage::fjall::SegmentRegistry;
use crate::storage::segment::{
    ALIGN, AlignedBuffer, SegmentId, SegmentKind, SegmentMeta, SegmentStatus,
};
use folio_core::error::{FolioError, Result};
use folio_core::metrics::STORAGE;
use std::collections::HashMap;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

pub const DEFAULT_SEAL_THRESHOLD: u64 = 128 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 64 * 1024;

// ── Wire types ────────────────────────────────────────────────────────────

struct WriteReq {
    ledger_id: u64,
    entry_id: u64,
    payload: Vec<u8>,
    ack: oneshot::Sender<Result<(SegmentId, u64, u32)>>,
}

enum JournalMsg {
    Write(WriteReq),
    Seal(oneshot::Sender<Result<SegmentId>>),
}

// ── Public handle ─────────────────────────────────────────────────────────

pub struct LsmcJournal {
    sender: mpsc::Sender<JournalMsg>,
    _thread: thread::JoinHandle<()>,
    /// Watch channel: latest sealed SegmentId; watched by BackgroundOffloader.
    pub sealed_rx: watch::Receiver<Option<SegmentId>>,
    pub data_dir: PathBuf,
}

impl LsmcJournal {
    pub async fn open(
        data_dir: impl AsRef<Path>,
        seal_threshold: u64,
        registry: Arc<SegmentRegistry>,
    ) -> Result<Self> {
        tokio::task::spawn_blocking(probe_io_uring)
            .await
            .map_err(|e| FolioError::Storage(format!("io_uring probe task: {e}")))??;

        let data_dir = data_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&data_dir).await?;

        let next_id = next_wal_segment_id(&data_dir, &registry)?;
        let (tx, rx) = mpsc::channel::<JournalMsg>(4096);
        let (sealed_tx, sealed_rx) = watch::channel::<Option<SegmentId>>(None);
        let dir = data_dir.clone();

        let thread = thread::Builder::new()
            .name("lsmc-journal".into())
            .spawn(move || run_journal(dir, next_id, seal_threshold, rx, sealed_tx, registry))
            .map_err(|e| FolioError::Storage(format!("lsmc-journal: spawn: {e}")))?;

        Ok(Self {
            sender: tx,
            _thread: thread,
            sealed_rx,
            data_dir,
        })
    }

    /// Durably append payload. Blocks until the batch write lands on disk.
    /// Returns (segment_id, file_offset, record_len).
    pub async fn append(
        &self,
        ledger_id: u64,
        entry_id: u64,
        payload: Vec<u8>,
    ) -> Result<(SegmentId, u64, u32)> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .send(JournalMsg::Write(WriteReq {
                ledger_id,
                entry_id,
                payload,
                ack: ack_tx,
            }))
            .await
            .map_err(|_| FolioError::Storage("lsmc-journal: channel closed".into()))?;
        ack_rx
            .await
            .map_err(|_| FolioError::Storage("lsmc-journal: ack channel dropped".into()))?
    }

    pub async fn seal(&self) -> Result<SegmentId> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .send(JournalMsg::Seal(ack_tx))
            .await
            .map_err(|_| FolioError::Storage("lsmc-journal: channel closed".into()))?;
        ack_rx
            .await
            .map_err(|_| FolioError::Storage("lsmc-journal: seal ack dropped".into()))?
    }
}

// ── io_uring availability probe ───────────────────────────────────────────

/// Fail-fast: returns Err if io_uring is blocked (Docker seccomp) or absent
/// (kernel < 5.1). Spawns a probe thread with a no-op tokio_uring runtime;
/// if that panics, surfaces a clear error before touching any segment files.
fn probe_io_uring() -> Result<()> {
    std::thread::spawn(|| tokio_uring::start(async {}))
        .join()
        .map_err(|_| {
            FolioError::Storage(
                "io_uring is not available on this host.\n\
             Requires: Linux kernel ≥ 5.1 with io_uring_setup(2) permitted.\n\
             In Docker: run with --security-opt seccomp=unconfined"
                    .into(),
            )
        })
}

// ── Path helpers ──────────────────────────────────────────────────────────

/// WAL files live under `{data_dir}/wal/`.  These helpers accept that sub-directory.
pub fn active_seg_path(wal_dir: &Path) -> PathBuf {
    wal_dir.join("active.seg")
}
pub fn sealed_seg_path(wal_dir: &Path, id: SegmentId) -> PathBuf {
    wal_dir.join(format!("seg-{id:016x}.seg"))
}

pub fn scan_next_segment_id(dir: &Path) -> Result<SegmentId> {
    let mut max: SegmentId = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let s = name.to_string_lossy();
            if let Some(hex) = s.strip_prefix("seg-").and_then(|s| s.strip_suffix(".seg"))
                && let Ok(id) = u64::from_str_radix(hex, 16)
            {
                max = max.max(id + 1);
            }
        }
    }
    Ok(max)
}

/// Returns the next WAL segment ID that is safe to use.
///
/// Takes the higher of the disk scan (counts surviving local files) and the
/// registry max (counts every segment ever allocated, including those whose
/// local files were deleted after S3 offload).  Using only the disk scan lets
/// IDs wrap back to 0 after offload, which corrupts the Fjall index when the
/// old segment ID is reused.
pub fn next_wal_segment_id(dir: &Path, registry: &SegmentRegistry) -> Result<SegmentId> {
    let disk_max = scan_next_segment_id(dir)?;
    let reg_max = registry.max_id()?.map(|id| id + 1).unwrap_or(0);
    Ok(disk_max.max(reg_max))
}

/// ALIGN-aligned existing size of active.seg (resume offset after crash).
fn resume_offset(dir: &Path) -> u64 {
    let len = std::fs::metadata(active_seg_path(dir))
        .map(|m| m.len())
        .unwrap_or(0);
    (len / ALIGN as u64) * ALIGN as u64
}

// ── Page-aligned allocation for O_DIRECT ─────────────────────────────────

fn make_aligned(data: &[u8]) -> Vec<u8> {
    let aligned_len = (data.len() + ALIGN - 1) & !(ALIGN - 1);
    let layout = std::alloc::Layout::from_size_align(aligned_len, ALIGN)
        .expect("ALIGN is a valid power-of-two");
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    // SAFETY: ptr is valid for aligned_len bytes from the global allocator.
    let mut v = unsafe { Vec::from_raw_parts(ptr, aligned_len, aligned_len) };
    v[..data.len()].copy_from_slice(data);
    v
}

// ── Journal thread ────────────────────────────────────────────────────────

fn run_journal(
    dir: PathBuf,
    start_id: SegmentId,
    threshold: u64,
    rx: mpsc::Receiver<JournalMsg>,
    sealed_tx: watch::Sender<Option<SegmentId>>,
    registry: Arc<SegmentRegistry>,
) {
    tokio_uring::start(async move {
        if let Err(e) = journal_loop(dir, start_id, threshold, rx, sealed_tx, registry).await {
            tracing::error!("lsmc-journal: {e}");
        }
    });
}

async fn journal_loop(
    dir: PathBuf,
    start_id: SegmentId,
    threshold: u64,
    mut rx: mpsc::Receiver<JournalMsg>,
    sealed_tx: watch::Sender<Option<SegmentId>>,
    registry: Arc<SegmentRegistry>,
) -> std::io::Result<()> {
    let mut seg_id = start_id;
    let mut file_offset = resume_offset(&dir);
    let mut file = open_odirect(&dir)?;

    // Register the active segment immediately so TieredReader can locate it
    // for reads that arrive before the first seal.  CrashRecovery may have
    // already inserted it as Local; we overwrite to Active so the offloader
    // never mistakes it for a sealed segment eligible for upload.
    register_active(&registry, seg_id, &dir);

    loop {
        let first = match rx.recv().await {
            Some(m) => m,
            None => break,
        };

        match first {
            JournalMsg::Seal(ack) => {
                match do_seal(&mut file, &dir, seg_id, &sealed_tx, &registry).await {
                    Ok(next_id) => {
                        seg_id = next_id;
                        file_offset = 0;
                        let _ = ack.send(Ok(next_id - 1));
                    }
                    Err(e) => {
                        let _ = ack.send(Err(FolioError::Storage(e.to_string())));
                    }
                }
            }

            JournalMsg::Write(first_req) => {
                let mut batch: Vec<WriteReq> = Vec::new();
                let mut offsets: Vec<(usize, u32)> = Vec::new();
                let mut buf = AlignedBuffer::new();

                let (off, len) =
                    buf.push(first_req.ledger_id, first_req.entry_id, &first_req.payload);
                offsets.push((off, len));
                batch.push(first_req);

                while buf.len() < MAX_BATCH_BYTES {
                    match rx.try_recv() {
                        Ok(JournalMsg::Write(r)) => {
                            let (o, l) = buf.push(r.ledger_id, r.entry_id, &r.payload);
                            offsets.push((o, l));
                            batch.push(r);
                        }
                        Ok(JournalMsg::Seal(ack)) => {
                            let _ = ack
                                .send(Err(FolioError::Storage("seal during batch; retry".into())));
                        }
                        Err(_) => break,
                    }
                }

                // Single O_DIRECT write + fdatasync for the whole batch.
                // O_DIRECT bypasses the kernel page cache but NOT the drive's
                // internal write-back DRAM.  sync_data() flushes that cache so
                // every ACK'd entry is durable against power loss.
                // ACKs are sent ONLY after both write_at and sync_data return.
                let padded = buf.padded_aligned();
                let write_len = padded.len() as u64;
                let write_pos = file_offset;
                let aligned = make_aligned(&padded);

                if let Some(m) = STORAGE.get() {
                    m.batch_size_bytes.set(write_len as i64);
                }
                let write_started = Instant::now();
                let (res, _buf) = file.write_at(aligned, write_pos).await;
                match res {
                    Ok(n) if n < write_len as usize => {
                        let msg = format!(
                            "lsmc-journal: short write at offset {write_pos}: \
                             expected {write_len} bytes, wrote {n}"
                        );
                        for req in batch {
                            let _ = req.ack.send(Err(FolioError::Storage(msg.clone())));
                        }
                    }
                    Ok(_) => {
                        // !! SAFETY-CRITICAL — DO NOT REMOVE !!
                        // O_DIRECT bypasses the kernel page cache but does NOT
                        // flush the drive's internal write-back DRAM cache.
                        // Without this fdatasync, a power failure between write_at
                        // and the next seal can silently lose ACK'd entries even
                        // though write_at returned Ok.  Removing this call breaks
                        // the durability guarantee that every AppendAck carries.
                        match file.sync_data().await {
                            Ok(()) => {
                                if let Some(m) = STORAGE.get() {
                                    m.fsync_duration
                                        .observe(write_started.elapsed().as_secs_f64());
                                }
                                file_offset += write_len;
                                for (req, (buf_off, rec_len)) in batch.into_iter().zip(offsets) {
                                    let _ = req.ack.send(Ok((
                                        seg_id,
                                        write_pos + buf_off as u64,
                                        rec_len,
                                    )));
                                }
                            }
                            Err(e) => {
                                let msg = format!("fdatasync failed: {e}");
                                for req in batch {
                                    let _ = req.ack.send(Err(FolioError::Storage(msg.clone())));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        for req in batch {
                            let _ = req.ack.send(Err(FolioError::Storage(msg.clone())));
                        }
                    }
                }

                if file_offset >= threshold {
                    match do_seal(&mut file, &dir, seg_id, &sealed_tx, &registry).await {
                        Ok(next_id) => {
                            seg_id = next_id;
                            file_offset = 0;
                        }
                        Err(e) => tracing::error!("lsmc-journal auto-seal: {e}"),
                    }
                }
            }
        }
    }
    Ok(())
}

fn open_odirect(dir: &Path) -> std::io::Result<tokio_uring::fs::File> {
    let std_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT)
        .open(active_seg_path(dir))?;
    Ok(tokio_uring::fs::File::from_std(std_file))
}

async fn do_seal(
    file: &mut tokio_uring::fs::File,
    dir: &Path,
    seg_id: SegmentId,
    sealed_tx: &watch::Sender<Option<SegmentId>>,
    registry: &Arc<SegmentRegistry>,
) -> std::io::Result<SegmentId> {
    file.sync_all().await?;
    let sealed_path = sealed_seg_path(dir, seg_id);
    std::fs::rename(active_seg_path(dir), &sealed_path)?;

    // Update registry: segment transitions from Active → Local with its permanent path.
    let byte_len = std::fs::metadata(&sealed_path)
        .map(|m| m.len())
        .unwrap_or(0);
    if let Err(e) = registry.upsert(&SegmentMeta {
        id: seg_id,
        kind: SegmentKind::Wal,
        status: SegmentStatus::Local,
        local_path: Some(sealed_path),
        s3_key: None,
        byte_len,
        ledger_sizes: HashMap::new(),
    }) {
        tracing::error!("lsmc-journal: registry update for sealed segment {seg_id}: {e}");
    }

    let _ = sealed_tx.send(Some(seg_id));
    *file = open_odirect(dir)?;

    let next_id = seg_id + 1;
    register_active(registry, next_id, dir);
    Ok(next_id)
}

fn register_active(registry: &Arc<SegmentRegistry>, seg_id: SegmentId, dir: &Path) {
    if let Err(e) = registry.upsert(&SegmentMeta {
        id: seg_id,
        kind: SegmentKind::Wal,
        status: SegmentStatus::Active,
        local_path: Some(active_seg_path(dir)),
        s3_key: None,
        byte_len: 0,
        ledger_sizes: HashMap::new(),
    }) {
        tracing::error!("lsmc-journal: failed to register active segment {seg_id}: {e}");
    }
}
