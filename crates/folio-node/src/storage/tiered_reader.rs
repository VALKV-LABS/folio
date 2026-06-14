//! Four-tier read path for the LSM-C storage engine.
//!
//! L0 → EntrySegmentCache (in-memory; checked in JournalService::read_entry before calling here)
//! L1 → Moka block cache (RAM) + local EntrySegment file
//! L2 → local WAL file (entries not yet flushed; only populated after crash recovery)
//! L3 → S3 range request (aws-sdk-s3; optional)
//!
//! The caller (JournalService) handles L0. TieredReader owns L1–L3.
//!
//! Index routing (no SegmentKind on IndexValue):
//!   - entry_index hit → read from entry_registry path (EntrySegment), with block cache.
//!   - wal_index hit   → read from wal_registry path (WAL file), no block cache.
//!   - both miss       → EntryNotFound (or S3 via entry_registry if offloaded).

use crate::storage::block_cache::BlockCache;
use crate::storage::fjall::{FjallIndex, SegmentRegistry};
use crate::storage::segment::{BLOCK_SIZE, SegmentStatus, decode_record};
use bytes::Bytes;
use folio_core::error::{FolioError, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    pub endpoint: Option<String>, // None = AWS default; Some = MinIO / custom
    pub region: String,
}

pub struct TieredReader {
    cache: BlockCache,
    /// Permanent EntrySegment index (flush task writes here).
    entry_index: Arc<FjallIndex>,
    entry_registry: Arc<SegmentRegistry>,
    /// WAL index — populated only by CrashRecovery for unflushed entries.
    wal_index: Arc<FjallIndex>,
    wal_registry: Arc<SegmentRegistry>,
    s3: Option<(S3Config, aws_sdk_s3::Client)>,
}

impl TieredReader {
    pub fn new(
        cache: BlockCache,
        entry_index: Arc<FjallIndex>,
        entry_registry: Arc<SegmentRegistry>,
        wal_index: Arc<FjallIndex>,
        wal_registry: Arc<SegmentRegistry>,
        s3: Option<(S3Config, aws_sdk_s3::Client)>,
    ) -> Self {
        Self {
            cache,
            entry_index,
            entry_registry,
            wal_index,
            wal_registry,
            s3,
        }
    }

    /// Read the payload bytes for `(ledger_id, entry_id)`.
    /// L0 (cache) is handled by the caller; this covers L1–L3.
    pub async fn read_entry(&self, ledger_id: u64, entry_id: u64) -> Result<Bytes> {
        // L1: EntrySegment (permanent store, with block cache).
        if let Some(iv) = self.entry_index.get_location(ledger_id, entry_id)? {
            let meta = self.entry_registry.get(iv.segment_id)?.ok_or_else(|| {
                FolioError::Storage(format!("entry segment {} not in registry", iv.segment_id))
            })?;

            return match meta.status {
                SegmentStatus::Active | SegmentStatus::Local | SegmentStatus::Offloading => {
                    let path = meta.local_path.clone().ok_or_else(|| {
                        FolioError::Storage(format!(
                            "entry segment {} has no local_path",
                            iv.segment_id
                        ))
                    })?;
                    // Under normal operation, Active-segment entries are always served
                    // from the L0 warm cache before reaching here, so this branch is
                    // only hit after crash recovery (when cache was lost).  In that case
                    // the orphan-handling in flush_loop has already transitioned the
                    // segment to Local, so Active should be rare in practice.
                    // Only cache sealed segments (Local, Offloading) — their data is
                    // immutable, so blocks are safe to cache indefinitely.
                    let cacheable = !matches!(meta.status, SegmentStatus::Active);
                    let block_start = BlockCache::block_start(iv.offset);
                    let block = self
                        .get_entry_block(iv.segment_id, &path, block_start, cacheable)
                        .await?;

                    let rec_end = (iv.offset + iv.length as u64) as usize;
                    let block_end = (block_start + BLOCK_SIZE as u64) as usize;
                    let data: Bytes = if rec_end <= block_end {
                        block
                    } else {
                        let block2_start = block_start + BLOCK_SIZE as u64;
                        let block2 = self
                            .get_entry_block(iv.segment_id, &path, block2_start, cacheable)
                            .await?;
                        let mut combined =
                            bytes::BytesMut::with_capacity(block.len() + block2.len());
                        combined.extend_from_slice(&block);
                        combined.extend_from_slice(&block2);
                        combined.freeze()
                    };

                    let rec_off = (iv.offset - block_start) as usize;
                    let (_, _, payload, _) = decode_record(&data, rec_off)?;
                    Ok(payload)
                }
                SegmentStatus::S3 => {
                    let s3_key = meta.s3_key.clone().ok_or_else(|| {
                        FolioError::Storage(format!(
                            "entry segment {} has no s3_key",
                            iv.segment_id
                        ))
                    })?;
                    self.read_s3_record(&s3_key, iv.offset, iv.length as u64)
                        .await
                }
                SegmentStatus::Deleted => Err(FolioError::Storage(format!(
                    "entry segment {} is deleted",
                    iv.segment_id
                ))),
            };
        }

        // defensive code will be read from l0 cache

        // L2: WAL (populated only after crash recovery for unflushed entries).
        if let Some(iv) = self.wal_index.get_location(ledger_id, entry_id)? {
            let block_start = BlockCache::block_start(iv.offset);
            let block = self.get_wal_block(iv.segment_id, block_start).await?;

            let rec_end = (iv.offset + iv.length as u64) as usize;
            let block_end = (block_start + BLOCK_SIZE as u64) as usize;
            let data: Bytes = if rec_end <= block_end {
                block
            } else {
                let block2_start = block_start + BLOCK_SIZE as u64;
                let block2 = self.get_wal_block(iv.segment_id, block2_start).await?;
                let mut combined = bytes::BytesMut::with_capacity(block.len() + block2.len());
                combined.extend_from_slice(&block);
                combined.extend_from_slice(&block2);
                combined.freeze()
            };

            let rec_off = (iv.offset - block_start) as usize;
            let (_, _, payload, _) = decode_record(&data, rec_off)?;
            return Ok(payload);
        }

        Err(FolioError::EntryNotFound {
            ledger_id,
            entry_id,
        })
    }

    // ── EntrySegment block read (with block cache) ────────────────────────────

    async fn get_entry_block(
        &self,
        seg_id: u64,
        path: &Path,
        block_start: u64,
        cacheable: bool,
    ) -> Result<Bytes> {
        if cacheable && let Some(cached) = self.cache.get(seg_id, block_start).await {
            return Ok(cached);
        }
        let block = self
            .read_local_block(path.to_path_buf(), block_start)
            .await?;
        if cacheable {
            self.cache.insert(seg_id, block_start, block.clone()).await;
        }
        Ok(block)
    }

    // ── WAL block read (no block cache — WAL reads are rare, post-crash only) ──

    async fn get_wal_block(&self, seg_id: u64, block_start: u64) -> Result<Bytes> {
        let meta = self
            .wal_registry
            .get(seg_id)?
            .ok_or_else(|| FolioError::Storage(format!("WAL segment {seg_id} not in registry")))?;

        match meta.status {
            SegmentStatus::Active | SegmentStatus::Local | SegmentStatus::Offloading => {
                let path = meta.local_path.clone().ok_or_else(|| {
                    FolioError::Storage(format!("WAL segment {seg_id} has no local_path"))
                })?;
                self.read_local_block(path, block_start).await
            }
            SegmentStatus::S3 => {
                // WAL segments are not uploaded to S3 (only EntrySegments are).
                // This path should not be reached in the current design.
                Err(FolioError::Storage(format!(
                    "WAL segment {seg_id} is on S3 (unexpected)"
                )))
            }
            SegmentStatus::Deleted => Err(FolioError::Storage(format!(
                "WAL segment {seg_id} is deleted"
            ))),
        }
    }

    // ── Shared helpers ────────────────────────────────────────────────────

    async fn read_local_block(&self, path: PathBuf, block_start: u64) -> Result<Bytes> {
        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            let file = std::fs::File::open(&path)
                .map_err(|e| FolioError::Storage(format!("open {}: {e}", path.display())))?;
            let mut buf = vec![0u8; BLOCK_SIZE];
            let n = file
                .read_at(&mut buf, block_start)
                .map_err(|e| FolioError::Storage(format!("read_at {block_start}: {e}")))?;
            buf.truncate(n);
            Ok::<Bytes, FolioError>(Bytes::from(buf))
        })
        .await
        .map_err(|e| FolioError::Storage(format!("spawn_blocking: {e}")))?
    }

    async fn read_s3_record(&self, s3_key: &str, offset: u64, length: u64) -> Result<Bytes> {
        let (cfg, client) = self
            .s3
            .as_ref()
            .ok_or_else(|| FolioError::Storage("S3 read required but no S3 configured".into()))?;

        let end = offset + length - 1;
        let range = format!("bytes={offset}-{end}");

        let resp = client
            .get_object()
            .bucket(&cfg.bucket)
            .key(s3_key)
            .range(range)
            .send()
            .await
            .map_err(|e| FolioError::Storage(format!("S3 get_object: {e}")))?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| FolioError::Storage(format!("S3 body collect: {e}")))?
            .into_bytes();

        // Decode the record from the S3 bytes (the range covers exactly one record).
        let (_, _, payload, _) = decode_record(&data, 0)?;
        Ok(payload)
    }
}

// ── S3 client builder ─────────────────────────────────────────────────────

/// Build an `aws_sdk_s3::Client` from the given config. Sets `force_path_style`
/// when an explicit endpoint is provided (required for MinIO).
pub async fn build_s3_client(cfg: &S3Config) -> aws_sdk_s3::Client {
    let mut loader = aws_config::from_env().region(aws_config::Region::new(cfg.region.clone()));
    if let Some(ep) = &cfg.endpoint {
        loader = loader.endpoint_url(ep);
    }
    let base = loader.load().await;
    let s3_cfg = aws_sdk_s3::config::Builder::from(&base)
        .force_path_style(cfg.endpoint.is_some()) // MinIO requires path-style
        .build();
    aws_sdk_s3::Client::from_conf(s3_cfg)
}
