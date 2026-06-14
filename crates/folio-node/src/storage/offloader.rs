//! Background offloader: uploads sealed EntrySegments to S3 and GC's WAL segments.
//!
//! Triggered by the flush task's `entry_sealed_rx` watch channel.  Each newly
//! sealed EntrySegment ID wakes the task; it then:
//!   1. Uploads eligible EntrySegments to S3 (when S3 is configured).
//!   2. GC's WAL segments regardless of S3 — any WAL segment whose entries
//!      have all been flushed to entry_index is eligible for deletion once it
//!      is older than `max(policy.min_age, DEFAULT_WAL_MIN_AGE)`.
//!
//! WAL GC runs even when S3 is not configured.  Without this, WAL segments
//! would accumulate forever even after their entries are safely in EntrySegments.
//!
//! Eligibility is controlled by two independent conditions (either triggers):
//!   • `min_age`         — segment file is at least this old (mtime-based).
//!   • `max_local_bytes` — total Local EntrySegment bytes exceed this budget;
//!                         overrides `min_age` for both S3 upload and WAL GC.

use crate::storage::fjall::{FjallIndex, SegmentRegistry};
use crate::storage::segment::{SegmentId, SegmentStatus};
use crate::storage::tiered_reader::S3Config;
use folio_core::error::FolioError;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const MULTIPART_PART_BYTES: usize = 10 * 1024 * 1024;
const MAX_RETRIES: u32 = 5;
const DEFAULT_WAL_MIN_AGE: Duration = Duration::from_secs(3600);

// ── Policy ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct OffloadPolicy {
    pub min_age: Duration,
    pub max_local_bytes: Option<u64>,
}

impl Default for OffloadPolicy {
    fn default() -> Self {
        Self {
            min_age: Duration::ZERO,
            max_local_bytes: None,
        }
    }
}

// ── Public handle ─────────────────────────────────────────────────────────

pub struct BackgroundOffloader {
    handle: JoinHandle<()>,
}

impl BackgroundOffloader {
    pub fn spawn(
        entry_registry: Arc<SegmentRegistry>,
        wal_registry: Arc<SegmentRegistry>,
        wal_index: Arc<FjallIndex>,
        s3_cfg: Option<S3Config>,
        policy: OffloadPolicy,
        entry_sealed_rx: watch::Receiver<Option<SegmentId>>,
    ) -> Self {
        let handle = tokio::spawn(offload_loop(
            entry_registry,
            wal_registry,
            wal_index,
            s3_cfg,
            policy,
            entry_sealed_rx,
        ));
        Self { handle }
    }

    pub fn abort(&self) {
        self.handle.abort();
    }
}

// ── Loop ──────────────────────────────────────────────────────────────────

async fn offload_loop(
    entry_registry: Arc<SegmentRegistry>,
    wal_registry: Arc<SegmentRegistry>,
    wal_index: Arc<FjallIndex>,
    s3_cfg: Option<S3Config>,
    policy: OffloadPolicy,
    mut rx: watch::Receiver<Option<SegmentId>>,
) {
    let s3 = match s3_cfg {
        Some(cfg) => {
            let client = crate::storage::tiered_reader::build_s3_client(&cfg).await;
            Some((cfg, client))
        }
        None => None,
    };

    // WAL GC minimum age: at least DEFAULT_WAL_MIN_AGE even if policy.min_age is shorter,
    // so flushed WAL segments are kept briefly for fast local recovery.
    let wal_min_age = policy.min_age.max(DEFAULT_WAL_MIN_AGE);

    // Poll often enough to honour min_age, but not more than once a minute.
    // When no min_age is set, wake on disk pressure (watch) and poll hourly.
    let poll_interval = if !policy.min_age.is_zero() {
        (policy.min_age / 4).max(Duration::from_secs(60))
    } else {
        Duration::from_secs(3600)
    };

    loop {
        tokio::select! {
            res = rx.changed() => { if res.is_err() { break; } }
            _ = tokio::time::sleep(poll_interval) => {}
        }

        let disk_pressure = compute_disk_pressure(&entry_registry, policy.max_local_bytes);

        if let Some((ref cfg, ref client)) = s3 {
            process_pending(&entry_registry, client, cfg, &policy, disk_pressure).await;
        }

        wal_gc(&wal_registry, &wal_index, wal_min_age, disk_pressure);
    }
}

fn compute_disk_pressure(
    entry_registry: &Arc<SegmentRegistry>,
    max_local_bytes: Option<u64>,
) -> bool {
    let Ok(pending) = entry_registry.list_by_status(SegmentStatus::Local) else {
        return false;
    };
    let total: u64 = pending.iter().map(|m| m.byte_len).sum();
    max_local_bytes.is_some_and(|budget| total > budget)
}

async fn process_pending(
    entry_registry: &Arc<SegmentRegistry>,
    client: &aws_sdk_s3::Client,
    s3_cfg: &S3Config,
    policy: &OffloadPolicy,
    disk_pressure: bool,
) {
    let pending = match entry_registry.list_by_status(SegmentStatus::Local) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("offloader: list_by_status: {e}");
            return;
        }
    };

    for meta in pending {
        let local_path = match &meta.local_path {
            Some(p) => p.clone(),
            None => continue,
        };

        if !disk_pressure && !is_old_enough(&local_path, policy.min_age) {
            continue;
        }

        match entry_registry.cas_status(meta.id, SegmentStatus::Local, SegmentStatus::Offloading) {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => {
                tracing::error!("offloader: cas: {e}");
                continue;
            }
        }

        let s3_key = format!("entries/{:016x}.ent", meta.id);

        if let Err(e) = upload_with_retry(client, &s3_cfg.bucket, &s3_key, &local_path).await {
            tracing::error!("offloader: upload entry seg {} failed: {e}", meta.id);
            let _ =
                entry_registry.cas_status(meta.id, SegmentStatus::Offloading, SegmentStatus::Local);
            continue;
        }

        // Verify the object is readable in S3 before deleting the local copy.
        // upload_with_retry returning Ok means the PUT was acknowledged, but a
        // head_object confirms the object is visible and retrievable.  If this
        // fails, roll back to Local so the next pass retries.
        if let Err(e) = head_object(client, &s3_cfg.bucket, &s3_key).await {
            tracing::error!(
                "offloader: S3 head_object verify failed for entry seg {} (key={s3_key}): {e} — keeping local copy",
                meta.id
            );
            let _ =
                entry_registry.cas_status(meta.id, SegmentStatus::Offloading, SegmentStatus::Local);
            continue;
        }

        if let Err(e) = entry_registry.cas_to_s3(meta.id, SegmentStatus::Offloading, s3_key) {
            tracing::error!("offloader: registry update entry seg {}: {e}", meta.id);
            // Roll back so the segment is retried on the next pass.
            let _ =
                entry_registry.cas_status(meta.id, SegmentStatus::Offloading, SegmentStatus::Local);
            continue;
        }

        if let Err(e) = tokio::fs::remove_file(&local_path).await {
            tracing::warn!("offloader: remove_file {}: {e}", local_path.display());
        }

        tracing::info!(
            "offloader: entry segment {} uploaded, verified, and deleted locally",
            meta.id
        );
    }
}

/// Delete WAL segments whose entries have all migrated to entry_index.
/// Segments are eligible when:
///   - wal_index has no remaining references to them, AND
///   - the segment file is at least `min_age` old (bypassed when `disk_pressure` is true).
fn wal_gc(
    wal_registry: &Arc<SegmentRegistry>,
    wal_index: &Arc<FjallIndex>,
    min_age: Duration,
    disk_pressure: bool,
) {
    let still_needed = match wal_index.referenced_segment_ids() {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("wal_gc: referenced_segment_ids: {e}");
            return;
        }
    };

    let local_segs = match wal_registry.list_by_status(SegmentStatus::Local) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("wal_gc: list_by_status: {e}");
            return;
        }
    };

    for meta in local_segs {
        if still_needed.contains(&meta.id) {
            continue;
        }
        if let Some(path) = &meta.local_path {
            if !disk_pressure && !is_old_enough(path, min_age) {
                continue;
            }
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("wal_gc: remove {}: {e}", path.display());
                continue;
            }
        }
        if let Err(e) =
            wal_registry.cas_status(meta.id, SegmentStatus::Local, SegmentStatus::Deleted)
        {
            tracing::warn!("wal_gc: mark deleted seg {}: {e}", meta.id);
        } else {
            tracing::debug!("wal_gc: deleted WAL segment {}", meta.id);
        }
    }
}

fn is_old_enough(path: &Path, min_age: Duration) -> bool {
    if min_age.is_zero() {
        return true;
    }
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_none_or(|age| age >= min_age)
}

// ── S3 upload helpers ─────────────────────────────────────────────────────

async fn upload_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    path: &std::path::PathBuf,
) -> Result<(), FolioError> {
    let mut delay = Duration::from_secs(1);
    for attempt in 0..=MAX_RETRIES {
        match do_upload(client, bucket, key, path).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt == MAX_RETRIES {
                    return Err(FolioError::Storage(format!(
                        "S3 upload failed after {MAX_RETRIES} retries: {e}"
                    )));
                }
                tracing::warn!(
                    "offloader: upload attempt {attempt} failed: {e}; retry in {delay:?}"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
    }
    unreachable!()
}

async fn head_object(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<(), String> {
    client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn do_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    path: &std::path::PathBuf,
) -> Result<(), String> {
    let meta = tokio::fs::metadata(path).await.map_err(|e| e.to_string())?;
    let file_size = meta.len() as usize;

    if file_size <= MULTIPART_PART_BYTES {
        let body = aws_sdk_s3::primitives::ByteStream::from_path(path)
            .await
            .map_err(|e| e.to_string())?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_length(file_size as i64)
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        return Ok(());
    }

    let mpu = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let upload_id = mpu
        .upload_id()
        .ok_or_else(|| "no upload_id".to_string())?
        .to_owned();

    let mut completed_parts = Vec::new();
    let mut offset: u64 = 0;
    let mut part_num: i32 = 1;

    while offset < file_size as u64 {
        let read_len = ((file_size as u64 - offset) as usize).min(MULTIPART_PART_BYTES);
        let path_clone = path.clone();

        let chunk = tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            let file = std::fs::File::open(&path_clone).map_err(|e| e.to_string())?;
            let mut buf = vec![0u8; read_len];
            let n = file.read_at(&mut buf, offset).map_err(|e| e.to_string())?;
            buf.truncate(n);
            Ok::<Vec<u8>, String>(buf)
        })
        .await
        .map_err(|e| e.to_string())??;

        let body = aws_sdk_s3::primitives::ByteStream::from(bytes::Bytes::from(chunk));
        let part_resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_num)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                let _ = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id);
                e.to_string()
            })?;

        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(part_resp.e_tag().unwrap_or_default())
                .part_number(part_num)
                .build(),
        );
        offset += read_len as u64;
        part_num += 1;
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}
