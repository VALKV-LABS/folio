//! L1 in-memory block cache backed by Moka (concurrent LRU).
//!
//! Keyed by (SegmentId, block_start_offset) where block_start is the
//! BLOCK_SIZE-aligned offset of the 256 KB read window. Values are
//! `bytes::Bytes` (zero-copy slices into the cached buffer).

use crate::storage::segment::{BLOCK_SIZE, SegmentId};
use bytes::Bytes;
use moka::future::Cache;
use std::sync::Arc;

/// Shared L1 block cache. Clone-cheap (Arc inside).
#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<CacheInner>,
}

struct CacheInner {
    cache: Cache<(SegmentId, u64), Bytes>,
}

impl BlockCache {
    /// `max_bytes`: approximate memory budget. Each slot holds a BLOCK_SIZE
    /// (256 KB) buffer; `max_bytes / BLOCK_SIZE` gives the entry count.
    pub fn new(max_bytes: u64) -> Self {
        let max_entries = (max_bytes / BLOCK_SIZE as u64).max(1);
        let cache = Cache::builder()
            .max_capacity(max_entries)
            .support_invalidation_closures()
            .build();
        BlockCache {
            inner: Arc::new(CacheInner { cache }),
        }
    }

    /// Aligned block start for a given byte offset.
    pub fn block_start(offset: u64) -> u64 {
        (offset / BLOCK_SIZE as u64) * BLOCK_SIZE as u64
    }

    pub async fn get(&self, seg: SegmentId, block_start: u64) -> Option<Bytes> {
        self.inner.cache.get(&(seg, block_start)).await
    }

    pub async fn insert(&self, seg: SegmentId, block_start: u64, data: Bytes) {
        self.inner.cache.insert((seg, block_start), data).await;
    }

    /// Evict all cached blocks for a segment (called after S3 offload + local delete).
    pub async fn invalidate_segment(&self, seg: SegmentId) {
        // Moka doesn't support prefix invalidation; iterate known blocks by
        // invalidating one entry and letting LRU age the rest. For correctness
        // we invalidate all entries matching the segment by running a
        // `invalidate_entries_if` predicate.
        self.inner
            .cache
            .invalidate_entries_if(move |k, _| k.0 == seg)
            .expect("invalidate_entries_if requires WeighFn or default; this is fine with default");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_and_get() {
        let cache = BlockCache::new(16 * 1024 * 1024);
        let data = Bytes::from(vec![1u8; BLOCK_SIZE]);
        cache.insert(1, 0, data.clone()).await;
        let got = cache.get(1, 0).await.expect("cache hit");
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn miss_returns_none() {
        let cache = BlockCache::new(16 * 1024 * 1024);
        assert!(cache.get(99, 0).await.is_none());
    }

    #[tokio::test]
    async fn invalidate_segment_removes_entries() {
        let cache = BlockCache::new(16 * 1024 * 1024);
        cache.insert(5, 0, Bytes::from(vec![0u8; 8])).await;
        cache
            .insert(5, BLOCK_SIZE as u64, Bytes::from(vec![1u8; 8]))
            .await;
        cache.insert(6, 0, Bytes::from(vec![2u8; 8])).await;

        cache.invalidate_segment(5).await;
        // Moka invalidation is eventual; run a sync to drain pending tasks.
        cache.inner.cache.run_pending_tasks().await;

        assert!(cache.get(5, 0).await.is_none());
        assert!(cache.get(5, BLOCK_SIZE as u64).await.is_none());
        // Segment 6 untouched.
        assert!(cache.get(6, 0).await.is_some());
    }

    #[test]
    fn block_start_alignment() {
        assert_eq!(BlockCache::block_start(0), 0);
        assert_eq!(BlockCache::block_start(1), 0);
        assert_eq!(BlockCache::block_start(BLOCK_SIZE as u64 - 1), 0);
        assert_eq!(
            BlockCache::block_start(BLOCK_SIZE as u64),
            BLOCK_SIZE as u64
        );
        assert_eq!(
            BlockCache::block_start(BLOCK_SIZE as u64 + 100),
            BLOCK_SIZE as u64
        );
    }
}
