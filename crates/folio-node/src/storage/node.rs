//! JournalService: gRPC data-plane handler.
//!
//! Supports two storage backends via the `Backend` enum:
//!   - `InProcess`: legacy FileJournal + LedgerIndex (used by in-process unit tests).
//!   - `Lsmc`:      LSM-C engine with WAL + EntrySegment cache:
//!    - add_entry → WAL (durability) + EntrySegmentCache (in-memory)
//!    - read_entry → cache (L0) → entry_index (L1) → wal_index (L2) → S3 (L3)
//!
//! Both backends honour the same durability rule: AppendAck is returned only
//! after the write has landed on disk (O_DIRECT for Lsmc; fsync for InProcess).

use crate::node::HealthMonitor;
use crate::storage::entry_cache::EntrySegmentCache;
use crate::storage::fjall::FjallIndex;
use crate::storage::index::LedgerIndex;
use crate::storage::lsmc_journal::LsmcJournal;
use crate::storage::offloader::BackgroundOffloader;
use crate::storage::tiered_reader::TieredReader;
use crate::storage::{Journal, StoredEntry};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use folio_core::error::{FolioError, Result};
use folio_core::metrics::STORAGE;
use folio_core::protocol::{AppendAck, Entry, ReadResult, now_ms};
use folio_core::resolver::StorageNodeClient;
use parking_lot::Mutex;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;
use tokio::sync::Notify;

// ── Backend ───────────────────────────────────────────────────────────────

enum Backend {
    /// In-process storage used by unit tests.
    InProcess {
        journal: Arc<dyn Journal>,
        index: Arc<dyn LedgerIndex>,
    },
    /// LSM-C production storage (WAL + EntrySegment cache).
    Lsmc {
        journal: Arc<LsmcJournal>,
        /// WAL Fjall index — populated by CrashRecovery for unflushed WAL entries.
        wal_index: Arc<FjallIndex>,
        /// EntrySegment Fjall index — populated by the flush task (permanent).
        entry_index: Arc<FjallIndex>,
        /// In-memory hot-write buffer: entries live here between WAL write and flush.
        cache: Arc<Mutex<EntrySegmentCache>>,
        reader: Arc<TieredReader>,
        _flush_thread: JoinHandle<()>,
        _offload: BackgroundOffloader,
    },
}

// ── JournalService ────────────────────────────────────────────────────────

pub struct JournalService {
    node_id: String,
    backend: Backend,
    lac: DashMap<u64, u64>,
    /// Highest entry_id durably written for each ledger this session.
    /// Distinct from `lac` (which tracks the client-piggybacked LAC field).
    /// Used by fence_ledger to report the true highest stored entry to the new leader.
    hi_entry: DashMap<u64, u64>,
    fences: DashSet<u64>,
    /// Ledger IDs this node has seen at least one write for (this process lifetime).
    /// Avoids calling ensure_ledger_known on every append — only on the first.
    known_ledgers: DashSet<u64>,
    notifiers: DashMap<u64, Arc<Notify>>,
    health: Option<Arc<HealthMonitor>>,
}

impl JournalService {
    // ── Constructors ─────────────────────────────────────────────────────

    /// Legacy constructor — kept so existing in-process tests compile unchanged.
    pub fn new(
        node_id: impl Into<String>,
        journal: Arc<dyn Journal>,
        index: Arc<dyn LedgerIndex>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            backend: Backend::InProcess { journal, index },
            lac: DashMap::new(),
            hi_entry: DashMap::new(),
            fences: DashSet::new(),
            known_ledgers: DashSet::new(),
            notifiers: DashMap::new(),
            health: None,
        }
    }

    /// LSM-C constructor used by the production `folio-node` binary.
    #[allow(clippy::too_many_arguments)]
    pub fn new_lsmc(
        node_id: impl Into<String>,
        journal: LsmcJournal,
        wal_index: Arc<FjallIndex>,
        entry_index: Arc<FjallIndex>,
        cache: Arc<Mutex<EntrySegmentCache>>,
        reader: Arc<TieredReader>,
        offload: BackgroundOffloader,
        flush_thread: JoinHandle<()>,
    ) -> Self {
        // Fences live in the entry_index keyspace (permanent store).
        let initial_fences = entry_index.load_fenced_ledgers().unwrap_or_default();
        let fences = DashSet::new();
        for id in &initial_fences {
            fences.insert(*id);
        }

        // Pre-populate known_ledgers from the durable ledger manifest so that
        // ensure_ledger_known is skipped for ledgers that survived a restart.
        let known_ledgers = DashSet::new();
        for id in entry_index.list_known_ledger_ids().unwrap_or_default() {
            known_ledgers.insert(id);
        }

        Self {
            node_id: node_id.into(),
            backend: Backend::Lsmc {
                journal: Arc::new(journal),
                wal_index,
                entry_index,
                cache,
                reader,
                _flush_thread: flush_thread,
                _offload: offload,
            },
            lac: DashMap::new(),
            hi_entry: DashMap::new(),
            fences,
            known_ledgers,
            notifiers: DashMap::new(),
            health: None,
        }
    }

    pub fn with_health_monitor(mut self, monitor: Arc<HealthMonitor>) -> Self {
        self.health = Some(monitor);
        self
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Force-seal the active WAL segment (LSM-C backend only).
    pub async fn seal(&self) -> Result<u64> {
        match &self.backend {
            Backend::Lsmc { journal, .. } => journal.seal().await,
            _ => Err(FolioError::Storage(
                "seal is only available on the LSM-C backend".into(),
            )),
        }
    }

    // ── Data plane operations ─────────────────────────────────────────────

    pub async fn add_entry(&self, entry: Entry) -> Result<AppendAck> {
        if !entry.validate_digest() {
            return Err(FolioError::Serialization(
                "entry checksum validation failed".into(),
            ));
        }

        // Fence check.
        if self.fences.contains(&entry.ledger_id) {
            return Err(FolioError::LedgerFenced(entry.ledger_id));
        }

        let t0 = Instant::now();

        // Duplicate check then write. BookKeeper bookies use the same model:
        // no sequential entry_id enforcement — that is the client's job.
        // If the entry is already present, return OK silently (idempotent
        // re-replication and client retries are handled this way).
        match &self.backend {
            Backend::InProcess { journal, index } => {
                if index.get(entry.ledger_id, entry.entry_id)?.is_some() {
                    let local_lac = self.lac.get(&entry.ledger_id).map(|r| *r).unwrap_or(0);
                    return Ok(AppendAck {
                        ledger_id: entry.ledger_id,
                        entry_id: entry.entry_id,
                        local_lac,
                        node_id: self.node_id.clone(),
                    });
                }
                let stored = StoredEntry {
                    entry: entry.clone(),
                    appended_at_ms: now_ms(),
                };
                journal.append(&stored).await?;
                index.insert(stored)?;
            }
            Backend::Lsmc {
                journal,
                wal_index,
                entry_index,
                cache,
                ..
            } => {
                let lid = entry.ledger_id;
                let eid = entry.entry_id;

                // On first write for this ledger, durably record the ledger in Fjall
                // and register the master key if the client provided one.
                if !self.known_ledgers.contains(&lid) {
                    let key_written = entry_index.ensure_ledger_known(lid, entry.master_key)?;
                    // Persist immediately when a master key is stored for the first time
                    // so it survives a crash before Fjall compaction.
                    if key_written && entry.master_key.is_some() {
                        entry_index.persist()?;
                    }
                    self.known_ledgers.insert(lid);
                } else if entry.master_key.is_some() {
                    // Subsequent writes carrying a master_key (e.g. re-registration
                    // after a node restart) — verify or update without marking as new.
                    entry_index.ensure_ledger_known(lid, entry.master_key)?;
                }

                // HMAC verification: if the entry carries an HMAC and the bookie has
                // a stored master key for this ledger, verify before accepting the write.
                if entry.hmac.is_some()
                    && let Some(mk) = entry_index.get_ledger_master_key(lid)?
                    && !entry.validate_hmac(&mk)
                {
                    return Err(FolioError::Serialization(format!(
                        "ledger {lid} entry {eid}: HMAC verification failed"
                    )));
                }

                // Duplicate detection: cache → entry_index → wal_index.
                let is_dup = cache.lock().contains(lid, eid)
                    || entry_index.get_location(lid, eid)?.is_some()
                    || wal_index.get_location(lid, eid)?.is_some();

                if is_dup {
                    let local_lac = self.lac.get(&lid).map(|r| *r).unwrap_or(0);
                    return Ok(AppendAck {
                        ledger_id: lid,
                        entry_id: eid,
                        local_lac,
                        node_id: self.node_id.clone(),
                    });
                }

                // 1. WAL write (durability gate — ACK only after this returns).
                let stored = StoredEntry {
                    entry: entry.clone(),
                    appended_at_ms: now_ms(),
                };
                let payload = bincode::serialize(&stored)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                journal.append(lid, eid, payload.clone()).await?;

                // Track the highest durably-written entry_id for this ledger.
                // Used by fence_ledger to return the correct LAC to the new leader.
                self.hi_entry
                    .entry(lid)
                    .and_modify(|h| *h = (*h).max(eid))
                    .or_insert(eid);

                // 2. Insert into hot cache (flush task will move it to EntrySegment).
                cache.lock().insert(lid, eid, bytes::Bytes::from(payload));
            }
        }

        let elapsed = t0.elapsed();
        if let Some(h) = &self.health {
            h.record(elapsed, entry.data.len() as u64);
        }
        if let Some(m) = STORAGE.get() {
            m.append_duration.observe(elapsed.as_secs_f64());
            m.entries_written_total.inc();
        }

        // Update LAC atomically for this ledger.
        let local_lac = *self
            .lac
            .entry(entry.ledger_id)
            .and_modify(|l| *l = (*l).max(entry.lac))
            .or_insert(entry.lac);

        // Notify waiters for this ledger.
        let notify = self
            .notifiers
            .entry(entry.ledger_id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();
        notify.notify_waiters();

        Ok(AppendAck {
            ledger_id: entry.ledger_id,
            entry_id: entry.entry_id,
            local_lac,
            node_id: self.node_id.clone(),
        })
    }

    pub async fn read_entry(&self, ledger_id: u64, entry_id: u64) -> Result<ReadResult> {
        match &self.backend {
            Backend::InProcess { index, .. } => {
                let stored = index
                    .get(ledger_id, entry_id)?
                    .ok_or(FolioError::EntryNotFound {
                        ledger_id,
                        entry_id,
                    })?;
                if let Some(m) = STORAGE.get() {
                    m.entries_read_total.inc();
                }
                Ok(ReadResult {
                    entry: stored.entry,
                    node_id: self.node_id.clone(),
                    appended_at_ms: 0,
                })
            }
            Backend::Lsmc { cache, reader, .. } => {
                // L0: in-memory cache (entries not yet flushed to EntrySegment).
                if let Some(payload) = cache.lock().get(ledger_id, entry_id).cloned() {
                    let stored: StoredEntry = bincode::deserialize(&payload)
                        .map_err(|e| FolioError::Serialization(e.to_string()))?;
                    if let Some(m) = STORAGE.get() {
                        m.entries_read_total.inc();
                    }
                    return Ok(ReadResult {
                        entry: stored.entry,
                        node_id: self.node_id.clone(),
                        appended_at_ms: stored.appended_at_ms,
                    });
                }
                // L1–L3: entry_index → EntrySegment file → wal_index → WAL file → S3.
                let payload_bytes = reader.read_entry(ledger_id, entry_id).await?;
                let stored: StoredEntry = bincode::deserialize(&payload_bytes)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                if let Some(m) = STORAGE.get() {
                    m.entries_read_total.inc();
                }
                Ok(ReadResult {
                    entry: stored.entry,
                    node_id: self.node_id.clone(),
                    appended_at_ms: stored.appended_at_ms,
                })
            }
        }
    }

    pub async fn fence_ledger(&self, ledger_id: u64) -> Result<u64> {
        // Write fence flag to Fjall and immediately persist the keyspace WAL.
        // `PartitionHandle::insert` is deferred — without persist() a crash before
        // the next compaction would lose the flag and let writes through on restart.
        if let Backend::Lsmc { entry_index, .. } = &self.backend {
            entry_index.fence_ledger(ledger_id)?;
            entry_index.persist()?;
        }
        self.fences.insert(ledger_id);
        if let Some(m) = STORAGE.get() {
            m.fenced_ledgers_total.inc();
        }

        // Return the highest entry_id durably stored for this ledger.
        //
        // `hi_entry` is accurate for the current process lifetime (updated on
        // every WAL write in add_entry).  On a fresh restart hi_entry is empty,
        // so we fall back to scanning the persistent indices — after crash recovery
        // all durable entries are indexed in either entry_index or wal_index.
        let mem_hi = self.hi_entry.get(&ledger_id).map(|r| *r).unwrap_or(0);
        let storage_hi = match &self.backend {
            Backend::Lsmc {
                entry_index,
                wal_index,
                ..
            } => {
                let hi_entry = entry_index.get_next_entry_id(ledger_id)?.saturating_sub(1);
                let hi_wal = wal_index.get_next_entry_id(ledger_id)?.saturating_sub(1);
                hi_entry.max(hi_wal)
            }
            Backend::InProcess { .. } => 0,
        };
        Ok(mem_hi.max(storage_hi))
    }

    pub async fn get_lac(
        &self,
        ledger_id: u64,
        wait_timeout: Option<std::time::Duration>,
    ) -> Result<u64> {
        let current = self.lac.get(&ledger_id).map(|r| *r).unwrap_or(0);
        if current > 0 || wait_timeout.is_none() {
            return Ok(current);
        }

        let notify = self.notifiers.get(&ledger_id).map(|r| r.value().clone());
        match (notify, wait_timeout) {
            (Some(n), Some(t)) => {
                tokio::time::timeout(t, n.notified())
                    .await
                    .map_err(|_| FolioError::LacTimeout)?;
                Ok(self.lac.get(&ledger_id).map(|r| *r).unwrap_or(0))
            }
            _ => Ok(current),
        }
    }

    /// Used by in-process tests and recovery worker.
    pub fn entries_for_ledger(&self, ledger_id: u64) -> Result<Vec<StoredEntry>> {
        match &self.backend {
            Backend::InProcess { index, .. } => index.range(ledger_id),
            Backend::Lsmc { .. } => Err(FolioError::Storage(
                "entries_for_ledger not supported on Lsmc backend; use TieredReader".into(),
            )),
        }
    }
}

#[async_trait]
impl StorageNodeClient for JournalService {
    async fn add_entry(&self, entry: Entry) -> Result<AppendAck> {
        JournalService::add_entry(self, entry).await
    }
    async fn fence_ledger(&self, ledger_id: u64) -> Result<u64> {
        JournalService::fence_ledger(self, ledger_id).await
    }
    async fn read_entry(&self, ledger_id: u64, entry_id: u64) -> Result<ReadResult> {
        JournalService::read_entry(self, ledger_id, entry_id).await
    }
    async fn get_lac(&self, ledger_id: u64, wait: Option<std::time::Duration>) -> Result<u64> {
        JournalService::get_lac(self, ledger_id, wait).await
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{FileJournal, MemoryLedgerIndex};
    use folio_core::protocol::Entry;
    use tempfile::tempdir;

    fn dyn_journal(j: impl Journal + 'static) -> Arc<dyn Journal> {
        Arc::new(j)
    }
    fn dyn_index(
        i: impl crate::storage::index::LedgerIndex + 'static,
    ) -> Arc<dyn crate::storage::index::LedgerIndex> {
        Arc::new(i)
    }

    async fn make_inprocess(dir: &std::path::Path) -> JournalService {
        let journal = dyn_journal(FileJournal::new(dir.join("journal.log")).await.unwrap());
        let index = dyn_index(MemoryLedgerIndex::default());
        JournalService::new("node-0", journal, index)
    }

    fn entry(ledger_id: u64, entry_id: u64, data: &[u8]) -> Entry {
        Entry::new(ledger_id, entry_id, 0, data.to_vec())
    }

    #[tokio::test]
    async fn add_and_read_entry() {
        let dir = tempdir().unwrap();
        let svc = make_inprocess(dir.path()).await;

        let e = entry(1, 0, b"hello");
        let ack = svc.add_entry(e.clone()).await.unwrap();
        assert_eq!(ack.entry_id, 0);
        assert_eq!(ack.ledger_id, 1);

        let rr = svc.read_entry(1, 0).await.unwrap();
        assert_eq!(rr.entry.data, b"hello");
    }

    #[tokio::test]
    async fn duplicate_add_is_idempotent() {
        let dir = tempdir().unwrap();
        let svc = make_inprocess(dir.path()).await;

        let e = entry(1, 0, b"data");
        svc.add_entry(e.clone()).await.unwrap();
        // Second add with same (ledger_id, entry_id) must return OK, not an error.
        svc.add_entry(e).await.unwrap();
    }

    #[tokio::test]
    async fn fence_blocks_writes() {
        let dir = tempdir().unwrap();
        let svc = make_inprocess(dir.path()).await;

        svc.add_entry(entry(1, 0, b"before")).await.unwrap();
        svc.fence_ledger(1).await.unwrap();

        let err = svc.add_entry(entry(1, 1, b"after")).await.unwrap_err();
        assert!(matches!(err, FolioError::LedgerFenced(1)));
    }

    #[tokio::test]
    async fn get_lac_returns_highest_acknowledged() {
        let dir = tempdir().unwrap();
        let svc = make_inprocess(dir.path()).await;

        svc.add_entry(Entry::new(1, 0, 5, b"a".to_vec()))
            .await
            .unwrap();
        svc.add_entry(Entry::new(1, 1, 5, b"b".to_vec()))
            .await
            .unwrap();
        let lac = svc.get_lac(1, None).await.unwrap();
        // LAC is carried in the entry's lac field; both entries carry lac=5.
        assert_eq!(lac, 5);
    }

    #[tokio::test]
    async fn read_missing_entry_returns_not_found() {
        let dir = tempdir().unwrap();
        let svc = make_inprocess(dir.path()).await;
        let err = svc.read_entry(99, 0).await.unwrap_err();
        assert!(matches!(err, FolioError::EntryNotFound { .. }));
    }

    #[tokio::test]
    async fn checksum_validation_rejects_corrupt_entry() {
        let dir = tempdir().unwrap();
        let svc = make_inprocess(dir.path()).await;

        let mut e = entry(1, 0, b"data");
        e.digest = 0xDEADBEEF; // intentionally wrong
        let err = svc.add_entry(e).await.unwrap_err();
        assert!(matches!(err, FolioError::Serialization(_)));
    }
}
