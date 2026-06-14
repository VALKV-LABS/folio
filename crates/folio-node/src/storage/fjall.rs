//! Fjall-backed storage for the LSM-C engine.
//!
//! Single keyspace at `{data_dir}/folio.db/` opened via `FolioDb::open`.
//! Partitions (all within the same keyspace):
//!   wal.entries, wal.locations, wal.checkpoints, wal.ledgers
//!   entry.entries, entry.locations, entry.checkpoints, entry.ledgers
//!   wal.registry, entry.registry

use crate::storage::StoredEntry;
use crate::storage::index::LedgerIndex;
use crate::storage::segment::{IndexValue, SegmentId, SegmentMeta, SegmentStatus};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};

// ── DbConfig ──────────────────────────────────────────────────────────────

/// Tuning parameters for the Fjall keyspace opened by [`FolioDb`].
///
/// All fields are optional; `None` means "use Fjall's built-in default".
/// Values are driven by env variables parsed in `main()` and forwarded here.
#[derive(Debug, Clone, Default)]
pub struct DbConfig {
    /// Bloom-filter bits-per-key applied to every partition.
    /// Valid range 1–20; typical good value is 10.
    /// `None` = use Fjall's compiled-in default (usually enabled at ~10 bits).
    /// Controlled by `FJALL_BLOOM_FILTER_BITS`.
    pub bloom_filter_bits: Option<u8>,

    /// Shared block-cache budget for the Fjall keyspace, in bytes.
    /// `None` = use Fjall's default (16 MiB).
    /// Controlled by `FJALL_BLOCK_CACHE_BYTES`.
    pub block_cache_bytes: Option<u64>,
}
use folio_core::error::{FolioError, Result};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

// ── LedgerState ───────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct LedgerState {
    pub is_fenced: bool,
    /// HMAC master key registered on first write for this ledger.
    /// None = ledger known but operating in CRC32-only mode.
    pub master_key: Option<[u8; 32]>,
}

// ── FolioDb ───────────────────────────────────────────────────────────────

/// Single-keyspace entry point. All sub-objects share the same Fjall keyspace
/// (`{data_dir}/folio.db/`) — one WAL, one compaction coordinator, atomic
/// cross-partition commits.
pub struct FolioDb {
    pub wal_index: Arc<FjallIndex>,
    pub entry_index: Arc<FjallIndex>,
    pub wal_registry: Arc<SegmentRegistry>,
    pub entry_registry: Arc<SegmentRegistry>,
    /// Shared Fjall keyspace — passed to the co-located Table Service so it
    /// can open additional named partitions without a separate DB directory.
    pub keyspace: Keyspace,
}

impl FolioDb {
    pub fn open(data_dir: impl AsRef<Path>, cfg: &DbConfig) -> Result<Self> {
        let mut ks_cfg = Config::new(data_dir.as_ref().join("folio.db"));
        if let Some(bytes) = cfg.block_cache_bytes {
            ks_cfg = ks_cfg.cache_size(bytes);
        }
        let ks = ks_cfg
            .open()
            .map_err(|e| FolioError::Storage(e.to_string()))?;

        let mut part_opts = PartitionCreateOptions::default();
        if let Some(bits) = cfg.bloom_filter_bits {
            part_opts = part_opts.bloom_filter_bits(Some(bits));
        }

        Ok(Self {
            wal_index: Arc::new(FjallIndex::from_keyspace(
                ks.clone(),
                "wal",
                part_opts.clone(),
            )?),
            entry_index: Arc::new(FjallIndex::from_keyspace(
                ks.clone(),
                "entry",
                part_opts.clone(),
            )?),
            wal_registry: Arc::new(SegmentRegistry::from_keyspace(
                ks.clone(),
                "wal",
                part_opts.clone(),
            )?),
            entry_registry: Arc::new(SegmentRegistry::from_keyspace(
                ks.clone(),
                "entry",
                part_opts.clone(),
            )?),
            keyspace: ks,
        })
    }
}

// ── FjallIndex ────────────────────────────────────────────────────────────

pub struct FjallIndex {
    _keyspace: Keyspace,
    entries: PartitionHandle,   // legacy
    locations: PartitionHandle, // LSM-C
    checkpoints: PartitionHandle,
    ledgers: PartitionHandle,
}

impl FjallIndex {
    fn from_keyspace(
        ks: Keyspace,
        prefix: &str,
        part_opts: PartitionCreateOptions,
    ) -> Result<Self> {
        let open = |name: &str| {
            ks.open_partition(name, part_opts.clone())
                .map_err(|e| FolioError::Storage(e.to_string()))
        };
        Ok(Self {
            entries: open(&format!("{prefix}.entries"))?,
            locations: open(&format!("{prefix}.locations"))?,
            checkpoints: open(&format!("{prefix}.checkpoints"))?,
            ledgers: open(&format!("{prefix}.ledgers"))?,
            _keyspace: ks,
        })
    }

    /// Open a standalone index at the given path. Used by unit tests.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let ks = Config::new(path)
            .open()
            .map_err(|e| FolioError::Storage(e.to_string()))?;
        Self::from_keyspace(ks, "index", PartitionCreateOptions::default())
    }

    // ── Key encoding ────────────────────────────────────────────────────

    fn encode_key(ledger_id: u64, entry_id: u64) -> [u8; 16] {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&ledger_id.to_be_bytes());
        k[8..].copy_from_slice(&entry_id.to_be_bytes());
        k
    }

    fn decode_key(k: &[u8]) -> Result<(u64, u64)> {
        if k.len() < 16 {
            return Err(FolioError::Storage(format!(
                "corrupt index key: expected 16 bytes, got {}",
                k.len()
            )));
        }
        let lid = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
        let eid = u64::from_be_bytes(k[8..16].try_into().expect("8 bytes"));
        Ok((lid, eid))
    }

    // ── LSM-C index (locations partition) ───────────────────────────────

    pub fn insert_location(
        &self,
        ledger_id: u64,
        entry_id: u64,
        segment_id: SegmentId,
        offset: u64,
        length: u32,
    ) -> Result<()> {
        let key = Self::encode_key(ledger_id, entry_id);
        let val = bincode::serialize(&IndexValue {
            segment_id,
            offset,
            length,
        })
        .map_err(|e| FolioError::Serialization(e.to_string()))?;
        self.locations
            .insert(key, val)
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    pub fn get_location(&self, ledger_id: u64, entry_id: u64) -> Result<Option<IndexValue>> {
        let key = Self::encode_key(ledger_id, entry_id);
        match self
            .locations
            .get(key)
            .map_err(|e| FolioError::Storage(e.to_string()))?
        {
            None => Ok(None),
            Some(b) => {
                let v: IndexValue = bincode::deserialize(&b)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                Ok(Some(v))
            }
        }
    }

    /// Remove a location entry. Used by WAL GC after entries are flushed to an EntrySegment.
    pub fn delete_location(&self, ledger_id: u64, entry_id: u64) -> Result<()> {
        let key = Self::encode_key(ledger_id, entry_id);
        self.locations
            .remove(key)
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    /// Remove EntrySegment locations that point into the truncated tail of a
    /// torn segment. Crash recovery uses this after truncating an EntrySegment
    /// so WAL replay can restore those specific entries from the durable WAL.
    pub fn delete_locations_in_segment_tail(
        &self,
        segment_id: SegmentId,
        min_offset: u64,
    ) -> Result<Vec<(u64, u64)>> {
        let mut to_delete = Vec::new();
        for item in self.locations.iter() {
            let (k, v) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            let iv: IndexValue =
                bincode::deserialize(&v).map_err(|e| FolioError::Serialization(e.to_string()))?;
            if iv.segment_id == segment_id && iv.offset >= min_offset {
                to_delete.push(Self::decode_key(&k)?);
            }
        }

        for (ledger_id, entry_id) in &to_delete {
            self.delete_location(*ledger_id, *entry_id)?;
        }
        Ok(to_delete)
    }

    /// Iterate all locations for a ledger (ordered by entry_id).
    pub fn scan_ledger_locations(&self, ledger_id: u64) -> Result<Vec<(u64, IndexValue)>> {
        let start = Self::encode_key(ledger_id, 0);
        let end = Self::encode_key(ledger_id, u64::MAX);
        let mut out = Vec::new();
        for item in self.locations.range(start..=end) {
            let (k, v) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            let (_, eid) = Self::decode_key(&k)?;
            let iv: IndexValue =
                bincode::deserialize(&v).map_err(|e| FolioError::Serialization(e.to_string()))?;
            out.push((eid, iv));
        }
        Ok(out)
    }

    /// Returns the next expected entry_id for a ledger on this node.
    /// Reads only the last key in the ledger's range — O(log N), not O(N).
    /// Returns 0 if the ledger has no entries on this node yet.
    pub fn get_next_entry_id(&self, ledger_id: u64) -> Result<u64> {
        let start = Self::encode_key(ledger_id, 0);
        let end = Self::encode_key(ledger_id, u64::MAX);
        let last = self
            .locations
            .range(start..=end)
            .next_back()
            .transpose()
            .map_err(|e| FolioError::Storage(e.to_string()))?;
        Ok(match last {
            None => 0,
            Some((k, _)) => {
                let (_, eid) = Self::decode_key(&k)?;
                eid + 1
            }
        })
    }

    /// Collect the unique set of segment IDs referenced by any location entry.
    /// Used by WAL GC to identify which WAL segments are still needed.
    pub fn referenced_segment_ids(&self) -> Result<std::collections::BTreeSet<SegmentId>> {
        let mut out = std::collections::BTreeSet::new();
        for item in self.locations.iter() {
            let (_, v) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            let iv: IndexValue =
                bincode::deserialize(&v).map_err(|e| FolioError::Serialization(e.to_string()))?;
            out.insert(iv.segment_id);
        }
        Ok(out)
    }

    // ── Crash-recovery checkpoints ───────────────────────────────────────

    pub fn get_checkpoint(&self, segment_id: SegmentId) -> Result<u64> {
        let key = segment_id.to_be_bytes();
        match self
            .checkpoints
            .get(key)
            .map_err(|e| FolioError::Storage(e.to_string()))?
        {
            None => Ok(0),
            Some(b) => {
                let off = u64::from_be_bytes(
                    b[..8]
                        .try_into()
                        .map_err(|_| FolioError::Storage("checkpoint corrupt".into()))?,
                );
                Ok(off)
            }
        }
    }

    pub fn set_checkpoint(&self, segment_id: SegmentId, offset: u64) -> Result<()> {
        let key = segment_id.to_be_bytes();
        self.checkpoints
            .insert(key, offset.to_be_bytes())
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    // ── Per-ledger durable state (ledgers partition) ─────────────────────

    fn ledger_key(ledger_id: u64) -> [u8; 8] {
        ledger_id.to_be_bytes()
    }

    fn get_ledger_state(&self, ledger_id: u64) -> Result<Option<LedgerState>> {
        match self
            .ledgers
            .get(Self::ledger_key(ledger_id))
            .map_err(|e| FolioError::Storage(e.to_string()))?
        {
            None => Ok(None),
            Some(b) => {
                let s: LedgerState = bincode::deserialize(&b)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                Ok(Some(s))
            }
        }
    }

    /// Durably mark a ledger as fenced. Reads existing state first to preserve future fields.
    pub fn fence_ledger(&self, ledger_id: u64) -> Result<()> {
        let mut state = self.get_ledger_state(ledger_id)?.unwrap_or_default();
        state.is_fenced = true;
        let val =
            bincode::serialize(&state).map_err(|e| FolioError::Serialization(e.to_string()))?;
        self.ledgers
            .insert(Self::ledger_key(ledger_id), val)
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    /// Force-sync the keyspace WAL to disk.  Must be called after `fence_ledger`
    /// so that the fence flag survives a crash before the next Fjall compaction.
    pub fn persist(&self) -> Result<()> {
        self._keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    /// Returns the set of all durably fenced ledger IDs. Called once at startup.
    pub fn load_fenced_ledgers(&self) -> Result<BTreeSet<u64>> {
        let mut out = BTreeSet::new();
        for item in self.ledgers.iter() {
            let (k, v) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            let state: LedgerState =
                bincode::deserialize(&v).map_err(|e| FolioError::Serialization(e.to_string()))?;
            if state.is_fenced {
                if k.len() < 8 {
                    return Err(FolioError::Storage(format!(
                        "corrupt ledger key: expected 8 bytes, got {}",
                        k.len()
                    )));
                }
                let id = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
                out.insert(id);
            }
        }
        Ok(out)
    }

    /// Returns all ledger IDs this node has ever seen (fenced or not).
    /// Useful for enumerating the full ledger manifest without a locations scan.
    pub fn list_known_ledger_ids(&self) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for item in self.ledgers.iter() {
            let (k, _) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            if k.len() >= 8 {
                out.push(u64::from_be_bytes(k[..8].try_into().expect("8 bytes")));
            }
        }
        Ok(out)
    }

    /// Return the stored master key for a ledger, if one was registered.
    pub fn get_ledger_master_key(&self, ledger_id: u64) -> Result<Option<[u8; 32]>> {
        Ok(self.get_ledger_state(ledger_id)?.and_then(|s| s.master_key))
    }

    /// Record that this node has seen `ledger_id`.  Called on the first write
    /// for a ledger so the bookie maintains a durable ledger manifest in addition
    /// to the locations index.
    ///
    /// * If the ledger is not yet known: inserts `LedgerState { is_fenced: false, master_key }`.
    /// * If the ledger is already known with no master key and a key is supplied now: stores it.
    /// * If both sides have a master key: verifies they match; returns an error on mismatch.
    /// * Returns `true` when the state was newly written (first-ever write or key update),
    ///   so the caller knows to call `persist()` for durability of the key.
    pub fn ensure_ledger_known(
        &self,
        ledger_id: u64,
        master_key: Option<[u8; 32]>,
    ) -> Result<bool> {
        match self.get_ledger_state(ledger_id)? {
            None => {
                let state = LedgerState {
                    is_fenced: false,
                    master_key,
                };
                let val = bincode::serialize(&state)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                self.ledgers
                    .insert(Self::ledger_key(ledger_id), val)
                    .map_err(|e| FolioError::Storage(e.to_string()))?;
                Ok(true)
            }
            Some(mut state) => match (state.master_key, master_key) {
                (None, Some(new_key)) => {
                    state.master_key = Some(new_key);
                    let val = bincode::serialize(&state)
                        .map_err(|e| FolioError::Serialization(e.to_string()))?;
                    self.ledgers
                        .insert(Self::ledger_key(ledger_id), val)
                        .map_err(|e| FolioError::Storage(e.to_string()))?;
                    Ok(true)
                }
                (Some(existing), Some(new_key)) if existing != new_key => {
                    Err(FolioError::Serialization(format!(
                        "ledger {ledger_id}: master key mismatch — rejecting write"
                    )))
                }
                _ => Ok(false),
            },
        }
    }
}

// ── Legacy LedgerIndex impl (used by in-process tests) ───────────────────

impl LedgerIndex for FjallIndex {
    fn insert(&self, entry: StoredEntry) -> Result<()> {
        let key = Self::encode_key(entry.entry.ledger_id, entry.entry.entry_id);
        let val =
            bincode::serialize(&entry).map_err(|e| FolioError::Serialization(e.to_string()))?;
        self.entries
            .insert(key, val)
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    fn get(&self, ledger_id: u64, entry_id: u64) -> Result<Option<StoredEntry>> {
        let key = Self::encode_key(ledger_id, entry_id);
        match self
            .entries
            .get(key)
            .map_err(|e| FolioError::Storage(e.to_string()))?
        {
            None => Ok(None),
            Some(b) => {
                let e: StoredEntry = bincode::deserialize(&b)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                Ok(Some(e))
            }
        }
    }

    fn range(&self, ledger_id: u64) -> Result<Vec<StoredEntry>> {
        let start = Self::encode_key(ledger_id, 0);
        let end = Self::encode_key(ledger_id, u64::MAX);
        let mut out = Vec::new();
        for item in self.entries.range(start..=end) {
            let (_, v) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            let e: StoredEntry =
                bincode::deserialize(&v).map_err(|e| FolioError::Serialization(e.to_string()))?;
            out.push(e);
        }
        Ok(out)
    }
}

// ── SegmentRegistry ───────────────────────────────────────────────────────

pub struct SegmentRegistry {
    _keyspace: Keyspace,
    partition: PartitionHandle,
}

impl SegmentRegistry {
    fn from_keyspace(
        ks: Keyspace,
        prefix: &str,
        part_opts: PartitionCreateOptions,
    ) -> Result<Self> {
        let partition = ks
            .open_partition(&format!("{prefix}.registry"), part_opts)
            .map_err(|e| FolioError::Storage(e.to_string()))?;
        Ok(Self {
            _keyspace: ks,
            partition,
        })
    }

    /// Open a standalone registry at the given path. Used by unit tests.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let ks = Config::new(path)
            .open()
            .map_err(|e| FolioError::Storage(e.to_string()))?;
        Self::from_keyspace(ks, "registry", PartitionCreateOptions::default())
    }

    fn id_key(id: SegmentId) -> [u8; 8] {
        id.to_be_bytes()
    }

    pub fn upsert(&self, meta: &SegmentMeta) -> Result<()> {
        let val = bincode::serialize(meta).map_err(|e| FolioError::Serialization(e.to_string()))?;
        self.partition
            .insert(Self::id_key(meta.id), val)
            .map_err(|e| FolioError::Storage(e.to_string()))
    }

    pub fn get(&self, id: SegmentId) -> Result<Option<SegmentMeta>> {
        match self
            .partition
            .get(Self::id_key(id))
            .map_err(|e| FolioError::Storage(e.to_string()))?
        {
            None => Ok(None),
            Some(b) => {
                let m: SegmentMeta = bincode::deserialize(&b)
                    .map_err(|e| FolioError::Serialization(e.to_string()))?;
                Ok(Some(m))
            }
        }
    }

    /// List all segments with the given status (full scan; call infrequently).
    pub fn list_by_status(&self, status: SegmentStatus) -> Result<Vec<SegmentMeta>> {
        let mut out = Vec::new();
        for item in self.partition.iter() {
            let (_, v) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            let m: SegmentMeta =
                bincode::deserialize(&v).map_err(|e| FolioError::Serialization(e.to_string()))?;
            if m.status == status {
                out.push(m);
            }
        }
        Ok(out)
    }

    /// Atomic compare-and-swap on status. Returns `true` if the swap succeeded.
    pub fn cas_status(
        &self,
        id: SegmentId,
        expected: SegmentStatus,
        new: SegmentStatus,
    ) -> Result<bool> {
        let mut meta = match self.get(id)? {
            Some(m) => m,
            None => return Ok(false),
        };
        if meta.status != expected {
            return Ok(false);
        }
        meta.status = new;
        self.upsert(&meta)?;
        Ok(true)
    }

    /// CAS status and atomically set the s3_key field.
    pub fn cas_to_s3(
        &self,
        id: SegmentId,
        expected: SegmentStatus,
        s3_key: String,
    ) -> Result<bool> {
        let mut meta = match self.get(id)? {
            Some(m) => m,
            None => return Ok(false),
        };
        if meta.status != expected {
            return Ok(false);
        }
        meta.status = SegmentStatus::S3;
        meta.s3_key = Some(s3_key);
        meta.local_path = None;
        self.upsert(&meta)?;
        Ok(true)
    }

    /// Returns the highest segment ID in the registry, or `None` if empty.
    ///
    /// Used on startup to seed the next-ID counter so that IDs already
    /// allocated — including those whose local files were deleted after S3
    /// offload — are never reused.
    pub fn max_id(&self) -> Result<Option<SegmentId>> {
        let mut max: Option<SegmentId> = None;
        for item in self.partition.iter() {
            let (k, _) = item.map_err(|e| FolioError::Storage(e.to_string()))?;
            if k.len() == 8 {
                let id = u64::from_be_bytes(k.as_ref().try_into().unwrap());
                max = Some(max.map_or(id, |m: u64| m.max(id)));
            }
        }
        Ok(max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::segment::SegmentKind;
    use tempfile::tempdir;

    #[test]
    fn locations_insert_get_round_trip() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        idx.insert_location(1, 0, 42, 4096, 56).unwrap();
        let iv = idx.get_location(1, 0).unwrap().unwrap();
        assert_eq!(
            iv,
            IndexValue {
                segment_id: 42,
                offset: 4096,
                length: 56
            }
        );
    }

    #[test]
    fn delete_locations_in_segment_tail_removes_only_truncated_offsets() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();

        idx.insert_location(1, 0, 4, 0, 10).unwrap();
        idx.insert_location(1, 1, 4, 100, 10).unwrap();
        idx.insert_location(1, 2, 4, 200, 10).unwrap();
        idx.insert_location(1, 3, 5, 0, 10).unwrap();

        let removed = idx.delete_locations_in_segment_tail(4, 100).unwrap();

        assert_eq!(removed, vec![(1, 1), (1, 2)]);
        assert!(idx.get_location(1, 0).unwrap().is_some());
        assert!(idx.get_location(1, 1).unwrap().is_none());
        assert!(idx.get_location(1, 2).unwrap().is_none());
        assert!(idx.get_location(1, 3).unwrap().is_some());
    }

    #[test]
    fn checkpoint_persist_reopen() {
        let dir = tempdir().unwrap();
        {
            let idx = FjallIndex::new(dir.path()).unwrap();
            idx.set_checkpoint(7, 8192).unwrap();
        }
        let idx = FjallIndex::new(dir.path()).unwrap();
        assert_eq!(idx.get_checkpoint(7).unwrap(), 8192);
        assert_eq!(idx.get_checkpoint(99).unwrap(), 0); // absent = 0
    }

    #[test]
    fn registry_cas_status() {
        let dir = tempdir().unwrap();
        let reg = SegmentRegistry::new(dir.path()).unwrap();
        let meta = SegmentMeta {
            id: 1,
            kind: SegmentKind::Wal,
            status: SegmentStatus::Local,
            local_path: Some("/tmp/seg-1.seg".into()),
            s3_key: None,
            byte_len: 0,
            ledger_sizes: std::collections::HashMap::new(),
        };
        reg.upsert(&meta).unwrap();
        // Correct expected status → succeeds
        assert!(
            reg.cas_status(1, SegmentStatus::Local, SegmentStatus::Offloading)
                .unwrap()
        );
        // Wrong expected status → fails
        assert!(
            !reg.cas_status(1, SegmentStatus::Local, SegmentStatus::S3)
                .unwrap()
        );
        // Verify new status
        assert_eq!(
            reg.get(1).unwrap().unwrap().status,
            SegmentStatus::Offloading
        );
    }

    #[test]
    fn fence_persists_and_loads() {
        let dir = tempdir().unwrap();
        {
            let idx = FjallIndex::new(dir.path()).unwrap();
            idx.fence_ledger(42).unwrap();
        }
        let idx = FjallIndex::new(dir.path()).unwrap();
        let fenced = idx.load_fenced_ledgers().unwrap();
        assert!(fenced.contains(&42));
        assert!(!fenced.contains(&99));
        // Idempotent: fence again should not corrupt state
        idx.fence_ledger(42).unwrap();
        assert!(idx.load_fenced_ledgers().unwrap().contains(&42));
    }

    #[test]
    fn ensure_ledger_known_creates_new_record() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        // Ledger not yet known
        assert!(idx.get_ledger_state(7).unwrap().is_none());
        let is_new = idx.ensure_ledger_known(7, None).unwrap();
        assert!(is_new);
        let state = idx.get_ledger_state(7).unwrap().unwrap();
        assert!(!state.is_fenced);
        assert!(state.master_key.is_none());
    }

    #[test]
    fn ensure_ledger_known_idempotent_without_key() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        assert!(idx.ensure_ledger_known(1, None).unwrap()); // new
        assert!(!idx.ensure_ledger_known(1, None).unwrap()); // already known
    }

    #[test]
    fn ensure_ledger_known_stores_master_key_on_first_write() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        let key = [0xBEu8; 32];
        let is_new = idx.ensure_ledger_known(5, Some(key)).unwrap();
        assert!(is_new);
        assert_eq!(idx.get_ledger_master_key(5).unwrap(), Some(key));
    }

    #[test]
    fn ensure_ledger_known_updates_key_when_none_was_stored() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        idx.ensure_ledger_known(3, None).unwrap();
        let key = [0xCCu8; 32];
        let updated = idx.ensure_ledger_known(3, Some(key)).unwrap();
        assert!(updated);
        assert_eq!(idx.get_ledger_master_key(3).unwrap(), Some(key));
    }

    #[test]
    fn ensure_ledger_known_rejects_key_mismatch() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        let key1 = [0x11u8; 32];
        let key2 = [0x22u8; 32];
        idx.ensure_ledger_known(9, Some(key1)).unwrap();
        let err = idx.ensure_ledger_known(9, Some(key2)).unwrap_err();
        assert!(err.to_string().contains("master key mismatch"));
    }

    #[test]
    fn ensure_ledger_known_same_key_idempotent() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        let key = [0xAAu8; 32];
        idx.ensure_ledger_known(10, Some(key)).unwrap();
        // Same key again — should not error, returns false (not new)
        let is_new = idx.ensure_ledger_known(10, Some(key)).unwrap();
        assert!(!is_new);
    }

    #[test]
    fn list_known_ledger_ids() {
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        idx.ensure_ledger_known(1, None).unwrap();
        idx.ensure_ledger_known(5, None).unwrap();
        idx.ensure_ledger_known(3, None).unwrap();
        let mut ids = idx.list_known_ledger_ids().unwrap();
        ids.sort();
        assert_eq!(ids, vec![1, 3, 5]);
    }

    #[test]
    fn master_key_persists_across_reopen() {
        let dir = tempdir().unwrap();
        let key = [0xDDu8; 32];
        {
            let idx = FjallIndex::new(dir.path()).unwrap();
            idx.ensure_ledger_known(99, Some(key)).unwrap();
        }
        let idx = FjallIndex::new(dir.path()).unwrap();
        assert_eq!(idx.get_ledger_master_key(99).unwrap(), Some(key));
    }

    #[test]
    fn fence_preserves_master_key() {
        let dir = tempdir().unwrap();
        let key = [0xEEu8; 32];
        let idx = FjallIndex::new(dir.path()).unwrap();
        idx.ensure_ledger_known(20, Some(key)).unwrap();
        idx.fence_ledger(20).unwrap();
        let state = idx.get_ledger_state(20).unwrap().unwrap();
        assert!(state.is_fenced);
        assert_eq!(state.master_key, Some(key));
    }

    #[test]
    fn legacy_ledger_index_still_works() {
        use folio_core::protocol::Entry;
        let dir = tempdir().unwrap();
        let idx = FjallIndex::new(dir.path()).unwrap();
        let entry = crate::storage::StoredEntry {
            entry: Entry::new(1, 0, 0, b"test".to_vec()),
            appended_at_ms: 0,
        };
        idx.insert(entry.clone()).unwrap();
        let got = idx.get(1, 0).unwrap().unwrap();
        assert_eq!(got.entry.data, b"test");
    }

    #[test]
    fn folio_db_open_all_partitions_shared_keyspace() {
        let dir = tempdir().unwrap();
        let db = FolioDb::open(dir.path(), &DbConfig::default()).unwrap();
        // wal_index and entry_index are independent (different prefixes, same keyspace)
        db.wal_index.insert_location(1, 0, 10, 0, 64).unwrap();
        db.entry_index.insert_location(1, 0, 20, 0, 64).unwrap();
        assert_eq!(
            db.wal_index.get_location(1, 0).unwrap().unwrap().segment_id,
            10
        );
        assert_eq!(
            db.entry_index
                .get_location(1, 0)
                .unwrap()
                .unwrap()
                .segment_id,
            20
        );
        // registries are independent
        let meta = SegmentMeta {
            id: 1,
            kind: SegmentKind::Wal,
            status: SegmentStatus::Local,
            local_path: None,
            s3_key: None,
            byte_len: 0,
            ledger_sizes: std::collections::HashMap::new(),
        };
        db.wal_registry.upsert(&meta).unwrap();
        assert!(db.entry_registry.get(1).unwrap().is_none());
    }

    #[test]
    fn folio_db_open_with_bloom_filter() {
        let dir = tempdir().unwrap();
        let cfg = DbConfig {
            bloom_filter_bits: Some(10),
            block_cache_bytes: None,
        };
        let db = FolioDb::open(dir.path(), &cfg).unwrap();
        db.wal_index.insert_location(2, 5, 99, 512, 32).unwrap();
        let iv = db.wal_index.get_location(2, 5).unwrap().unwrap();
        assert_eq!(iv.segment_id, 99);
    }

    #[test]
    fn folio_db_open_with_block_cache() {
        let dir = tempdir().unwrap();
        let cfg = DbConfig {
            bloom_filter_bits: None,
            block_cache_bytes: Some(32 * 1024 * 1024), // 32 MiB
        };
        let db = FolioDb::open(dir.path(), &cfg).unwrap();
        db.entry_index.insert_location(3, 0, 7, 0, 128).unwrap();
        let iv = db.entry_index.get_location(3, 0).unwrap().unwrap();
        assert_eq!(iv.offset, 0);
    }

    #[test]
    fn folio_db_open_with_bloom_and_block_cache() {
        let dir = tempdir().unwrap();
        let cfg = DbConfig {
            bloom_filter_bits: Some(8),
            block_cache_bytes: Some(16 * 1024 * 1024),
        };
        let db = FolioDb::open(dir.path(), &cfg).unwrap();
        db.wal_index.insert_location(10, 0, 1, 0, 64).unwrap();
        db.entry_index.insert_location(10, 0, 2, 0, 64).unwrap();
        assert_ne!(
            db.wal_index
                .get_location(10, 0)
                .unwrap()
                .unwrap()
                .segment_id,
            db.entry_index
                .get_location(10, 0)
                .unwrap()
                .unwrap()
                .segment_id,
        );
    }
}
