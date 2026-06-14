use crate::storage::StoredEntry;
use folio_core::error::Result;
use parking_lot::RwLock;
use std::collections::BTreeMap;

pub trait LedgerIndex: Send + Sync {
    fn insert(&self, entry: StoredEntry) -> Result<()>;
    fn get(&self, ledger_id: u64, entry_id: u64) -> Result<Option<StoredEntry>>;
    fn range(&self, ledger_id: u64) -> Result<Vec<StoredEntry>>;
}

#[derive(Debug, Default)]
pub struct MemoryLedgerIndex {
    entries: RwLock<BTreeMap<(u64, u64), StoredEntry>>,
}

impl LedgerIndex for MemoryLedgerIndex {
    fn insert(&self, entry: StoredEntry) -> Result<()> {
        self.entries
            .write()
            .insert((entry.entry.ledger_id, entry.entry.entry_id), entry);
        Ok(())
    }

    fn get(&self, ledger_id: u64, entry_id: u64) -> Result<Option<StoredEntry>> {
        Ok(self.entries.read().get(&(ledger_id, entry_id)).cloned())
    }

    fn range(&self, ledger_id: u64) -> Result<Vec<StoredEntry>> {
        Ok(self
            .entries
            .read()
            .range((ledger_id, 0)..=(ledger_id, u64::MAX))
            .map(|(_, e)| e.clone())
            .collect())
    }
}
