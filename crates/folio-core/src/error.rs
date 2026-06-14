use thiserror::Error;

#[derive(Debug, Error)]
pub enum FolioError {
    #[error("ledger not found: {0}")]
    LedgerNotFound(u64),
    #[error("entry not found: ledger={ledger_id} entry={entry_id}")]
    EntryNotFound { ledger_id: u64, entry_id: u64 },
    #[error("ledger is fenced: {0}")]
    LedgerFenced(u64),
    #[error("quorum not met: required={required} received={received}")]
    QuorumNotMet { required: usize, received: usize },
    #[error("ledger state conflict: expected {expected:?}, found {found:?}")]
    StateConflict {
        expected: crate::protocol::LedgerState,
        found: crate::protocol::LedgerState,
    },
    #[error("compare-and-swap conflict")]
    CasConflict,
    #[error("invalid fragment for ledger {0}")]
    InvalidFragment(u64),
    #[error("invalid entry id {entry_id}; expected {expected}")]
    InvalidEntryId { expected: u64, entry_id: u64 },
    #[error("timed out waiting for LAC")]
    LacTimeout,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("metadata error: {0}")]
    Metadata(String),
    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, FolioError>;
