use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LedgerState {
    Open,
    InRecovery,
    Closed,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    ReadWrite,
    ReadOnly,
    Recovering,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,
    pub status: NodeStatus,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fragment {
    pub first_entry_id: u64,
    pub ensemble: Vec<String>,
    pub write_quorum: u8,
    pub ack_quorum: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerMetadata {
    pub id: u64,
    pub state: LedgerState,
    pub fragments: Vec<Fragment>,
    pub last_entry_id: Option<u64>,
    pub last_add_confirmed: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerOptions {
    pub ensemble_size: usize,
    pub write_quorum: usize,
    pub ack_quorum: usize,
}

impl LedgerOptions {
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.ensemble_size == 0 {
            return Err(crate::error::FolioError::Metadata(
                "ensemble size must be greater than zero".to_string(),
            ));
        }
        if self.write_quorum == 0 || self.ack_quorum == 0 {
            return Err(crate::error::FolioError::Metadata(
                "quorums must be greater than zero".to_string(),
            ));
        }
        if self.ack_quorum > self.write_quorum || self.write_quorum > self.ensemble_size {
            return Err(crate::error::FolioError::Metadata(
                "quorum sizes must satisfy ack <= write <= ensemble".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub ledger_id: u64,
    pub entry_id: u64,
    pub lac: u64,
    /// CRC32 of `data` only. Always present — default integrity check.
    pub digest: u32,
    pub data: Vec<u8>,
    /// 32-byte HMAC master key for this ledger. Non-None only on the first write
    /// to a bookie for this ledger, so the bookie can store it. The client holds
    /// the key and re-sends it to register on each new bookie in the ensemble.
    pub master_key: Option<[u8; 32]>,
    /// HMAC-SHA256(master_key, ledger_id||entry_id||lac||data).
    /// When present, the bookie verifies this in addition to the CRC32.
    /// None = CRC32-only mode (backwards compatible).
    pub hmac: Option<[u8; 32]>,
}

impl Entry {
    pub fn new(ledger_id: u64, entry_id: u64, lac: u64, data: Vec<u8>) -> Self {
        let digest = crc32fast::hash(&data);
        Self {
            ledger_id,
            entry_id,
            lac,
            digest,
            data,
            master_key: None,
            hmac: None,
        }
    }

    /// Attach a master key so the bookie stores it on first write for this ledger.
    pub fn with_master_key(mut self, key: [u8; 32]) -> Self {
        self.master_key = Some(key);
        self
    }

    /// Compute and attach HMAC-SHA256 using `master_key`.
    pub fn with_hmac(mut self, master_key: &[u8; 32]) -> Self {
        self.hmac = Some(Self::compute_hmac(
            master_key,
            self.ledger_id,
            self.entry_id,
            self.lac,
            &self.data,
        ));
        self
    }

    /// Compute HMAC-SHA256(master_key, ledger_id_be || entry_id_be || lac_be || data).
    pub fn compute_hmac(
        master_key: &[u8; 32],
        ledger_id: u64,
        entry_id: u64,
        lac: u64,
        data: &[u8],
    ) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(master_key).expect("HMAC accepts any key size");
        mac.update(&ledger_id.to_be_bytes());
        mac.update(&entry_id.to_be_bytes());
        mac.update(&lac.to_be_bytes());
        mac.update(data);
        mac.finalize().into_bytes().into()
    }

    /// Returns true if the entry's HMAC is valid for the given master key.
    /// Uses constant-time comparison (via `hmac::Mac::verify_slice`) to resist timing attacks.
    pub fn validate_hmac(&self, master_key: &[u8; 32]) -> bool {
        let Some(stored) = self.hmac else {
            return false;
        };
        let mut mac = HmacSha256::new_from_slice(master_key).expect("HMAC accepts any key size");
        mac.update(&self.ledger_id.to_be_bytes());
        mac.update(&self.entry_id.to_be_bytes());
        mac.update(&self.lac.to_be_bytes());
        mac.update(&self.data);
        mac.verify_slice(&stored).is_ok()
    }

    pub fn validate_digest(&self) -> bool {
        crc32fast::hash(&self.data) == self.digest
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendAck {
    pub ledger_id: u64,
    pub entry_id: u64,
    pub local_lac: u64,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadResult {
    pub entry: Entry,
    pub node_id: String,
    /// Server-assigned wall-clock timestamp when this entry was appended.
    /// Used by the Reaper for TTL-based compaction decisions.
    /// `0` when reading from the InProcess (test) backend.
    pub appended_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReReplicationTask {
    pub ledger_id: u64,
    pub failed_node_id: String,
    pub target_node_id: String,
    /// First entry_id in the fragment where the failed node participated.
    /// restore_replication starts copying from here, not from 0.
    pub from_entry_id: u64,
    pub created_at_ms: u64,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0xAB; 32]
    }

    #[test]
    fn hmac_compute_is_deterministic() {
        let key = test_key();
        let a = Entry::compute_hmac(&key, 1, 0, 0, b"hello");
        let b = Entry::compute_hmac(&key, 1, 0, 0, b"hello");
        assert_eq!(a, b);
    }

    #[test]
    fn hmac_differs_for_different_inputs() {
        let key = test_key();
        let h1 = Entry::compute_hmac(&key, 1, 0, 0, b"hello");
        let h2 = Entry::compute_hmac(&key, 1, 0, 0, b"world");
        let h3 = Entry::compute_hmac(&key, 2, 0, 0, b"hello"); // different ledger
        let h4 = Entry::compute_hmac(&key, 1, 1, 0, b"hello"); // different entry_id
        assert_ne!(h1, h2);
        assert_ne!(h1, h3);
        assert_ne!(h1, h4);
    }

    #[test]
    fn with_hmac_validates_correctly() {
        let key = test_key();
        let entry = Entry::new(1, 0, 0, b"payload".to_vec()).with_hmac(&key);
        assert!(entry.hmac.is_some());
        assert!(entry.validate_hmac(&key));
    }

    #[test]
    fn wrong_key_fails_validation() {
        let key = test_key();
        let entry = Entry::new(1, 0, 0, b"payload".to_vec()).with_hmac(&key);
        let wrong_key = [0x00u8; 32];
        assert!(!entry.validate_hmac(&wrong_key));
    }

    #[test]
    fn no_hmac_field_fails_validation() {
        let key = test_key();
        let entry = Entry::new(1, 0, 0, b"payload".to_vec());
        assert!(entry.hmac.is_none());
        assert!(!entry.validate_hmac(&key));
    }

    #[test]
    fn with_master_key_builder() {
        let key = test_key();
        let entry = Entry::new(1, 0, 0, b"x".to_vec()).with_master_key(key);
        assert_eq!(entry.master_key, Some(key));
        assert!(entry.hmac.is_none()); // master_key and hmac are independent builders
    }

    #[test]
    fn crc32_digest_unchanged_after_hmac() {
        let key = test_key();
        let data = b"test data".to_vec();
        let plain = Entry::new(1, 0, 0, data.clone());
        let with_hmac = Entry::new(1, 0, 0, data).with_hmac(&key);
        assert_eq!(plain.digest, with_hmac.digest);
        assert!(plain.validate_digest());
        assert!(with_hmac.validate_digest());
    }
}
