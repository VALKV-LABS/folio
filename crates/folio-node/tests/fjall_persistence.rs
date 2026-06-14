//! FjallIndex persistence: insert entries, reopen from same path, verify all present.
//! Also covers LSM-C locations partition (insert_location / get_location).

use folio_core::protocol::Entry;
use folio_node::storage::{LedgerIndex, StoredEntry};
use folio_node::{FjallIndex, IndexValue};
use tempfile::tempdir;

fn make_stored_entry(ledger_id: u64, entry_id: u64) -> StoredEntry {
    StoredEntry {
        entry: Entry {
            ledger_id,
            entry_id,
            lac: entry_id.saturating_sub(1),
            digest: 0,
            data: vec![entry_id as u8; 32],
            master_key: None,
            hmac: None,
        },
        appended_at_ms: 0,
    }
}

#[test]
fn reopen_and_read_all_entries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("index");

    {
        let index = FjallIndex::new(&path).unwrap();
        for i in 0u64..1_000 {
            index.insert(make_stored_entry(1, i)).unwrap();
        }
    }

    let index = FjallIndex::new(&path).unwrap();
    for i in 0u64..1_000 {
        let stored = index
            .get(1, i)
            .expect("get failed")
            .expect("entry missing after reopen");
        assert_eq!(stored.entry.entry_id, i);
        assert_eq!(stored.entry.data[0], i as u8);
    }
}

#[test]
fn locations_persist_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("index");

    {
        let index = FjallIndex::new(&path).unwrap();
        for i in 0u64..100 {
            // segment_id=42, offset=i*256, length=280
            index.insert_location(1, i, 42, i * 256, 280).unwrap();
        }
    }

    let index = FjallIndex::new(&path).unwrap();
    for i in 0u64..100 {
        let iv = index
            .get_location(1, i)
            .expect("get_location failed")
            .expect("location missing after reopen");
        assert_eq!(
            iv,
            IndexValue {
                segment_id: 42,
                offset: i * 256,
                length: 280
            }
        );
    }
    // Absent entry returns None, not an error.
    assert!(index.get_location(1, 9999).unwrap().is_none());
}
