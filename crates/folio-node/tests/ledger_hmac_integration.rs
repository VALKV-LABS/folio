//! Integration tests for:
//!   1. Ledger-state-on-first-write — bookie records every ledger in Fjall on
//!      the first append, enabling manifest enumeration without scanning locations.
//!   2. Optional HMAC-SHA256 — when a client registers a master key, the bookie
//!      stores it and verifies subsequent entries that carry an HMAC tag.
//!
//! These tests use FjallIndex directly (no io_uring) so they work inside Docker.

use folio_core::protocol::Entry;
use folio_node::{DbConfig, FjallIndex, FolioDb};
use std::sync::Arc;
use tempfile::tempdir;

// ── helpers ───────────────────────────────────────────────────────────────────

fn open_index(dir: &std::path::Path) -> Arc<FjallIndex> {
    FolioDb::open(dir, &DbConfig::default())
        .unwrap()
        .entry_index
}

fn test_key() -> [u8; 32] {
    [0xABu8; 32]
}

// ── Feature 1: ledger manifest (ensure_ledger_known) ─────────────────────────

#[test]
fn first_write_records_ledger_in_manifest() {
    let dir = tempdir().unwrap();
    let idx = open_index(dir.path());

    // Before any write, ledger is unknown.
    assert!(idx.get_ledger_master_key(1).unwrap().is_none());
    assert!(!idx.list_known_ledger_ids().unwrap().contains(&1));

    idx.ensure_ledger_known(1, None).unwrap();

    // After ensure_ledger_known, ledger appears in manifest.
    let ids = idx.list_known_ledger_ids().unwrap();
    assert!(ids.contains(&1));
}

#[test]
fn manifest_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let idx = open_index(dir.path());
        idx.ensure_ledger_known(10, None).unwrap();
        idx.ensure_ledger_known(20, None).unwrap();
    }
    let idx = open_index(dir.path());
    let mut ids = idx.list_known_ledger_ids().unwrap();
    ids.sort();
    assert!(ids.contains(&10));
    assert!(ids.contains(&20));
}

#[test]
fn manifest_contains_only_written_ledgers() {
    let dir = tempdir().unwrap();
    let idx = open_index(dir.path());
    idx.ensure_ledger_known(7, None).unwrap();
    idx.ensure_ledger_known(8, None).unwrap();
    let ids = idx.list_known_ledger_ids().unwrap();
    assert!(ids.contains(&7));
    assert!(ids.contains(&8));
    assert!(!ids.contains(&9));
}

#[test]
fn fenced_ledger_appears_in_manifest() {
    let dir = tempdir().unwrap();
    let idx = open_index(dir.path());
    // fence_ledger internally calls get_ledger_state / insert — also adds to manifest.
    idx.fence_ledger(42).unwrap();
    assert!(idx.list_known_ledger_ids().unwrap().contains(&42));
}

// ── Feature 2: master key storage ────────────────────────────────────────────

#[test]
fn master_key_stored_on_first_write_survives_reopen() {
    let dir = tempdir().unwrap();
    let key = test_key();
    {
        let idx = open_index(dir.path());
        idx.ensure_ledger_known(1, Some(key)).unwrap();
        idx.persist().unwrap();
    }
    let idx = open_index(dir.path());
    assert_eq!(idx.get_ledger_master_key(1).unwrap(), Some(key));
}

#[test]
fn master_key_registered_lazily_after_crc32_only_write() {
    let dir = tempdir().unwrap();
    let key = test_key();
    let idx = open_index(dir.path());

    // First write without key (CRC32-only mode).
    idx.ensure_ledger_known(5, None).unwrap();
    assert!(idx.get_ledger_master_key(5).unwrap().is_none());

    // Later the client registers a master key.
    idx.ensure_ledger_known(5, Some(key)).unwrap();
    assert_eq!(idx.get_ledger_master_key(5).unwrap(), Some(key));
}

#[test]
fn mismatched_master_key_is_rejected() {
    let dir = tempdir().unwrap();
    let idx = open_index(dir.path());
    let key_a = [0x11u8; 32];
    let key_b = [0x22u8; 32];

    idx.ensure_ledger_known(3, Some(key_a)).unwrap();
    let err = idx.ensure_ledger_known(3, Some(key_b)).unwrap_err();
    assert!(
        err.to_string().contains("master key mismatch"),
        "expected mismatch error, got: {err}"
    );
}

#[test]
fn same_master_key_repeated_is_idempotent() {
    let dir = tempdir().unwrap();
    let key = test_key();
    let idx = open_index(dir.path());
    idx.ensure_ledger_known(6, Some(key)).unwrap();
    idx.ensure_ledger_known(6, Some(key)).unwrap(); // must not error
    assert_eq!(idx.get_ledger_master_key(6).unwrap(), Some(key));
}

// ── Feature 3: HMAC verification helpers ─────────────────────────────────────

#[test]
fn hmac_compute_and_validate_roundtrip() {
    let key = test_key();
    let entry = Entry::new(1, 0, 0, b"hello".to_vec()).with_hmac(&key);
    assert!(entry.hmac.is_some());
    assert!(entry.validate_hmac(&key));
}

#[test]
fn tampered_data_fails_hmac() {
    let key = test_key();
    let mut entry = Entry::new(1, 0, 0, b"original".to_vec()).with_hmac(&key);
    entry.data = b"tampered".to_vec();
    assert!(!entry.validate_hmac(&key));
}

#[test]
fn wrong_key_fails_hmac() {
    let key = test_key();
    let entry = Entry::new(1, 0, 0, b"data".to_vec()).with_hmac(&key);
    let other = [0x00u8; 32];
    assert!(!entry.validate_hmac(&other));
}

#[test]
fn entry_without_hmac_field_always_fails_validate_hmac() {
    let key = test_key();
    let entry = Entry::new(1, 0, 0, b"data".to_vec()); // no .with_hmac()
    assert!(!entry.validate_hmac(&key));
}

#[test]
fn crc32_still_valid_alongside_hmac() {
    let key = test_key();
    let entry = Entry::new(1, 0, 0, b"payload".to_vec())
        .with_master_key(key)
        .with_hmac(&key);
    assert!(entry.validate_digest());
    assert!(entry.validate_hmac(&key));
}

// ── Feature 4: fence preserves master key ────────────────────────────────────

#[test]
fn fence_does_not_erase_master_key() {
    let dir = tempdir().unwrap();
    let key = test_key();
    let idx = open_index(dir.path());
    idx.ensure_ledger_known(99, Some(key)).unwrap();
    idx.fence_ledger(99).unwrap();
    // Both master key and fenced state are preserved.
    assert_eq!(idx.get_ledger_master_key(99).unwrap(), Some(key));
    assert!(idx.load_fenced_ledgers().unwrap().contains(&99));
}

// ── Feature 5: Entry HMAC fields clone and compare correctly ─────────────────

#[test]
fn entry_with_hmac_fields_clone_and_compare() {
    let key = test_key();
    let original = Entry::new(2, 5, 3, b"test payload".to_vec())
        .with_master_key(key)
        .with_hmac(&key);

    assert_eq!(original.master_key, Some(key));
    assert!(original.hmac.is_some());
    assert!(original.validate_hmac(&key));
    assert!(original.validate_digest());

    let cloned = original.clone();
    assert_eq!(cloned.master_key, original.master_key);
    assert_eq!(cloned.hmac, original.hmac);
    assert!(cloned.validate_hmac(&key));
}
