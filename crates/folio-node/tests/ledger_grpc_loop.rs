//! Real ledger loop over gRPC: open node → write entries → read entries back.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use folio_ledger::StorageNodeClient;
use folio_ledger::protocol::Entry;
use folio_ledger::transport::JournalGrpcClient;
use folio_node::{FileJournal, JournalGrpcService, JournalService, MemoryLedgerIndex};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tonic::transport::Channel;

async fn start_server(node_id: &str) -> (SocketAddr, Arc<JournalService>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let journal = Arc::new(
        FileJournal::new(dir.path().join("journal.log"))
            .await
            .unwrap(),
    );
    let index = Arc::new(MemoryLedgerIndex::default());
    let svc = Arc::new(JournalService::new(node_id.to_string(), journal, index));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let svc2 = svc.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        JournalGrpcService::new(svc2)
            .serve(addr, None, async {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();
    });
    std::mem::forget(shutdown_tx);

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, svc, dir)
}

/// Write N entries then read them all back and verify payload and metadata.
#[tokio::test]
async fn write_then_read_full_ledger() {
    let (addr, _, _dir) = start_server("loop-node").await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = JournalGrpcClient::new(channel, "loop-node");

    let ledger_id = 1u64;
    let n = 20u64;

    // Write phase — entry_id is monotonically increasing; lac trails by one.
    for i in 0..n {
        let payload = format!("entry-{i}-data").into_bytes();
        let lac = i.saturating_sub(1);
        let ack = client
            .add_entry(Entry::new(ledger_id, i, lac, payload))
            .await
            .unwrap_or_else(|e| panic!("add_entry({i}) failed: {e}"));
        assert_eq!(ack.ledger_id, ledger_id);
        assert_eq!(ack.entry_id, i);
    }

    // Read phase — verify each entry round-trips correctly.
    for i in 0..n {
        let result = client
            .read_entry(ledger_id, i)
            .await
            .unwrap_or_else(|e| panic!("read_entry({i}) failed: {e}"));
        let e = &result.entry;
        assert_eq!(e.ledger_id, ledger_id);
        assert_eq!(e.entry_id, i);
        assert_eq!(e.data, format!("entry-{i}-data").into_bytes());
        assert!(e.validate_digest(), "digest mismatch on entry {i}");
    }
}

/// Read from an empty ledger returns EntryNotFound.
#[tokio::test]
async fn read_missing_entry_returns_not_found() {
    let (addr, _, _dir) = start_server("loop-node-empty").await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = JournalGrpcClient::new(channel, "loop-node-empty");

    let err = client.read_entry(99, 0).await.unwrap_err();
    assert!(
        matches!(err, folio_core::error::FolioError::EntryNotFound { .. }),
        "expected EntryNotFound, got {err:?}"
    );
}

/// Two ledgers on the same node are independent — entries don't bleed across.
#[tokio::test]
async fn multiple_ledgers_are_isolated() {
    let (addr, _, _dir) = start_server("loop-node-multi").await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = JournalGrpcClient::new(channel, "loop-node-multi");

    // Write 3 entries to ledger A and 3 to ledger B with distinct payloads.
    for i in 0..3u64 {
        client
            .add_entry(Entry::new(
                10,
                i,
                i.saturating_sub(1),
                format!("A-{i}").into_bytes(),
            ))
            .await
            .unwrap();
        client
            .add_entry(Entry::new(
                20,
                i,
                i.saturating_sub(1),
                format!("B-{i}").into_bytes(),
            ))
            .await
            .unwrap();
    }

    for i in 0..3u64 {
        let ra = client.read_entry(10, i).await.unwrap();
        assert_eq!(ra.entry.data, format!("A-{i}").into_bytes());

        let rb = client.read_entry(20, i).await.unwrap();
        assert_eq!(rb.entry.data, format!("B-{i}").into_bytes());
    }
}
