//! Verify that FenceLedger gRPC RPC returns FAILED_PRECONDITION for subsequent writes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use folio_core::error::FolioError;
use folio_ledger::StorageNodeClient;
use folio_ledger::protocol::Entry;
use folio_ledger::transport::JournalGrpcClient;
use folio_node::{FileJournal, JournalGrpcService, JournalService, MemoryLedgerIndex};
use tempfile::tempdir;
use tonic::transport::Channel;

async fn spin_up(node_id: &str) -> (SocketAddr, Arc<JournalService>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let journal = Arc::new(FileJournal::new(dir.path().join("j.log")).await.unwrap());
    let index = Arc::new(MemoryLedgerIndex::default());
    let svc = Arc::new(JournalService::new(node_id.to_string(), journal, index));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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

#[tokio::test]
async fn add_entry_after_fence_is_failed_precondition() {
    let (addr, _, _dir) = spin_up("fence-test").await;

    let ch = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = JournalGrpcClient::new(ch, "fence-test");

    let ledger_id = 10;

    client
        .add_entry(Entry::new(ledger_id, 0, 0, vec![0]))
        .await
        .unwrap();

    client.fence_ledger(ledger_id).await.unwrap();

    let err = client
        .add_entry(Entry::new(ledger_id, 1, 0, vec![1]))
        .await
        .unwrap_err();

    assert!(
        matches!(err, FolioError::LedgerFenced(_)),
        "expected LedgerFenced, got {err:?}"
    );
}
