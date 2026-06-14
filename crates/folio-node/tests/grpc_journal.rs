//! gRPC integration test: tonic client → tonic JournalServer
//! over a real loopback TCP connection (no in-process shortcut).

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
        let grpc = JournalGrpcService::new(svc2);
        grpc.serve(addr, None, async {
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
async fn add_and_read_entry_over_grpc() {
    let (addr, _svc, _dir) = start_server("grpc-node").await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let client = JournalGrpcClient::new(channel, "grpc-node");

    let entry = Entry::new(1, 0, 0, b"hello grpc".to_vec());

    let ack = client.add_entry(entry.clone()).await.unwrap();
    assert_eq!(ack.ledger_id, 1);
    assert_eq!(ack.entry_id, 0);

    let result = client.read_entry(1, 0).await.unwrap();
    assert_eq!(result.entry.data, b"hello grpc");
}

#[tokio::test]
async fn fence_over_grpc_blocks_further_writes() {
    let (addr, _svc, _dir) = start_server("grpc-fence-node").await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let client = JournalGrpcClient::new(channel, "grpc-fence-node");

    client
        .add_entry(Entry::new(2, 0, 0, vec![1]))
        .await
        .unwrap();

    let lac = client.fence_ledger(2).await.unwrap();
    assert_eq!(lac, 0);

    let err = client.add_entry(Entry::new(2, 1, 0, vec![2])).await;
    assert!(err.is_err(), "expected write to fail after fencing");
}

#[tokio::test]
async fn get_lac_over_grpc() {
    let (addr, _svc, _dir) = start_server("grpc-lac-node").await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let client = JournalGrpcClient::new(channel, "grpc-lac-node");

    let lac = client.get_lac(3, None).await.unwrap();
    assert_eq!(lac, 0);
}
