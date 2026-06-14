use std::collections::HashMap;
use std::sync::Arc;

use folio_core::metadata::{InMemoryMetadataStore, MetadataStore};
use folio_core::protocol::{NodeInfo, NodeStatus};
use folio_core::resolver::{NodeResolver, StorageNodeClient};
use folio_ledger::FolioClient;
use folio_ledger::protocol::{LedgerOptions, LedgerState};
use folio_node::{FileJournal, JournalService, MemoryLedgerIndex, RecoveryManager};
use tempfile::tempdir;

async fn make_node(base: &std::path::Path, node_id: &str) -> Arc<dyn StorageNodeClient> {
    tokio::fs::create_dir_all(base.join(node_id)).await.unwrap();
    let journal = Arc::new(
        FileJournal::new(base.join(node_id).join("journal.log"))
            .await
            .expect("journal"),
    );
    let index = Arc::new(MemoryLedgerIndex::default());
    Arc::new(JournalService::new(node_id.to_string(), journal, index))
}

#[tokio::test]
async fn append_and_read_across_quorum() {
    let temp = tempdir().expect("tempdir");
    let node_a = make_node(temp.path(), "node-a").await;
    let node_b = make_node(temp.path(), "node-b").await;
    let node_c = make_node(temp.path(), "node-c").await;

    let resolver = NodeResolver::new(HashMap::from([
        ("node-a".to_string(), node_a),
        ("node-b".to_string(), node_b),
        ("node-c".to_string(), node_c),
    ]));
    let metadata = Arc::new(InMemoryMetadataStore::new());
    let client = FolioClient::new(metadata, resolver);

    let mut handle = client
        .create_ledger(
            LedgerOptions {
                ensemble_size: 3,
                write_quorum: 3,
                ack_quorum: 2,
            },
            vec![
                "node-a".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
            ],
        )
        .await
        .expect("ledger created");

    let entry_id = handle
        .append(b"hello distributed log".to_vec())
        .await
        .expect("append");
    let data = handle.read(entry_id).await.expect("read");
    assert_eq!(data, b"hello distributed log");
}

#[tokio::test]
async fn fence_and_reject_future_writes() {
    let temp = tempdir().expect("tempdir");
    let node_a = make_node(temp.path(), "node-a").await;
    let node_b = make_node(temp.path(), "node-b").await;
    let node_c = make_node(temp.path(), "node-c").await;

    let resolver = NodeResolver::new(HashMap::from([
        ("node-a".to_string(), node_a.clone()),
        ("node-b".to_string(), node_b.clone()),
        ("node-c".to_string(), node_c.clone()),
    ]));
    let metadata = Arc::new(InMemoryMetadataStore::new());
    let client = FolioClient::new(metadata.clone(), resolver.clone());

    let mut handle = client
        .create_ledger(
            LedgerOptions {
                ensemble_size: 3,
                write_quorum: 3,
                ack_quorum: 2,
            },
            vec![
                "node-a".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
            ],
        )
        .await
        .expect("ledger created");

    handle
        .append(b"before fence".to_vec())
        .await
        .expect("append");

    let recovery = RecoveryManager::new(metadata, resolver);
    let sealed = recovery
        .fence_and_seal(handle.metadata().id)
        .await
        .expect("sealed");

    assert!(matches!(sealed.state, LedgerState::Closed));
    assert!(handle.append(b"after fence".to_vec()).await.is_err());
}

#[tokio::test]
async fn restore_replication_to_new_node() {
    let temp = tempdir().expect("tempdir");
    let node_a = make_node(temp.path(), "node-a").await;
    let node_b = make_node(temp.path(), "node-b").await;
    let node_c = make_node(temp.path(), "node-c").await;
    let node_d = make_node(temp.path(), "node-d").await;

    let resolver = NodeResolver::new(HashMap::from([
        ("node-a".to_string(), node_a.clone()),
        ("node-b".to_string(), node_b.clone()),
        ("node-c".to_string(), node_c.clone()),
        ("node-d".to_string(), node_d.clone()),
    ]));
    let metadata = Arc::new(InMemoryMetadataStore::new());

    for id in ["node-a", "node-b", "node-c", "node-d"] {
        metadata
            .register_node(NodeInfo {
                node_id: id.to_string(),
                address: id.to_string(),
                status: NodeStatus::ReadWrite,
                last_heartbeat_ms: 1,
            })
            .await
            .expect("register");
    }

    let client = FolioClient::new(metadata.clone(), resolver.clone());
    let mut handle = client
        .create_ledger(
            LedgerOptions {
                ensemble_size: 3,
                write_quorum: 3,
                ack_quorum: 2,
            },
            vec![
                "node-a".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
            ],
        )
        .await
        .expect("ledger created");

    handle.append(b"entry-0".to_vec()).await.expect("append 0");
    handle.append(b"entry-1".to_vec()).await.expect("append 1");

    let recovery = RecoveryManager::new(metadata.clone(), resolver);
    recovery
        .restore_replication(handle.metadata().clone(), "node-c", 0)
        .await
        .expect("restore");

    let updated = metadata
        .get_ledger(handle.metadata().id)
        .await
        .expect("metadata");
    let ensemble = &updated.fragments.last().expect("fragment").ensemble;
    assert!(ensemble.contains(&"node-d".to_string()));
    assert!(!ensemble.contains(&"node-c".to_string()));
}
