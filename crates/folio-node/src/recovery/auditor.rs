//! Auditor: watches etcd for storage node expiry and enqueues re-replication tasks.
//!
//! Exactly one Auditor is active at a time; the rest wait via etcd leader election.
//! On elected, the Auditor watches `/folio/nodes/` for DELETE events (lease expiry)
//! and pushes a `ReReplicationTask` per affected ledger onto the task channel
//! (FR-RS-01, FR-RS-02, FR-RS-05).

use etcd_client::{Client, EventType, WatchOptions};
use folio_core::error::{FolioError, Result};
use folio_core::metadata::MetadataStore;
use folio_core::metrics::RECOVERY;
use folio_core::protocol::{ReReplicationTask, now_ms};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const NODE_PREFIX: &str = "/folio/nodes/";
const ELECTION_NAME: &str = "/folio/auditor/election";
const LEASE_TTL_SECS: i64 = 30;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

pub struct Auditor<M: MetadataStore> {
    etcd: Client,
    metadata: Arc<M>,
    node_id: String,
    task_tx: mpsc::Sender<ReReplicationTask>,
}

impl<M: MetadataStore> Auditor<M> {
    pub fn new(
        etcd: Client,
        metadata: Arc<M>,
        node_id: impl Into<String>,
        task_tx: mpsc::Sender<ReReplicationTask>,
    ) -> Self {
        Self {
            etcd,
            metadata,
            node_id: node_id.into(),
            task_tx,
        }
    }

    /// Runs the Auditor: acquires leadership via etcd election, then watches
    /// for node expiry events until the channel is closed or an error occurs.
    pub async fn run(&mut self) -> Result<()> {
        let lease_id = self.acquire_lease().await?;
        self.start_keepalive(lease_id).await?;
        self.campaign_for_leadership(lease_id).await?;
        self.watch_node_events().await
    }

    async fn acquire_lease(&mut self) -> Result<i64> {
        let resp = self
            .etcd
            .lease_client()
            .grant(LEASE_TTL_SECS, None)
            .await
            .map_err(|e| FolioError::Metadata(format!("auditor lease grant: {e}")))?;
        Ok(resp.id())
    }

    async fn start_keepalive(&mut self, lease_id: i64) -> Result<()> {
        let (mut keeper, mut stream) = self
            .etcd
            .lease_client()
            .keep_alive(lease_id)
            .await
            .map_err(|e| FolioError::Metadata(format!("auditor keepalive init: {e}")))?;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(KEEPALIVE_INTERVAL).await;
                if keeper.keep_alive().await.is_err() {
                    break;
                }
                // Drain the ack stream.
                while stream.message().await.ok().flatten().is_some() {}
            }
        });
        Ok(())
    }

    async fn campaign_for_leadership(&mut self, lease_id: i64) -> Result<()> {
        tracing::info!("auditor {}: campaigning for leadership", self.node_id);
        self.etcd
            .election_client()
            .campaign(ELECTION_NAME, self.node_id.as_bytes(), lease_id)
            .await
            .map_err(|e| FolioError::Metadata(format!("auditor campaign: {e}")))?;
        tracing::info!("auditor {}: elected leader", self.node_id);
        Ok(())
    }

    async fn watch_node_events(&mut self) -> Result<()> {
        let (_watcher, mut stream) = self
            .etcd
            .watch(NODE_PREFIX, Some(WatchOptions::new().with_prefix()))
            .await
            .map_err(|e| FolioError::Metadata(format!("auditor watch setup: {e}")))?;

        while let Some(resp) = stream
            .message()
            .await
            .map_err(|e| FolioError::Metadata(format!("auditor watch stream: {e}")))?
        {
            for event in resp.events() {
                if event.event_type() != EventType::Delete {
                    continue;
                }
                let Some(kv) = event.kv() else { continue };
                let key = std::str::from_utf8(kv.key()).unwrap_or("");
                let failed_node_id = key.strip_prefix(NODE_PREFIX).unwrap_or(key).to_string();
                tracing::info!("auditor: node lease expired: {failed_node_id}");
                if let Err(e) = self.enqueue_for_node(&failed_node_id).await {
                    tracing::error!("auditor: enqueue error for {failed_node_id}: {e}");
                }
            }
        }
        Ok(())
    }

    async fn enqueue_for_node(&self, failed_node_id: &str) -> Result<()> {
        let ledgers = self.metadata.list_ledgers().await?;
        let nodes = self.metadata.list_nodes().await?;
        let mut queued = 0i64;

        for meta in ledgers {
            // Scan every fragment: the failed node may appear in historical
            // fragments too (e.g. before a proactive change_fragment swapped it
            // out), and those entry ranges still need to be replicated.
            for fragment in &meta.fragments {
                if !fragment.ensemble.contains(&failed_node_id.to_string()) {
                    continue;
                }

                // Pick a healthy node not already in this fragment's ensemble.
                let target = nodes.iter().find(|n| {
                    n.node_id != failed_node_id && !fragment.ensemble.contains(&n.node_id)
                });

                let Some(target_node) = target else {
                    tracing::warn!(
                        "auditor: no available target for ledger {} fragment@{} (failed={})",
                        meta.id,
                        fragment.first_entry_id,
                        failed_node_id
                    );
                    continue;
                };

                let task = ReReplicationTask {
                    ledger_id: meta.id,
                    failed_node_id: failed_node_id.to_string(),
                    target_node_id: target_node.node_id.clone(),
                    from_entry_id: fragment.first_entry_id,
                    created_at_ms: now_ms(),
                };
                tracing::debug!(
                    "auditor: queuing re-replication ledger={} from={} target={}",
                    task.ledger_id,
                    task.from_entry_id,
                    task.target_node_id
                );
                self.task_tx
                    .send(task)
                    .await
                    .map_err(|_| FolioError::Storage("auditor: task channel closed".into()))?;
                queued += 1;
                if let Some(m) = RECOVERY.get() {
                    m.rereplicate_tasks_total.inc();
                }
            }
        }
        if let Some(m) = RECOVERY.get() {
            m.under_replicated_ledgers.set(queued);
        }
        Ok(())
    }
}
