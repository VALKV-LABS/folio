//! Worker: drains the re-replication task channel and restores ensemble health.
//!
//! For OPEN ledgers the Worker fences and seals the ledger before copying entries,
//! ensuring no new writes land on the failed node after recovery begins.
//! After copying, etcd metadata is updated atomically to reflect the new ensemble
//! (FR-RS-03, FR-RS-04).

use crate::recovery::RecoveryManager;
use folio_core::error::Result;
use folio_core::metadata::MetadataStore;
use folio_core::metrics::RECOVERY;
use folio_core::protocol::{LedgerState, ReReplicationTask};
use folio_core::resolver::NodeResolver;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

pub struct Worker<M: MetadataStore> {
    recovery: Arc<RecoveryManager<M>>,
    task_rx: mpsc::Receiver<ReReplicationTask>,
}

impl<M: MetadataStore> Worker<M> {
    pub fn new(
        metadata: Arc<M>,
        nodes: NodeResolver,
        task_rx: mpsc::Receiver<ReReplicationTask>,
    ) -> Self {
        Self {
            recovery: Arc::new(RecoveryManager::new(metadata, nodes)),
            task_rx,
        }
    }

    /// Drains the task channel until it is closed. Each task is processed
    /// sequentially; errors are logged and do not halt the worker.
    pub async fn run(&mut self) -> Result<()> {
        while let Some(task) = self.task_rx.recv().await {
            let ledger_id = task.ledger_id;
            if let Err(e) = self.process(task).await {
                if let Some(m) = RECOVERY.get() {
                    m.rereplicate_failures_total.inc();
                }
                tracing::error!("worker: ledger {ledger_id} re-replication failed: {e}");
            }
        }
        Ok(())
    }

    async fn process(&self, task: ReReplicationTask) -> Result<()> {
        tracing::info!(
            "worker: starting re-replication ledger={} failed={} target={}",
            task.ledger_id,
            task.failed_node_id,
            task.target_node_id,
        );
        let started = Instant::now();

        let metadata = self.recovery.metadata.get_ledger(task.ledger_id).await?;

        // Fence and seal before copying to prevent split-brain on OPEN ledgers.
        let metadata = if metadata.state == LedgerState::Open {
            tracing::debug!("worker: fencing ledger {}", task.ledger_id);
            self.recovery.fence_and_seal(task.ledger_id).await?
        } else {
            metadata
        };

        self.recovery
            .restore_replication(metadata, &task.failed_node_id, task.from_entry_id)
            .await?;

        if let Some(m) = RECOVERY.get() {
            m.rereplicate_duration
                .observe(started.elapsed().as_secs_f64());
            if m.under_replicated_ledgers.get() > 0 {
                m.under_replicated_ledgers.dec();
            }
        }
        tracing::info!("worker: ledger {} re-replication complete", task.ledger_id);
        Ok(())
    }
}
