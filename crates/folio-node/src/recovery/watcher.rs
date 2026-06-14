use crate::recovery::RecoveryManager;
use folio_core::metadata::MetadataStore;
use folio_core::protocol::LedgerMetadata;
use std::sync::Arc;

pub struct NodeWatcher<M: MetadataStore> {
    recovery_manager: Arc<RecoveryManager<M>>,
}

impl<M: MetadataStore> NodeWatcher<M> {
    pub fn new(recovery_manager: Arc<RecoveryManager<M>>) -> Self {
        Self { recovery_manager }
    }

    pub async fn handle_node_failure(
        &self,
        failed_node_id: &str,
    ) -> folio_core::error::Result<Vec<LedgerMetadata>> {
        let ledgers = self.recovery_manager.metadata.list_ledgers().await?;
        let affected = ledgers
            .into_iter()
            .filter(|metadata| Self::is_node_in_ensemble(metadata, failed_node_id))
            .collect::<Vec<_>>();

        for metadata in &affected {
            // Re-replicate every fragment that contains the failed node,
            // starting from each fragment's first_entry_id.
            for fragment in &metadata.fragments {
                if fragment.ensemble.iter().any(|n| n == failed_node_id) {
                    self.recovery_manager
                        .restore_replication(
                            metadata.clone(),
                            failed_node_id,
                            fragment.first_entry_id,
                        )
                        .await?;
                }
            }
        }

        Ok(affected)
    }

    pub fn is_node_in_ensemble(metadata: &LedgerMetadata, node_id: &str) -> bool {
        metadata
            .fragments
            .iter()
            .any(|fragment| fragment.ensemble.iter().any(|address| address == node_id))
    }
}
