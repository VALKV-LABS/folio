pub mod auditor;
pub mod crash_recovery;
pub mod scrubber;
mod watcher;
pub mod worker;

pub use auditor::Auditor;
pub use crash_recovery::{CrashRecovery, RecoveryStats};
pub use scrubber::Scrubber;
pub use watcher::NodeWatcher;
pub use worker::Worker;

use folio_core::error::{FolioError, Result};
use folio_core::metadata::MetadataStore;
use folio_core::protocol::{LedgerMetadata, LedgerState};
use folio_core::resolver::NodeResolver;
use std::sync::Arc;

pub struct RecoveryManager<M: MetadataStore> {
    pub(crate) metadata: Arc<M>,
    pub(crate) nodes: NodeResolver,
}

impl<M: MetadataStore> RecoveryManager<M> {
    pub fn new(metadata: Arc<M>, nodes: NodeResolver) -> Self {
        Self { metadata, nodes }
    }

    pub async fn initiate_recovery(&self, ledger_id: u64) -> Result<LedgerMetadata> {
        self.metadata
            .transition_state(ledger_id, LedgerState::Open, LedgerState::InRecovery)
            .await
    }

    pub async fn fence_and_seal(&self, ledger_id: u64) -> Result<LedgerMetadata> {
        let metadata = match self
            .metadata
            .transition_state(ledger_id, LedgerState::Open, LedgerState::InRecovery)
            .await
        {
            Ok(m) => m,
            Err(_) => self.metadata.get_ledger(ledger_id).await?,
        };
        let fragment = metadata
            .fragments
            .last()
            .cloned()
            .ok_or(FolioError::InvalidFragment(ledger_id))?;

        let mut lacs = Vec::new();
        for node_id in &fragment.ensemble {
            let client = self.nodes.resolve(node_id)?;
            lacs.push(client.fence_ledger(ledger_id).await?);
        }

        let mut sealed = metadata.clone();
        sealed.state = LedgerState::Closed;
        sealed.last_add_confirmed = *lacs.iter().max().unwrap_or(&0);
        sealed.last_entry_id = sealed.last_add_confirmed.checked_sub(1);
        self.metadata.put_ledger(sealed).await
    }

    pub async fn restore_replication(
        &self,
        metadata: LedgerMetadata,
        failed_node_id: &str,
        from_entry_id: u64,
    ) -> Result<()> {
        // Find the fragment that owns from_entry_id.
        let fragment = metadata
            .fragments
            .iter()
            .rev()
            .find(|f| from_entry_id >= f.first_entry_id)
            .cloned()
            .ok_or(FolioError::InvalidFragment(metadata.id))?;

        let source_node = fragment
            .ensemble
            .iter()
            .find(|n| n.as_str() != failed_node_id)
            .cloned()
            .ok_or_else(|| FolioError::Metadata("no surviving source node found".into()))?;
        let target_node = self
            .metadata
            .list_nodes()
            .await?
            .into_iter()
            .find(|n| n.node_id != failed_node_id && !fragment.ensemble.contains(&n.node_id))
            .ok_or_else(|| FolioError::Metadata("no healthy target node available".into()))?;

        let source = self.nodes.resolve(&source_node)?;
        let target = self.nodes.resolve(&target_node.node_id)?;

        // Copy only the entries that belong to this fragment's range.
        let end_entry_id = metadata
            .fragments
            .iter()
            .find(|f| f.first_entry_id > fragment.first_entry_id)
            .map(|f| f.first_entry_id)
            .unwrap_or(metadata.last_add_confirmed);

        for entry_id in from_entry_id..end_entry_id {
            let read = source.read_entry(metadata.id, entry_id).await?;
            match target.add_entry(read.entry).await {
                Ok(_) => {}
                // Target already has this entry (idempotent re-replication).
                Err(FolioError::InvalidEntryId { .. }) => {}
                Err(e) => return Err(e),
            }
        }

        // Remove the failed node from this fragment's ensemble and add the target.
        let mut updated = metadata.clone();
        if let Some(frag) = updated
            .fragments
            .iter_mut()
            .find(|f| f.first_entry_id == fragment.first_entry_id)
        {
            frag.ensemble.retain(|n| n != failed_node_id);
            frag.ensemble.push(target_node.node_id);
        }
        self.metadata.put_ledger(updated).await?;
        Ok(())
    }
}
