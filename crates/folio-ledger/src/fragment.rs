//! Fragment change logic — FR-CS-04.

use folio_core::error::{FolioError, Result};
use folio_core::metadata::MetadataStore;
use folio_core::protocol::{Fragment, LedgerMetadata, NodeStatus};
use std::collections::HashSet;
use std::sync::Arc;

pub async fn change_fragment<M: MetadataStore>(
    metadata_store: &Arc<M>,
    ledger: &mut LedgerMetadata,
    failed_node_ids: &[&str],
    next_entry_id: u64,
) -> Result<()> {
    let current = ledger
        .fragments
        .last()
        .cloned()
        .ok_or(FolioError::InvalidFragment(ledger.id))?;

    let write_quorum = current.write_quorum;
    let ack_quorum = current.ack_quorum;
    let failed: HashSet<&str> = failed_node_ids.iter().copied().collect();

    let mut new_ensemble: Vec<String> = current
        .ensemble
        .iter()
        .filter(|n| !failed.contains(n.as_str()))
        .cloned()
        .collect();

    if !failed.is_empty() {
        let current_members: HashSet<&str> = current.ensemble.iter().map(|s| s.as_str()).collect();
        let spares: Vec<String> = metadata_store
            .list_nodes()
            .await?
            .into_iter()
            .filter(|n| n.status == NodeStatus::ReadWrite)
            .filter(|n| !current_members.contains(n.node_id.as_str()))
            .take(failed.len())
            .map(|n| n.node_id)
            .collect();

        if new_ensemble.len() + spares.len() < write_quorum as usize {
            return Err(FolioError::QuorumNotMet {
                required: write_quorum as usize,
                received: new_ensemble.len() + spares.len(),
            });
        }
        new_ensemble.extend(spares);
    }

    ledger.fragments.push(Fragment {
        first_entry_id: next_entry_id,
        ensemble: new_ensemble,
        write_quorum,
        ack_quorum,
    });
    *ledger = metadata_store.put_ledger(ledger.clone()).await?;
    Ok(())
}
