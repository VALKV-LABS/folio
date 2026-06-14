use crate::fragment::change_fragment;
use crate::lac_tracker::LacTracker;
use crate::speculative::speculative_read;
use folio_core::error::{FolioError, Result};
use folio_core::metadata::MetadataStore;
use folio_core::metrics::CLIENT;
use folio_core::protocol::{Entry, LedgerMetadata, LedgerState};
use folio_core::resolver::NodeResolver;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;

pub struct LedgerHandle<M: MetadataStore> {
    metadata_store: Arc<M>,
    nodes: NodeResolver,
    pub(crate) metadata: LedgerMetadata,
    next_entry_id: u64,
    lac: LacTracker,
    hedge_delay: Duration,
}

impl<M: MetadataStore> LedgerHandle<M> {
    pub(crate) fn new(
        metadata_store: Arc<M>,
        nodes: NodeResolver,
        metadata: LedgerMetadata,
        hedge_delay: Duration,
    ) -> Self {
        let next_entry_id = metadata.last_entry_id.map(|v| v + 1).unwrap_or(0);
        let lac = LacTracker::new(metadata.last_add_confirmed);
        Self {
            metadata_store,
            nodes,
            metadata,
            next_entry_id,
            lac,
            hedge_delay,
        }
    }

    pub fn metadata(&self) -> &LedgerMetadata {
        &self.metadata
    }

    // ── Writes ────────────────────────────────────────────────────────────

    pub async fn append(&mut self, data: impl Into<Vec<u8>>) -> Result<u64> {
        self.do_append(data.into()).await
    }

    async fn do_append(&mut self, data: Vec<u8>) -> Result<u64> {
        if self.metadata.state != LedgerState::Open {
            record_append_error();
            return Err(FolioError::StateConflict {
                expected: LedgerState::Open,
                found: self.metadata.state,
            });
        }

        for attempt in 0..2usize {
            let fragment = self
                .metadata
                .fragments
                .last()
                .cloned()
                .ok_or(FolioError::InvalidFragment(self.metadata.id))?;

            let entry = Entry::new(
                self.metadata.id,
                self.next_entry_id,
                self.lac.current(),
                data.clone(),
            );

            let node_ids: Vec<String> = fragment
                .ensemble
                .iter()
                .take(fragment.write_quorum as usize)
                .cloned()
                .collect();

            let futures = node_ids.iter().map(|nid| {
                let nid = nid.clone();
                let client_res = self.nodes.resolve(&nid);
                let entry = entry.clone();
                async move {
                    let result = match client_res {
                        Ok(c) => c.add_entry(entry).await,
                        Err(e) => Err(e),
                    };
                    (nid, result)
                }
            });
            set_quorum_acks_pending(node_ids.len() as i64);
            let outcomes: Vec<(String, Result<_>)> = join_all(futures).await;
            set_quorum_acks_pending(0);

            let mut acks = Vec::new();
            let mut failed_nodes: Vec<String> = Vec::new();
            let mut fenced = false;

            for (nid, result) in outcomes {
                match result {
                    Ok(ack) => acks.push(ack),
                    Err(FolioError::LedgerFenced(_)) => {
                        fenced = true;
                        break;
                    }
                    Err(_) => failed_nodes.push(nid),
                }
            }

            if fenced {
                record_append_error();
                return Err(FolioError::LedgerFenced(entry.ledger_id));
            }

            if acks.len() >= fragment.ack_quorum as usize {
                self.lac.advance(&acks, entry.entry_id);
                set_lac_lag(entry.entry_id.saturating_sub(self.lac.current()) as i64);
                let mut updated = self.metadata.clone();
                updated.last_add_confirmed = self.lac.current();
                updated.last_entry_id = Some(entry.entry_id);
                self.metadata = updated;
                self.next_entry_id += 1;
                if !failed_nodes.is_empty() {
                    let refs: Vec<&str> = failed_nodes.iter().map(|s| s.as_str()).collect();
                    if let Err(e) = change_fragment(
                        &self.metadata_store,
                        &mut self.metadata,
                        &refs,
                        self.next_entry_id,
                    )
                    .await
                    {
                        tracing::warn!(
                            ledger_id = self.metadata.id,
                            failed = ?failed_nodes,
                            "proactive fragment change skipped: {e}"
                        );
                    } else {
                        record_fragment_change();
                    }
                }
                return Ok(entry.entry_id);
            }

            if !failed_nodes.is_empty() && attempt == 0 {
                let refs: Vec<&str> = failed_nodes.iter().map(|s| s.as_str()).collect();
                if change_fragment(
                    &self.metadata_store,
                    &mut self.metadata,
                    &refs,
                    self.next_entry_id,
                )
                .await
                .is_ok()
                {
                    record_fragment_change();
                    continue;
                }
            }

            record_append_error();
            return Err(FolioError::QuorumNotMet {
                required: fragment.ack_quorum as usize,
                received: acks.len(),
            });
        }

        record_append_error();
        Err(FolioError::QuorumNotMet {
            required: 0,
            received: 0,
        })
    }

    // ── Reads ─────────────────────────────────────────────────────────────

    pub async fn read(&self, entry_id: u64) -> Result<Vec<u8>> {
        let Some(last_entry_id) = self.metadata.last_entry_id else {
            return Err(FolioError::EntryNotFound {
                ledger_id: self.metadata.id,
                entry_id,
            });
        };
        if entry_id > last_entry_id || entry_id > self.lac.current() {
            return Err(FolioError::EntryNotFound {
                ledger_id: self.metadata.id,
                entry_id,
            });
        }
        let fragment = self
            .metadata
            .fragments
            .iter()
            .rev()
            .find(|f| entry_id >= f.first_entry_id)
            .cloned()
            .ok_or(FolioError::InvalidFragment(self.metadata.id))?;

        let clients: Vec<_> = fragment
            .ensemble
            .iter()
            .filter_map(|nid| self.nodes.resolve(nid).ok())
            .collect();

        let result =
            speculative_read(&clients, self.metadata.id, entry_id, self.hedge_delay).await?;
        Ok(result.entry.data)
    }
}

fn set_quorum_acks_pending(value: i64) {
    if let Some(m) = CLIENT.get() {
        m.quorum_acks_pending.set(value);
    }
}

fn set_lac_lag(value: i64) {
    if let Some(m) = CLIENT.get() {
        m.lac_lag.set(value);
    }
}

fn record_append_error() {
    if let Some(m) = CLIENT.get() {
        m.append_errors_total.inc();
    }
}

fn record_fragment_change() {
    if let Some(m) = CLIENT.get() {
        m.fragment_changes_total.inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use folio_core::metadata::{InMemoryMetadataStore, MetadataStore};
    use folio_core::protocol::{Fragment, LedgerOptions};

    #[tokio::test]
    async fn empty_ledger_read_returns_entry_not_found() {
        let metadata_store = Arc::new(InMemoryMetadataStore::new());
        let metadata = LedgerMetadata {
            id: 1,
            state: LedgerState::Open,
            fragments: vec![Fragment {
                first_entry_id: 0,
                ensemble: vec![],
                write_quorum: 1,
                ack_quorum: 1,
            }],
            last_entry_id: None,
            last_add_confirmed: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
            version: 1,
        };

        let handle = LedgerHandle::new(
            metadata_store,
            NodeResolver::default(),
            metadata,
            Duration::from_millis(0),
        );

        let err = handle.read(0).await.unwrap_err();

        assert!(matches!(
            err,
            FolioError::EntryNotFound {
                ledger_id: 1,
                entry_id: 0
            }
        ));
    }

    #[tokio::test]
    async fn newly_created_ledger_read_returns_entry_not_found() {
        let metadata_store = Arc::new(InMemoryMetadataStore::new());
        let metadata = metadata_store
            .create_ledger(
                LedgerOptions {
                    ensemble_size: 1,
                    write_quorum: 1,
                    ack_quorum: 1,
                },
                vec!["node-0".into()],
            )
            .await
            .unwrap();
        let handle = LedgerHandle::new(
            metadata_store,
            NodeResolver::default(),
            metadata,
            Duration::from_millis(0),
        );

        assert!(matches!(
            handle.read(0).await.unwrap_err(),
            FolioError::EntryNotFound { .. }
        ));
    }
}
