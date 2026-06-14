use crate::ledger::LedgerHandle;
use folio_core::error::Result;
use folio_core::metadata::MetadataStore;
use folio_core::protocol::{LedgerMetadata, LedgerOptions, LedgerState};
use folio_core::resolver::NodeResolver;
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_HEDGE_DELAY: Duration = Duration::from_millis(5);
const DEFAULT_LEDGER_CACHE_SIZE: u64 = 10_000;

pub struct FolioClient<M: MetadataStore> {
    metadata: Arc<M>,
    nodes: NodeResolver,
    hedge_delay: Duration,
    ledger_cache: Cache<u64, LedgerMetadata>,
}

impl<M: MetadataStore> FolioClient<M> {
    pub fn new(metadata: Arc<M>, nodes: NodeResolver) -> Self {
        Self {
            metadata,
            nodes,
            hedge_delay: DEFAULT_HEDGE_DELAY,
            ledger_cache: Cache::new(DEFAULT_LEDGER_CACHE_SIZE),
        }
    }

    pub fn with_hedge_delay(mut self, delay: Duration) -> Self {
        self.hedge_delay = delay;
        self
    }

    pub fn node_resolver(&self) -> &NodeResolver {
        &self.nodes
    }

    pub fn metadata_store(&self) -> Arc<M> {
        self.metadata.clone()
    }

    /// Create a new ledger. Appends update the returned handle's in-memory
    /// position; applications that need durable writer progress should persist
    /// their own checkpoint and seal the ledger with a final LAC at handoff.
    pub async fn create_ledger(
        &self,
        options: LedgerOptions,
        ensemble: Vec<String>,
    ) -> Result<LedgerHandle<M>> {
        let metadata = self.metadata.create_ledger(options, ensemble).await?;
        Ok(LedgerHandle::new(
            self.metadata.clone(),
            self.nodes.clone(),
            metadata,
            self.hedge_delay,
        ))
    }

    /// Open an existing ledger. Closed/deleted metadata can be served from the
    /// in-process cache. Open metadata is fetched fresh because lifecycle events
    /// such as fencing, sealing, and fragment changes are metadata-plane state.
    pub async fn open_ledger(&self, ledger_id: u64) -> Result<LedgerHandle<M>> {
        let metadata = match self.ledger_cache.get(&ledger_id).await {
            Some(m) => m,
            None => {
                let m = self.metadata.get_ledger(ledger_id).await?;
                if m.state != LedgerState::Open {
                    self.ledger_cache.insert(ledger_id, m.clone()).await;
                }
                m
            }
        };
        Ok(LedgerHandle::new(
            self.metadata.clone(),
            self.nodes.clone(),
            metadata,
            self.hedge_delay,
        ))
    }

    pub async fn seal_ledger(&self, ledger_id: u64, final_lac: u64) -> Result<()> {
        self.ledger_cache.invalidate(&ledger_id).await;
        let mut meta = self.metadata.get_ledger(ledger_id).await?;
        if meta.state == LedgerState::Closed {
            return Ok(());
        }
        meta.state = LedgerState::Closed;
        meta.last_add_confirmed = final_lac;
        meta.last_entry_id = Some(final_lac);
        self.metadata.put_ledger(meta).await?;
        Ok(())
    }

    pub async fn delete_ledger(&self, ledger_id: u64) -> Result<()> {
        self.ledger_cache.invalidate(&ledger_id).await;
        let mut metadata = self.metadata.get_ledger(ledger_id).await?;
        metadata.state = LedgerState::Deleted;
        self.metadata.put_ledger(metadata).await?;
        self.metadata.delete_ledger(ledger_id).await
    }
}
