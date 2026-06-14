use crate::error::{FolioError, Result};
use crate::protocol::{Fragment, LedgerMetadata, LedgerOptions, LedgerState, NodeInfo, now_ms};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp, WatchOptions};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

const LEDGER_PREFIX: &str = "/folio/ledgers/";
const NODE_PREFIX: &str = "/folio/nodes/";
const ID_GEN_KEY: &str = "/folio/id-gen";
const ID_GEN_CAS_RETRIES: usize = 32;

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn next_ledger_id(&self) -> Result<u64>;
    async fn create_ledger(
        &self,
        options: LedgerOptions,
        ensemble: Vec<String>,
    ) -> Result<LedgerMetadata>;
    async fn get_ledger(&self, ledger_id: u64) -> Result<LedgerMetadata>;
    async fn put_ledger(&self, metadata: LedgerMetadata) -> Result<LedgerMetadata>;
    async fn transition_state(
        &self,
        ledger_id: u64,
        expected: LedgerState,
        next: LedgerState,
    ) -> Result<LedgerMetadata>;
    async fn list_ledgers(&self) -> Result<Vec<LedgerMetadata>>;
    async fn delete_ledger(&self, ledger_id: u64) -> Result<()>;
    async fn register_node(&self, node: NodeInfo) -> Result<()>;
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;
}

#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
    inner: Arc<RwLock<InMemoryState>>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    next_id: u64,
    ledgers: BTreeMap<u64, LedgerMetadata>,
    nodes: BTreeMap<String, NodeInfo>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn next_ledger_id(&self) -> Result<u64> {
        let mut inner = self.inner.write();
        inner.next_id += 1;
        Ok(inner.next_id)
    }

    async fn create_ledger(
        &self,
        options: LedgerOptions,
        ensemble: Vec<String>,
    ) -> Result<LedgerMetadata> {
        options.validate()?;
        let ledger_id = self.next_ledger_id().await?;
        let now = now_ms();
        let metadata = LedgerMetadata {
            id: ledger_id,
            state: LedgerState::Open,
            fragments: vec![Fragment {
                first_entry_id: 0,
                ensemble,
                write_quorum: options.write_quorum as u8,
                ack_quorum: options.ack_quorum as u8,
            }],
            last_entry_id: None,
            last_add_confirmed: 0,
            created_at_ms: now,
            updated_at_ms: now,
            version: 1,
        };
        self.inner
            .write()
            .ledgers
            .insert(ledger_id, metadata.clone());
        Ok(metadata)
    }

    async fn get_ledger(&self, ledger_id: u64) -> Result<LedgerMetadata> {
        self.inner
            .read()
            .ledgers
            .get(&ledger_id)
            .cloned()
            .ok_or(FolioError::LedgerNotFound(ledger_id))
    }

    async fn put_ledger(&self, metadata: LedgerMetadata) -> Result<LedgerMetadata> {
        let mut inner = self.inner.write();
        let current = inner
            .ledgers
            .get(&metadata.id)
            .ok_or(FolioError::LedgerNotFound(metadata.id))?;
        if current.version != metadata.version {
            return Err(FolioError::CasConflict);
        }

        let mut updated = metadata;
        updated.updated_at_ms = now_ms();
        updated.version += 1;
        inner.ledgers.insert(updated.id, updated.clone());
        Ok(updated)
    }

    async fn transition_state(
        &self,
        ledger_id: u64,
        expected: LedgerState,
        next: LedgerState,
    ) -> Result<LedgerMetadata> {
        let mut inner = self.inner.write();
        let ledger = inner
            .ledgers
            .get_mut(&ledger_id)
            .ok_or(FolioError::LedgerNotFound(ledger_id))?;
        if ledger.state != expected {
            return Err(FolioError::StateConflict {
                expected,
                found: ledger.state,
            });
        }
        ledger.state = next;
        ledger.version += 1;
        ledger.updated_at_ms = now_ms();
        Ok(ledger.clone())
    }

    async fn list_ledgers(&self) -> Result<Vec<LedgerMetadata>> {
        Ok(self.inner.read().ledgers.values().cloned().collect())
    }

    async fn delete_ledger(&self, ledger_id: u64) -> Result<()> {
        self.inner.write().ledgers.remove(&ledger_id);
        Ok(())
    }

    async fn register_node(&self, node: NodeInfo) -> Result<()> {
        self.inner.write().nodes.insert(node.node_id.clone(), node);
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self.inner.read().nodes.values().cloned().collect())
    }
}

pub struct EtcdMetadataStore {
    client: Client,
}

impl EtcdMetadataStore {
    pub async fn connect(endpoints: &[&str]) -> Result<Self> {
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        Ok(Self { client })
    }

    fn ledger_key(ledger_id: u64) -> String {
        format!("{LEDGER_PREFIX}{ledger_id}")
    }

    fn node_key(node_id: &str) -> String {
        format!("{NODE_PREFIX}{node_id}")
    }

    fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(value).map_err(|e| FolioError::Serialization(e.to_string()))
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| FolioError::Serialization(e.to_string()))
    }

    pub async fn watch_nodes(
        &mut self,
    ) -> std::result::Result<etcd_client::WatchStream, etcd_client::Error> {
        let (_, stream) = self
            .client
            .watch(NODE_PREFIX, Some(WatchOptions::new().with_prefix()))
            .await?;
        Ok(stream)
    }
}

#[async_trait]
impl MetadataStore for EtcdMetadataStore {
    async fn next_ledger_id(&self) -> Result<u64> {
        for _ in 0..ID_GEN_CAS_RETRIES {
            let mut client = self.client.clone();
            let resp = client
                .get(ID_GEN_KEY, None)
                .await
                .map_err(|e| FolioError::Metadata(e.to_string()))?;

            let (next, compare) = match resp.kvs().first() {
                Some(kv) => {
                    let current = String::from_utf8_lossy(kv.value())
                        .parse::<u64>()
                        .map_err(|e| FolioError::Metadata(e.to_string()))?;
                    (
                        current + 1,
                        Compare::mod_revision(ID_GEN_KEY, CompareOp::Equal, kv.mod_revision()),
                    )
                }
                None => (1, Compare::create_revision(ID_GEN_KEY, CompareOp::Equal, 0)),
            };

            let txn = Txn::new().when([compare]).and_then([TxnOp::put(
                ID_GEN_KEY,
                next.to_string(),
                None,
            )]);
            let resp = client
                .txn(txn)
                .await
                .map_err(|e| FolioError::Metadata(e.to_string()))?;
            if resp.succeeded() {
                return Ok(next);
            }
        }

        Err(FolioError::CasConflict)
    }

    async fn create_ledger(
        &self,
        options: LedgerOptions,
        ensemble: Vec<String>,
    ) -> Result<LedgerMetadata> {
        options.validate()?;
        let ledger_id = self.next_ledger_id().await?;
        let now = now_ms();
        let metadata = LedgerMetadata {
            id: ledger_id,
            state: LedgerState::Open,
            fragments: vec![Fragment {
                first_entry_id: 0,
                ensemble,
                write_quorum: options.write_quorum as u8,
                ack_quorum: options.ack_quorum as u8,
            }],
            last_entry_id: None,
            last_add_confirmed: 0,
            created_at_ms: now,
            updated_at_ms: now,
            version: 1,
        };
        let key = Self::ledger_key(ledger_id);
        let txn = Txn::new()
            .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key, Self::encode(&metadata)?, None)]);
        let resp = self
            .client
            .clone()
            .txn(txn)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        if !resp.succeeded() {
            return Err(FolioError::CasConflict);
        }
        Ok(metadata)
    }

    async fn get_ledger(&self, ledger_id: u64) -> Result<LedgerMetadata> {
        let resp = self
            .client
            .clone()
            .get(Self::ledger_key(ledger_id), None)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        match resp.kvs().first() {
            Some(kv) => Self::decode(kv.value()),
            None => Err(FolioError::LedgerNotFound(ledger_id)),
        }
    }

    async fn put_ledger(&self, metadata: LedgerMetadata) -> Result<LedgerMetadata> {
        let current = self.get_ledger(metadata.id).await?;
        if current.version != metadata.version {
            return Err(FolioError::CasConflict);
        }

        let mut updated = metadata;
        updated.updated_at_ms = now_ms();
        updated.version += 1;

        let key = Self::ledger_key(updated.id);
        let compare = Compare::value(key.clone(), CompareOp::Equal, Self::encode(&current)?);
        let txn =
            Txn::new()
                .when([compare])
                .and_then([TxnOp::put(key, Self::encode(&updated)?, None)]);

        let resp = self
            .client
            .clone()
            .txn(txn)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        if !resp.succeeded() {
            return Err(FolioError::CasConflict);
        }
        Ok(updated)
    }

    async fn transition_state(
        &self,
        ledger_id: u64,
        expected: LedgerState,
        next: LedgerState,
    ) -> Result<LedgerMetadata> {
        let current = self.get_ledger(ledger_id).await?;
        if current.state != expected {
            return Err(FolioError::StateConflict {
                expected,
                found: current.state,
            });
        }
        let mut updated = current.clone();
        updated.state = next;
        updated.updated_at_ms = now_ms();
        updated.version += 1;

        let key = Self::ledger_key(ledger_id);
        let compare = Compare::value(key.clone(), CompareOp::Equal, Self::encode(&current)?);
        let txn =
            Txn::new()
                .when([compare])
                .and_then([TxnOp::put(key, Self::encode(&updated)?, None)]);

        let resp = self
            .client
            .clone()
            .txn(txn)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        if !resp.succeeded() {
            return Err(FolioError::CasConflict);
        }
        Ok(updated)
    }

    async fn list_ledgers(&self) -> Result<Vec<LedgerMetadata>> {
        let resp = self
            .client
            .clone()
            .get(LEDGER_PREFIX, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        resp.kvs()
            .iter()
            .map(|kv| Self::decode(kv.value()))
            .collect()
    }

    async fn delete_ledger(&self, ledger_id: u64) -> Result<()> {
        self.client
            .clone()
            .delete(Self::ledger_key(ledger_id), None)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        Ok(())
    }

    async fn register_node(&self, node: NodeInfo) -> Result<()> {
        self.client
            .clone()
            .put(Self::node_key(&node.node_id), Self::encode(&node)?, None)
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let resp = self
            .client
            .clone()
            .get(NODE_PREFIX, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| FolioError::Metadata(e.to_string()))?;
        resp.kvs()
            .iter()
            .map(|kv| Self::decode(kv.value()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> LedgerOptions {
        LedgerOptions {
            ensemble_size: 1,
            write_quorum: 1,
            ack_quorum: 1,
        }
    }

    #[tokio::test]
    async fn in_memory_allocates_unique_ledger_ids() {
        let store = InMemoryMetadataStore::new();

        let first = store
            .create_ledger(options(), vec!["node-0".into()])
            .await
            .unwrap();
        let second = store
            .create_ledger(options(), vec!["node-0".into()])
            .await
            .unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(second.id, first.id + 1);
    }

    #[tokio::test]
    async fn stale_in_memory_ledger_update_returns_cas_conflict() {
        let store = InMemoryMetadataStore::new();
        let base = store
            .create_ledger(options(), vec!["node-0".into()])
            .await
            .unwrap();

        let mut first_writer = base.clone();
        first_writer.last_entry_id = Some(0);
        first_writer.last_add_confirmed = 0;
        store.put_ledger(first_writer).await.unwrap();

        let mut stale_writer = base;
        stale_writer.last_entry_id = Some(1);
        let err = store.put_ledger(stale_writer).await.unwrap_err();

        assert!(matches!(err, FolioError::CasConflict));
    }
}
