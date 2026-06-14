//! `StorageNodeClient` trait and in-process `NodeResolver`.
//!
//! Both the storage node (recovery Worker/Scrubber) and the client library
//! depend on these types, so they live in the shared core crate.
//! The gRPC-backed `GrpcNodeResolver` lives in `folio-ledger` (client crate).

use crate::error::{FolioError, Result};
use crate::protocol::{AppendAck, Entry, ReadResult};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[async_trait]
pub trait StorageNodeClient: Send + Sync {
    async fn add_entry(&self, entry: Entry) -> Result<AppendAck>;
    async fn fence_ledger(&self, ledger_id: u64) -> Result<u64>;
    async fn read_entry(&self, ledger_id: u64, entry_id: u64) -> Result<ReadResult>;
    async fn get_lac(&self, ledger_id: u64, wait_timeout: Option<Duration>) -> Result<u64>;
}

/// In-process resolver: maps node_id → `Arc<dyn StorageNodeClient>`.
///
/// Used in unit tests and the MVP single-node path. Production uses
/// `GrpcNodeResolver` from the `folio-ledger` client crate.
///
/// The inner map is behind an `Arc<RwLock<...>>` so clones of the same
/// resolver (e.g. held by `FolioClient` and `LedgerHandle`) all see live
/// node-map updates pushed by `GrpcNodeResolver::update()`.
#[derive(Clone, Default)]
pub struct NodeResolver {
    inner: Arc<RwLock<HashMap<String, Arc<dyn StorageNodeClient>>>>,
}

impl NodeResolver {
    pub fn new(nodes: HashMap<String, Arc<dyn StorageNodeClient>>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(nodes)),
        }
    }

    /// Replace the entire node map. All existing clones of this resolver see
    /// the new map immediately (they share the same `Arc<RwLock<...>>`).
    pub fn update(&self, nodes: HashMap<String, Arc<dyn StorageNodeClient>>) {
        *self.inner.write() = nodes;
    }

    pub fn resolve(&self, node_id: &str) -> Result<Arc<dyn StorageNodeClient>> {
        self.inner
            .read()
            .get(node_id)
            .cloned()
            .ok_or_else(|| FolioError::Metadata(format!("unknown node: {node_id}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{AppendAck, Entry, ReadResult};

    struct FakeClient;

    #[async_trait::async_trait]
    impl StorageNodeClient for FakeClient {
        async fn add_entry(&self, _: Entry) -> Result<AppendAck> {
            unimplemented!()
        }
        async fn fence_ledger(&self, _: u64) -> Result<u64> {
            unimplemented!()
        }
        async fn read_entry(&self, _: u64, _: u64) -> Result<ReadResult> {
            unimplemented!()
        }
        async fn get_lac(&self, _: u64, _: Option<Duration>) -> Result<u64> {
            unimplemented!()
        }
    }

    fn fake() -> Arc<dyn StorageNodeClient> {
        Arc::new(FakeClient)
    }

    #[test]
    fn default_resolver_returns_error_for_any_node() {
        let r = NodeResolver::default();
        assert!(r.resolve("node-a").is_err());
    }

    #[test]
    fn update_is_visible_in_existing_clones() {
        let r = NodeResolver::default();
        let clone = r.clone();

        r.update(HashMap::from([("node-a".to_string(), fake())]));

        assert!(
            clone.resolve("node-a").is_ok(),
            "clone must see updated nodes"
        );
        assert!(r.resolve("node-a").is_ok());
    }

    #[test]
    fn update_replaces_previous_nodes() {
        let r = NodeResolver::new(HashMap::from([("old".to_string(), fake())]));
        let clone = r.clone();

        r.update(HashMap::from([("new".to_string(), fake())]));

        assert!(clone.resolve("new").is_ok());
        assert!(
            clone.resolve("old").is_err(),
            "old node must disappear after update"
        );
    }

    #[test]
    fn resolve_unknown_node_returns_error() {
        let r = NodeResolver::new(HashMap::from([("node-a".to_string(), fake())]));
        assert!(r.resolve("node-b").is_err());
    }

    #[test]
    fn multiple_clones_all_share_the_same_live_map() {
        let r = NodeResolver::default();
        let c1 = r.clone();
        let c2 = c1.clone();

        r.update(HashMap::from([("x".to_string(), fake())]));

        assert!(c1.resolve("x").is_ok());
        assert!(c2.resolve("x").is_ok());

        // Second update — clears x, adds y.
        c1.update(HashMap::from([("y".to_string(), fake())]));
        assert!(r.resolve("y").is_ok());
        assert!(r.resolve("x").is_err());
        assert!(c2.resolve("y").is_ok());
    }
}
