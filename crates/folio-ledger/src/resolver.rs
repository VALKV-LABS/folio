//! gRPC-backed `GrpcNodeResolver`: resolves node_id → pooled tonic channel.

use crate::transport::JournalGrpcClient;
use folio_core::error::{FolioError, Result};
use folio_core::resolver::{NodeResolver, StorageNodeClient};
use folio_core::transport::ChannelPool;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::ClientTlsConfig;

#[derive(Clone)]
pub struct GrpcNodeResolver {
    addresses: Arc<RwLock<HashMap<String, String>>>,
    node_resolver: NodeResolver,
    pool: ChannelPool,
    tls: Option<ClientTlsConfig>,
}

impl GrpcNodeResolver {
    /// `addresses`: `node_id → URI` (e.g. `"https://10.0.0.1:9090"`).
    /// `tls`: `None` for plain-text (dev/test), `Some(cfg)` for mTLS.
    pub fn new(
        addresses: HashMap<String, String>,
        pool: ChannelPool,
        tls: Option<ClientTlsConfig>,
    ) -> Self {
        let resolver = Self {
            addresses: Arc::new(RwLock::new(HashMap::new())),
            node_resolver: NodeResolver::default(),
            pool,
            tls,
        };
        // populate node_resolver from initial addresses
        resolver.apply_addresses(addresses);
        resolver
    }

    /// Replace the address map and push updated clients into the shared `NodeResolver`.
    /// All clones of this resolver (including those held by `FolioClient`) see
    /// the new nodes immediately.
    pub fn update(&self, addresses: HashMap<String, String>) {
        self.apply_addresses(addresses);
    }

    fn apply_addresses(&self, addresses: HashMap<String, String>) {
        *self.addresses.write() = addresses.clone();
        let client_map: HashMap<String, Arc<dyn StorageNodeClient>> = addresses
            .keys()
            .filter_map(|node_id| {
                self.resolve_client(node_id)
                    .ok()
                    .map(|c| (node_id.clone(), c))
            })
            .collect();
        self.node_resolver.update(client_map);
    }

    fn resolve_client(&self, node_id: &str) -> Result<Arc<dyn StorageNodeClient>> {
        let addr = self
            .addresses
            .read()
            .get(node_id)
            .cloned()
            .ok_or_else(|| FolioError::Metadata(format!("unknown node: {node_id}")))?;

        let channel = self
            .pool
            .get_or_insert(&addr, self.tls.clone())
            .map_err(|e| FolioError::Storage(format!("channel error for {node_id}: {e}")))?;

        Ok(Arc::new(JournalGrpcClient::new(channel, node_id)))
    }

    pub fn resolve(&self, node_id: &str) -> Result<Arc<dyn StorageNodeClient>> {
        self.resolve_client(node_id)
    }

    /// Return the live `NodeResolver` backed by this resolver's address map.
    /// Clones of the returned resolver share the same live map — all see updates
    /// from future `GrpcNodeResolver::update()` calls.
    pub fn as_node_resolver(&self) -> NodeResolver {
        self.node_resolver.clone()
    }
}
