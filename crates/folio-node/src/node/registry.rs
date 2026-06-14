//! Node registration with etcd — FR-SN-06.
//!
//! On startup the storage node:
//!   1. Grants an etcd lease with a configurable TTL (default 15 s).
//!   2. Writes `/folio/nodes/{node_id}` with `NodeInfo` JSON attached to that
//!      lease, so the key disappears automatically if the node crashes.
//!   3. Spawns a background keep-alive loop that refreshes the lease every
//!      `ttl / 3` seconds.
//!
//! `NodeRegistry::deregister()` revokes the lease immediately (clean shutdown).

use etcd_client::{Client, PutOptions};
use folio_core::error::{FolioError, Result};
use folio_core::protocol::NodeInfo;
use std::time::Duration;

pub const DEFAULT_LEASE_TTL_SECS: i64 = 15;

pub struct NodeRegistry {
    lease_id: i64,
    client: Client,
    pub node_id: String,
    _keep_alive_task: tokio::task::JoinHandle<()>,
}

impl NodeRegistry {
    /// Register `node` in etcd under an ephemeral lease.
    /// Returns a `NodeRegistry` whose drop does NOT automatically revoke the
    /// lease — call `deregister()` for a clean shutdown.
    pub async fn register(mut client: Client, node: &NodeInfo, ttl_secs: i64) -> Result<Self> {
        // 1. Grant lease
        let lease_resp = client
            .lease_grant(ttl_secs, None)
            .await
            .map_err(|e| FolioError::Metadata(format!("etcd lease_grant: {e}")))?;
        let lease_id = lease_resp.id();

        // 2. Write node key attached to lease
        let key = format!("/folio/nodes/{}", node.node_id);
        let value =
            serde_json::to_vec(node).map_err(|e| FolioError::Serialization(e.to_string()))?;
        client
            .put(key, value, Some(PutOptions::new().with_lease(lease_id)))
            .await
            .map_err(|e| FolioError::Metadata(format!("etcd put node key: {e}")))?;

        // 3. Start keep-alive loop
        let keep_alive_interval = Duration::from_secs(((ttl_secs / 3).max(1)) as u64);
        let keep_alive_task = {
            let mut ka_client = client.clone();
            let node_id = node.node_id.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(keep_alive_interval);
                // (keeper, stream) pair; re-created on every reconnect attempt.
                let Ok((mut keeper, mut stream)) = ka_client.lease_keep_alive(lease_id).await
                else {
                    tracing::error!(node_id = %node_id, "NodeRegistry: initial keep-alive stream failed");
                    return;
                };

                loop {
                    ticker.tick().await;

                    let send_ok = keeper.keep_alive().await.is_ok();
                    let recv_ok = send_ok
                        && matches!(
                            stream.message().await,
                            Ok(Some(ref r)) if r.ttl() > 0
                        );

                    if send_ok && recv_ok {
                        tracing::trace!(node_id = %node_id, "NodeRegistry: lease refreshed");
                        continue;
                    }

                    // Stream is broken — reconnect the keep-alive channel.
                    tracing::warn!(
                        node_id = %node_id,
                        "NodeRegistry: keep-alive stream lost — reconnecting"
                    );
                    loop {
                        tokio::time::sleep(keep_alive_interval).await;
                        match ka_client.lease_keep_alive(lease_id).await {
                            Ok((new_keeper, new_stream)) => {
                                keeper = new_keeper;
                                stream = new_stream;
                                tracing::info!(
                                    node_id = %node_id,
                                    "NodeRegistry: keep-alive stream reconnected"
                                );
                                break;
                            }
                            Err(e) => {
                                tracing::error!(
                                    node_id = %node_id,
                                    "NodeRegistry: keep-alive reconnect failed ({e}) — retrying"
                                );
                            }
                        }
                    }
                }
            })
        };

        tracing::info!(
            node_id = %node.node_id,
            lease_id,
            ttl_secs,
            "NodeRegistry: registered in etcd"
        );

        Ok(Self {
            lease_id,
            client,
            node_id: node.node_id.clone(),
            _keep_alive_task: keep_alive_task,
        })
    }

    /// Revoke the lease immediately, causing the etcd key to be deleted.
    /// Call this on clean shutdown so peers notice immediately rather than
    /// waiting for the TTL to expire.
    pub async fn deregister(&mut self) -> Result<()> {
        self.client
            .lease_revoke(self.lease_id)
            .await
            .map_err(|e| FolioError::Metadata(format!("etcd lease_revoke: {e}")))?;
        tracing::info!(node_id = %self.node_id, "NodeRegistry: deregistered from etcd");
        Ok(())
    }

    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }
}
