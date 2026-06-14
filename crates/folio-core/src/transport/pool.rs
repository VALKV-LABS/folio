//! Per-node gRPC channel pool.
//!
//! Channels are created lazily via `Endpoint::connect_lazy`, so `get_or_insert`
//! is synchronous and safe to call from non-async contexts. The underlying
//! `Channel` handles reconnection internally.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Clone)]
pub struct ChannelPool {
    channels_per_addr: usize,
    inner: Arc<RwLock<HashMap<String, Arc<ChannelSet>>>>,
}

impl Default for ChannelPool {
    fn default() -> Self {
        Self::new()
    }
}

struct ChannelSet {
    channels: Vec<Channel>,
    next: AtomicUsize,
}

impl ChannelSet {
    fn new(channels: Vec<Channel>) -> Self {
        Self {
            channels,
            next: AtomicUsize::new(0),
        }
    }

    fn next_channel(&self) -> Channel {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[idx].clone()
    }
}

impl ChannelPool {
    pub fn new() -> Self {
        Self::with_channels_per_addr(1)
    }

    pub fn with_channels_per_addr(channels_per_addr: usize) -> Self {
        Self {
            channels_per_addr: channels_per_addr.clamp(1, 64),
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Return a cached channel for `address`, or create a lazy channel set.
    ///
    /// `address` must be a valid URI string, e.g. `"https://10.0.0.1:9090"` for
    /// mTLS or `"http://127.0.0.1:9090"` for plain-text (dev/test).
    pub fn get_or_insert(
        &self,
        address: &str,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Channel, tonic::transport::Error> {
        {
            let map = self.inner.read();
            if let Some(channels) = map.get(address) {
                return Ok(channels.next_channel());
            }
        }

        let mut channels = Vec::with_capacity(self.channels_per_addr);
        for _ in 0..self.channels_per_addr {
            let mut ep = Endpoint::from_shared(address.to_owned())?
                .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
                .keep_alive_while_idle(true);

            if let Some(tls_cfg) = tls.clone() {
                ep = ep.tls_config(tls_cfg)?;
            }

            channels.push(ep.connect_lazy());
        }

        let channel_set = Arc::new(ChannelSet::new(channels));

        let mut map = self.inner.write();
        // Re-check after acquiring write lock (another thread may have raced).
        let channel_set = map
            .entry(address.to_owned())
            .or_insert_with(|| channel_set.clone())
            .clone();
        Ok(channel_set.next_channel())
    }

    /// Remove a channel from the pool (e.g. after a node is decommissioned).
    pub fn evict(&self, address: &str) {
        self.inner.write().remove(address);
    }
}
