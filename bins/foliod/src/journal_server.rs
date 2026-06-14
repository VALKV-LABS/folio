use crate::{Args, endpoint_refs};
use anyhow::Context;
use etcd_client::Client;
use folio_core::metadata::{EtcdMetadataStore, MetadataStore};
use folio_core::protocol::{NodeInfo, NodeStatus, now_ms};
use folio_core::resolver::{NodeResolver, StorageNodeClient};
use folio_core::transport::ChannelPool;
use folio_core::transport::TlsPaths;
use folio_ledger::GrpcNodeResolver;
use folio_node::storage::entry_cache::spawn_flush_loop;
use folio_node::{
    Auditor, BackgroundOffloader, BlockCache, CrashRecovery, DEFAULT_LEASE_TTL_SECS,
    DEFAULT_SEAL_THRESHOLD, DbConfig, EntrySegmentCache, FolioDb, HealthMonitor,
    JournalGrpcService, JournalService, LsmcJournal, NodeRegistry, OffloadPolicy, S3Config,
    Scrubber, TieredReader, Worker, build_s3_client,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::info;

#[allow(dead_code)] // fields held for Drop (connections, DB handle, channel pool)
pub(crate) struct JournalRuntime {
    pub(crate) etcd: Client,
    pub(crate) metadata: Arc<EtcdMetadataStore>,
    pub(crate) keyspace: fjall::Keyspace,
    pub(crate) journal_channel_pool: ChannelPool,
    pub(crate) s3_cfg: Option<S3Config>,
    registry_handle: NodeRegistry,
    shutdown_tx: watch::Sender<bool>,
    journal_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl JournalRuntime {
    pub(crate) async fn wait_for_shutdown(mut self) -> anyhow::Result<()> {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.ok();
                tracing::info!("folio-node: SIGINT received, shutting down");
                let _ = self.shutdown_tx.send(true);
            }
            result = &mut self.journal_handle => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(anyhow::anyhow!("journal gRPC task failed: {e}")),
                }
                self.registry_handle.deregister().await.ok();
                tracing::info!("folio-node: clean shutdown complete");
                return Ok(());
            }
        }

        match self.journal_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(anyhow::anyhow!("journal gRPC task failed: {e}")),
        }

        self.registry_handle.deregister().await.ok();
        tracing::info!("folio-node: clean shutdown complete");
        Ok(())
    }
}

pub(crate) async fn boot(args: &Args) -> anyhow::Result<JournalRuntime> {
    let wal_dir = args.data_dir.join("wal");
    let entries_dir = args.data_dir.join("entries");
    tokio::fs::create_dir_all(&wal_dir).await?;
    tokio::fs::create_dir_all(&entries_dir).await?;

    let db_cfg = DbConfig {
        bloom_filter_bits: args.fjall_bloom_filter_bits,
        block_cache_bytes: args.fjall_block_cache_bytes,
    };
    let db = FolioDb::open(&args.data_dir, &db_cfg)?;
    let wal_index = db.wal_index;
    let entry_index = db.entry_index;
    let wal_registry = db.wal_registry;
    let entry_registry = db.entry_registry;
    let keyspace = db.keyspace;

    let seal_bytes = args
        .segment_seal_threshold_mb
        .checked_mul(1024 * 1024)
        .unwrap_or(DEFAULT_SEAL_THRESHOLD);
    let journal = LsmcJournal::open(&wal_dir, seal_bytes, wal_registry.clone()).await?;

    let flush_size = args.entry_flush_size as usize;
    let entry_cache = Arc::new(Mutex::new(EntrySegmentCache::new(flush_size)));
    let (entry_sealed_tx, entry_sealed_rx) = watch::channel::<Option<u64>>(None);

    let stats = CrashRecovery::new(
        wal_index.clone(),
        entry_index.clone(),
        wal_registry.clone(),
        entry_registry.clone(),
        wal_dir.clone(),
        entries_dir.clone(),
        entry_cache.clone(),
    )
    .recover()?;
    tracing::info!(
        entry_seg_entries = stats.entry_seg_entries,
        wal_entries_replayed = stats.wal_entries_replayed,
        torn_writes = stats.torn_writes,
        "crash recovery complete"
    );

    let flush_thread = spawn_flush_loop(
        entry_cache.clone(),
        entries_dir.clone(),
        entry_index.clone(),
        wal_index.clone(),
        entry_registry.clone(),
        entry_sealed_tx,
        seal_bytes,
        args.flush_tick_ms,
    );

    let s3_cfg = args.s3_bucket.clone().map(|bucket| S3Config {
        bucket,
        endpoint: args.s3_endpoint.clone(),
        region: args.s3_region.clone(),
    });
    let s3_pair = match &s3_cfg {
        Some(cfg) => Some((cfg.clone(), build_s3_client(cfg).await)),
        None => None,
    };

    let offload_policy = OffloadPolicy {
        min_age: Duration::from_secs(args.s3_upload_min_age_hours * 3600),
        max_local_bytes: args
            .s3_upload_disk_budget_gb
            .map(|gb| gb * 1024 * 1024 * 1024),
    };
    let offload = BackgroundOffloader::spawn(
        entry_registry.clone(),
        wal_registry.clone(),
        wal_index.clone(),
        s3_cfg.clone(),
        offload_policy,
        entry_sealed_rx,
    );

    let reader = Arc::new(TieredReader::new(
        BlockCache::new(args.cache_max_bytes),
        entry_index.clone(),
        entry_registry.clone(),
        wal_index.clone(),
        wal_registry.clone(),
        s3_pair,
    ));

    let health = Arc::new(HealthMonitor::new(
        args.max_journal_bytes,
        Duration::from_millis(50),
        1000,
    ));
    let node_svc = Arc::new(
        JournalService::new_lsmc(
            args.node_id.clone(),
            journal,
            wal_index,
            entry_index,
            entry_cache,
            reader,
            offload,
            flush_thread,
        )
        .with_health_monitor(health.clone()),
    );

    let etcd_endpoints = args.etcd_endpoints();
    let etcd_endpoint_refs = endpoint_refs(&etcd_endpoints);
    let etcd = Client::connect(&etcd_endpoint_refs, None).await?;

    let node_info = NodeInfo {
        node_id: args.node_id.clone(),
        address: args.address.clone(),
        status: NodeStatus::ReadWrite,
        last_heartbeat_ms: now_ms(),
    };
    let registry_handle =
        NodeRegistry::register(etcd.clone(), &node_info, DEFAULT_LEASE_TTL_SECS).await?;
    health
        .clone()
        .run(etcd.clone(), node_info.clone(), Duration::from_secs(10));

    let metadata = Arc::new(EtcdMetadataStore::connect(&etcd_endpoint_refs).await?);
    let journal_channel_pool =
        ChannelPool::with_channels_per_addr(args.journal_grpc_channels_per_addr);
    spawn_recovery_tasks(
        args,
        metadata.clone(),
        etcd.clone(),
        node_svc.clone(),
        journal_channel_pool.clone(),
    )
    .await?;

    let tls = match (
        args.tls_ca_cert.clone(),
        args.tls_node_cert.clone(),
        args.tls_node_key.clone(),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(TlsPaths::new(ca, cert, key)),
        _ => {
            tracing::warn!("TLS not configured - running plain-text gRPC (dev/test only)");
            None
        }
    };

    let grpc_service = JournalGrpcService::new(node_svc);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let journal_addr = args.listen;
    let journal_handle = tokio::spawn(async move {
        let mut shutdown_rx = shutdown_rx;
        let shutdown = async move {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        };

        grpc_service
            .serve(journal_addr, tls, shutdown)
            .await
            .context("journal gRPC serve")
    });

    info!(addr = %args.listen, "journal service gRPC listening");

    Ok(JournalRuntime {
        etcd,
        metadata,
        keyspace,
        journal_channel_pool,
        s3_cfg,
        registry_handle,
        shutdown_tx,
        journal_handle,
    })
}

async fn spawn_recovery_tasks(
    args: &Args,
    metadata: Arc<EtcdMetadataStore>,
    etcd: Client,
    node_svc: Arc<JournalService>,
    recovery_pool: ChannelPool,
) -> anyhow::Result<()> {
    let (task_tx, task_rx) = mpsc::channel(256);

    let mut auditor = Auditor::new(
        etcd.clone(),
        metadata.clone(),
        args.node_id.clone(),
        task_tx,
    );
    tokio::spawn(async move {
        if let Err(e) = auditor.run().await {
            tracing::error!("Auditor exited: {e}");
        }
    });

    let mut recovery_clients: HashMap<String, Arc<dyn StorageNodeClient>> = HashMap::new();
    recovery_clients.insert(args.node_id.clone(), node_svc);

    let recovery_node_addrs: HashMap<String, String> = metadata
        .list_nodes()
        .await
        .map_err(|e| anyhow::anyhow!("list_nodes for recovery resolver: {e}"))?
        .into_iter()
        .filter(|node| node.node_id != args.node_id)
        .map(|node| (node.node_id, node.address))
        .collect();

    let recovery_grpc_resolver =
        GrpcNodeResolver::new(recovery_node_addrs.clone(), recovery_pool, None);
    for node_id in recovery_node_addrs.keys() {
        match recovery_grpc_resolver.resolve(node_id) {
            Ok(client) => {
                recovery_clients.insert(node_id.clone(), client);
            }
            Err(e) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %e,
                    "recovery resolver: skipping registered node"
                );
            }
        }
    }

    let resolver = NodeResolver::new(recovery_clients);
    let mut worker = Worker::new(metadata.clone(), resolver.clone(), task_rx);
    tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            tracing::error!("Worker exited: {e}");
        }
    });

    let scrubber = Scrubber::new(
        metadata,
        resolver,
        Duration::from_secs(args.scrubber_interval_secs),
    );
    tokio::spawn(async move {
        if let Err(e) = scrubber.run().await {
            tracing::error!("Scrubber exited: {e}");
        }
    });

    Ok(())
}
