mod journal_server;

use clap::Parser;
use folio_core::metrics;
use folio_node::{DEFAULT_FLUSH_TICK_MS, ENTRY_FLUSH_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(name = "folio-node", about = "Folio journal storage node")]
struct Args {
    #[arg(long, env = "NODE_ID", default_value = "node-0")]
    node_id: String,

    #[arg(long, env = "NODE_ADDRESS", default_value = "http://127.0.0.1:9090")]
    address: String,

    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:9090")]
    listen: SocketAddr,

    #[arg(long, env = "DATA_DIR", default_value = "/data/folio")]
    data_dir: PathBuf,

    #[arg(long, env = "ETCD_ENDPOINTS", default_value = "http://127.0.0.1:2379")]
    etcd_endpoints: String,

    #[arg(long, env = "SEGMENT_SEAL_THRESHOLD_MB", default_value_t = 128)]
    segment_seal_threshold_mb: u64,

    #[arg(long, env = "CACHE_MAX_BYTES", default_value_t = 512 * 1024 * 1024)]
    cache_max_bytes: u64,

    #[arg(long, env = "ENTRY_FLUSH_SIZE", default_value_t = ENTRY_FLUSH_SIZE as u64)]
    entry_flush_size: u64,

    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,

    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    #[arg(long, env = "S3_UPLOAD_MIN_AGE_HOURS", default_value_t = 0)]
    s3_upload_min_age_hours: u64,

    #[arg(long, env = "S3_UPLOAD_DISK_BUDGET_GB")]
    s3_upload_disk_budget_gb: Option<u64>,

    #[arg(long, env = "TLS_CA_CERT")]
    tls_ca_cert: Option<PathBuf>,

    #[arg(long, env = "TLS_NODE_CERT")]
    tls_node_cert: Option<PathBuf>,

    #[arg(long, env = "TLS_NODE_KEY")]
    tls_node_key: Option<PathBuf>,

    #[arg(long, env = "MAX_JOURNAL_BYTES", default_value_t = 400 * 1024 * 1024 * 1024)]
    max_journal_bytes: u64,

    #[arg(long, env = "SCRUBBER_INTERVAL_SECS", default_value_t = 3600)]
    scrubber_interval_secs: u64,

    #[arg(long, env = "METRICS_ADDR", default_value = "0.0.0.0:9092")]
    metrics_addr: SocketAddr,

    #[arg(long, env = "FLUSH_TICK_MS", default_value_t = DEFAULT_FLUSH_TICK_MS)]
    flush_tick_ms: u64,

    #[arg(long, env = "FJALL_BLOOM_FILTER_BITS")]
    fjall_bloom_filter_bits: Option<u8>,

    #[arg(long, env = "FJALL_BLOCK_CACHE_BYTES")]
    fjall_block_cache_bytes: Option<u64>,

    #[arg(long, env = "JOURNAL_GRPC_CHANNELS_PER_ADDR", default_value_t = 1usize)]
    journal_grpc_channels_per_addr: usize,
}

impl Args {
    fn etcd_endpoints(&self) -> Vec<String> {
        split_csv(&self.etcd_endpoints)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();
    let args = Args::parse();

    tracing::info!(node_id = %args.node_id, listen = %args.listen, "folio-node starting");

    let _ = metrics::registry();
    spawn_metrics_server(args.metrics_addr);

    let journal = journal_server::boot(&args).await?;
    journal.wait_for_shutdown().await
}

pub(crate) fn endpoint_refs(endpoints: &[String]) -> Vec<&str> {
    endpoints.iter().map(String::as_str).collect()
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn spawn_metrics_server(metrics_addr: SocketAddr) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(metrics_addr)
            .await
            .expect("failed to bind metrics listener");
        tracing::info!(addr = %metrics_addr, "metrics endpoint listening");
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let body = metrics::gather_text();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            }
        }
    });
}
