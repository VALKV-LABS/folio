# Folio

[![CI](https://github.com/valkv-labs/folio/actions/workflows/ci.yml/badge.svg)](https://github.com/valkv-labs/folio/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/folio-core.svg)](https://crates.io/crates/folio-core)

Distributed append-only ledger engine in Rust. Replicated, crash-safe, with an optional KV store on top.

Folio implements a BookKeeper-style distributed ledger: multiple storage nodes (_bookies_) hold segments of each ledger in a quorum. A writer appends entries across the ensemble; readers can follow at the _Last Add Confirmed_ cursor. The engine handles node failure, crash recovery, re-replication, and segment tiering to S3-compatible object storage.

## Architecture

```
┌─────────────────────────────────────┐
│         your application            │
│   folio-ledger (LedgerHandle API)   │
└──────────────┬──────────────────────┘
               │ gRPC (append / read)
       ┌───────▼────────────────────────────────────┐
       │              bookie ensemble                │
       │  folio-node   folio-node   folio-node  ...  │
       │  WAL + LSM-C  WAL + LSM-C  WAL + LSM-C     │
       └───────┬────────────────────────────────────┘
               │
      etcd (metadata + leader election)
      S3 / MinIO (cold segment offload)
```

## Crates

| Crate | Description |
|---|---|
| [`folio-core`](crates/folio-core/) | Protocol types, gRPC transport, etcd metadata store, metrics |
| [`folio-ledger`](crates/folio-ledger/) | Client library — `LedgerHandle`, quorum writes, LAC tracking, speculative reads |
| [`folio-node`](crates/folio-node/) | Storage node — WAL, LSM-C engine, crash recovery, auditor, S3 offload |

## Quick start

**Prerequisites:** Docker, Docker Compose.

```bash
git clone https://github.com/valkv-labs/folio.git
cd folio
make build   # build folio/node:latest
make up      # start etcd + minio + folio-node
```

The journal node is now accepting gRPC connections on `:9090` and serving Prometheus metrics on `:9092`. MinIO console is available at `http://localhost:9001` (user: `minioadmin`, password: `minioadmin`).

```bash
make logs    # tail all container logs
make down    # stop everything
```

## Building from source

**Prerequisites:** Rust (stable), `protoc` (protobuf compiler).

```bash
# Install protoc on Debian/Ubuntu
sudo apt-get install protobuf-compiler

# Install protoc on macOS
brew install protobuf

cargo build --workspace
cargo build --release --bins
```

The release binary is at `target/release/folio-node`.

## Running tests

```bash
# Fast tests — no io_uring required (macOS / WSL safe)
make unit

# All tests — requires Linux kernel ≥ 5.1 (io_uring)
make test

# Full suite inside Docker (handles seccomp automatically)
make docker-test
```

> **Note:** Some tests use `tokio-uring` which requires `io_uring` support and `seccomp:unconfined`. On macOS or Windows, use `make unit` or `make docker-test`.

### Benchmarks

Throughput benchmarks are excluded from CI (they require dedicated hardware to be meaningful). Run them manually:

```bash
cargo test lsmc_10k_1kb_concurrent_ledgers_throughput -- --ignored --nocapture
```

## Configuration

`folio-node` is configured via environment variables or CLI flags:

| Variable | Default | Description |
|---|---|---|
| `NODE_ID` | `node-0` | Unique identifier for this node |
| `NODE_ADDRESS` | `http://127.0.0.1:9090` | Advertised gRPC address |
| `LISTEN_ADDR` | `0.0.0.0:9090` | gRPC listen address |
| `DATA_DIR` | `/data/folio` | Local data directory |
| `ETCD_ENDPOINTS` | `http://127.0.0.1:2379` | Comma-separated etcd endpoints |
| `SEGMENT_SEAL_THRESHOLD_MB` | `128` | WAL segment seal size |
| `CACHE_MAX_BYTES` | `536870912` | Entry cache size (bytes) |
| `MAX_JOURNAL_BYTES` | `429496729600` | Max total journal size before health goes read-only |
| `S3_BUCKET` | — | S3 bucket for cold segment offload (optional) |
| `S3_ENDPOINT` | — | S3 endpoint override (for MinIO / custom S3) |
| `S3_REGION` | `us-east-1` | S3 region |
| `TLS_CA_CERT` | — | Path to CA certificate PEM (enables TLS) |
| `TLS_NODE_CERT` | — | Path to node certificate PEM |
| `TLS_NODE_KEY` | — | Path to node private key PEM |
| `METRICS_ADDR` | `0.0.0.0:9092` | Prometheus metrics listen address |
| `RUST_LOG` | `info` | Log filter (e.g. `info,folio_node=debug`) |

Run `folio-node --help` for the full list.

## Using as a library

Add to your `Cargo.toml`:

```toml
folio-core   = { git = "https://github.com/valkv-labs/folio" }
folio-ledger = { git = "https://github.com/valkv-labs/folio" }
```

```rust
use folio_ledger::{FolioClient, LedgerHandle};

let client = FolioClient::connect("http://node-0:9090").await?;
let mut ledger = client.create_ledger(ensemble_size, write_quorum, ack_quorum).await?;

ledger.append(b"hello world").await?;
let entry = ledger.read(0).await?;
```

## Contributing

Contributions are welcome. Please open an issue before starting work on a large change.

1. Fork the repo and create a branch from `main`
2. Add tests for any new behaviour
3. Run `make check` and `make docker-test` before opening a PR
4. Open a pull request — CI must pass before merge

## License

MIT — see [LICENSE](LICENSE).
