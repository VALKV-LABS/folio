//! Storage-node metrics.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};
use std::sync::OnceLock;

pub struct StorageMetrics {
    pub append_duration: Histogram,
    pub fsync_duration: Histogram,
    pub batch_size_bytes: IntGauge,
    pub fenced_ledgers_total: IntCounter,
    pub entries_written_total: IntCounter,
    pub entries_read_total: IntCounter,
}

// Microsecond buckets from 100 µs → 100 ms.
const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1,
];

pub static STORAGE: OnceLock<StorageMetrics> = OnceLock::new();

pub(super) fn register(r: &Registry) {
    let append = Histogram::with_opts(
        HistogramOpts::new(
            "folio_append_duration_seconds",
            "End-to-end append latency from add_entry call to fsync completion",
        )
        .buckets(LATENCY_BUCKETS.to_vec()),
    )
    .unwrap();

    let fsync = Histogram::with_opts(
        HistogramOpts::new(
            "folio_fsync_duration_seconds",
            "Wall-clock time for a single fsync or io_uring batch commit",
        )
        .buckets(LATENCY_BUCKETS.to_vec()),
    )
    .unwrap();

    let batch = IntGauge::with_opts(Opts::new(
        "folio_batch_size_bytes",
        "Current journal batch size in bytes (updated per group commit)",
    ))
    .unwrap();

    let fenced = IntCounter::with_opts(Opts::new(
        "folio_fenced_ledgers_total",
        "Total number of ledgers fenced on this node",
    ))
    .unwrap();

    let written = IntCounter::with_opts(Opts::new(
        "folio_entries_written_total",
        "Total journal entries written by this node",
    ))
    .unwrap();

    let read = IntCounter::with_opts(Opts::new(
        "folio_entries_read_total",
        "Total journal entries served from index on this node",
    ))
    .unwrap();

    r.register(Box::new(append.clone())).unwrap();
    r.register(Box::new(fsync.clone())).unwrap();
    r.register(Box::new(batch.clone())).unwrap();
    r.register(Box::new(fenced.clone())).unwrap();
    r.register(Box::new(written.clone())).unwrap();
    r.register(Box::new(read.clone())).unwrap();

    STORAGE.get_or_init(|| StorageMetrics {
        append_duration: append,
        fsync_duration: fsync,
        batch_size_bytes: batch,
        fenced_ledgers_total: fenced,
        entries_written_total: written,
        entries_read_total: read,
    });
}
