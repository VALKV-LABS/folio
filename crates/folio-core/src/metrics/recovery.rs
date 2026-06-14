//! Recovery subsystem metrics (Auditor + Worker + Scrubber).

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};
use std::sync::OnceLock;

pub struct RecoveryMetrics {
    pub under_replicated_ledgers: IntGauge,
    pub rereplicate_duration: Histogram,
    pub scrub_crc_errors_total: IntCounter,
    pub rereplicate_tasks_total: IntCounter,
    pub rereplicate_failures_total: IntCounter,
}

const REREPLICATE_BUCKETS: &[f64] = &[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0];

pub static RECOVERY: OnceLock<RecoveryMetrics> = OnceLock::new();

pub(super) fn register(r: &Registry) {
    let under_rep = IntGauge::with_opts(Opts::new(
        "folio_under_replicated_ledgers",
        "Current count of ledgers with fewer replicas than the write quorum",
    ))
    .unwrap();

    let rereplicate_dur = Histogram::with_opts(
        HistogramOpts::new(
            "folio_rereplicate_duration_seconds",
            "Wall-clock time to re-replicate a single under-replicated ledger",
        )
        .buckets(REREPLICATE_BUCKETS.to_vec()),
    )
    .unwrap();

    let scrub_crc = IntCounter::with_opts(Opts::new(
        "folio_scrub_crc_errors_total",
        "Total CRC32 mismatches detected by the Scrubber",
    ))
    .unwrap();

    let tasks = IntCounter::with_opts(Opts::new(
        "folio_rereplicate_tasks_total",
        "Total re-replication tasks enqueued by the Auditor",
    ))
    .unwrap();

    let failures = IntCounter::with_opts(Opts::new(
        "folio_rereplicate_failures_total",
        "Total re-replication tasks that failed (will be retried by Auditor)",
    ))
    .unwrap();

    r.register(Box::new(under_rep.clone())).unwrap();
    r.register(Box::new(rereplicate_dur.clone())).unwrap();
    r.register(Box::new(scrub_crc.clone())).unwrap();
    r.register(Box::new(tasks.clone())).unwrap();
    r.register(Box::new(failures.clone())).unwrap();

    RECOVERY.get_or_init(|| RecoveryMetrics {
        under_replicated_ledgers: under_rep,
        rereplicate_duration: rereplicate_dur,
        scrub_crc_errors_total: scrub_crc,
        rereplicate_tasks_total: tasks,
        rereplicate_failures_total: failures,
    });
}
