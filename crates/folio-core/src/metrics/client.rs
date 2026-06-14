//! FolioClient-side metrics (embedded in the client process).

use prometheus::{IntCounter, IntGauge, Opts, Registry};
use std::sync::OnceLock;

pub struct ClientMetrics {
    /// Current LAC lag in entries (entries written - LAC advanced).
    pub lac_lag: IntGauge,
    /// Number of outstanding quorum-ack futures waiting for WQ responses.
    pub quorum_acks_pending: IntGauge,
    /// Speculative read/write that returned faster than primary.
    pub speculative_hits_total: IntCounter,
    /// Total append errors (any error from quorum write).
    pub append_errors_total: IntCounter,
    /// Total fragment changes triggered by node failure mid-write.
    pub fragment_changes_total: IntCounter,
}

pub static CLIENT: OnceLock<ClientMetrics> = OnceLock::new();

pub(super) fn register(r: &Registry) {
    let lac_lag = IntGauge::with_opts(Opts::new(
        "folio_lac_lag",
        "Current LAC lag: entries written but not yet ACKed by all AQ replicas",
    ))
    .unwrap();

    let pending = IntGauge::with_opts(Opts::new(
        "folio_quorum_acks_pending",
        "Outstanding quorum write futures awaiting WQ responses",
    ))
    .unwrap();

    let spec_hits = IntCounter::with_opts(Opts::new(
        "folio_speculative_hits_total",
        "Count of speculative requests that returned before the primary",
    ))
    .unwrap();

    let append_errors = IntCounter::with_opts(Opts::new(
        "folio_append_errors_total",
        "Total append errors (quorum failures, fencing, timeouts)",
    ))
    .unwrap();

    let frag_changes = IntCounter::with_opts(Opts::new(
        "folio_fragment_changes_total",
        "Fragment changes triggered by node failure during append",
    ))
    .unwrap();

    r.register(Box::new(lac_lag.clone())).unwrap();
    r.register(Box::new(pending.clone())).unwrap();
    r.register(Box::new(spec_hits.clone())).unwrap();
    r.register(Box::new(append_errors.clone())).unwrap();
    r.register(Box::new(frag_changes.clone())).unwrap();

    CLIENT.get_or_init(|| ClientMetrics {
        lac_lag,
        quorum_acks_pending: pending,
        speculative_hits_total: spec_hits,
        append_errors_total: append_errors,
        fragment_changes_total: frag_changes,
    });
}
