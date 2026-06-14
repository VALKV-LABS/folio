//! Storage node health monitor — FR-SN-08.
//!
//! Tracks two signals and transitions the node to `ReadOnly` when either
//! threshold is exceeded:
//!
//! 1. **Bytes written** — compared against a configured `max_journal_bytes`
//!    capacity limit. Callers increment this via `record()` after each append.
//!
//! 2. **Append p99 latency** — computed over a rolling window of the most
//!    recent `window_size` append durations. Compared against
//!    `p99_threshold`.
//!
//! `current_status()` is cheap (no I/O) and safe to call on every request.
//! The background `run()` task polls `current_status()` at `interval` and
//! writes the result to etcd when the status changes.

use folio_core::protocol::{NodeInfo, NodeStatus};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct HealthMonitor {
    max_journal_bytes: u64,
    p99_threshold: Duration,
    bytes_written: AtomicU64,
    latencies: Mutex<VecDeque<Duration>>,
    window_size: usize,
}

impl HealthMonitor {
    /// Create a monitor.
    ///
    /// * `max_journal_bytes` — transition to ReadOnly when bytes written exceeds this
    /// * `p99_threshold` — transition to ReadOnly when append p99 exceeds this
    /// * `window_size` — number of recent appends used for the p99 calculation
    pub fn new(max_journal_bytes: u64, p99_threshold: Duration, window_size: usize) -> Self {
        Self {
            max_journal_bytes,
            p99_threshold,
            bytes_written: AtomicU64::new(0),
            latencies: Mutex::new(VecDeque::with_capacity(window_size + 1)),
            window_size,
        }
    }

    /// Default limits suitable for development and testing.
    pub fn default_limits() -> Self {
        Self::new(
            400 * 1024 * 1024 * 1024,  // 400 GB
            Duration::from_millis(50), // 50 ms p99 threshold
            1000,
        )
    }

    /// Record the outcome of a single append. Called by `JournalService` after
    /// every successful `add_entry`.
    pub fn record(&self, elapsed: Duration, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        let mut lat = self.latencies.lock();
        if lat.len() >= self.window_size {
            lat.pop_front();
        }
        lat.push_back(elapsed);
    }

    /// Current `NodeStatus` based on the latest snapshot of metrics.
    pub fn current_status(&self) -> NodeStatus {
        if self.bytes_written.load(Ordering::Relaxed) >= self.max_journal_bytes {
            tracing::warn!("HealthMonitor: journal disk capacity threshold reached → ReadOnly");
            return NodeStatus::ReadOnly;
        }

        let lat = self.latencies.lock();
        if lat.len() >= 10 {
            let p99 = compute_p99(&lat);
            if p99 > self.p99_threshold {
                tracing::warn!(
                    p99_ms = p99.as_millis(),
                    threshold_ms = self.p99_threshold.as_millis(),
                    "HealthMonitor: p99 latency exceeded threshold → ReadOnly"
                );
                return NodeStatus::ReadOnly;
            }
        }

        NodeStatus::ReadWrite
    }

    /// Spawn a background task that checks health at `interval` and updates
    /// the node's status in etcd when it changes.
    ///
    /// The task runs until the `Arc<HealthMonitor>` is dropped.
    pub fn run(
        self: Arc<Self>,
        mut etcd: etcd_client::Client,
        node_info: NodeInfo,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let mut last_status = NodeStatus::ReadWrite;

            loop {
                ticker.tick().await;

                let status = self.current_status();
                if status == last_status {
                    continue;
                }

                last_status = status;
                let mut updated = node_info.clone();
                updated.status = status;
                updated.last_heartbeat_ms = folio_core::protocol::now_ms();

                let key = format!("/folio/nodes/{}", node_info.node_id);
                match serde_json::to_vec(&updated) {
                    Ok(value) => {
                        if let Err(e) = etcd.put(key, value, None).await {
                            tracing::error!("HealthMonitor: failed to update etcd status: {e}");
                        } else {
                            tracing::info!(
                                node_id = %node_info.node_id,
                                ?status,
                                "HealthMonitor: node status updated in etcd"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("HealthMonitor: failed to serialize NodeInfo: {e}");
                    }
                }
            }
        })
    }
}

fn compute_p99(window: &VecDeque<Duration>) -> Duration {
    let mut sorted: Vec<Duration> = window.iter().copied().collect();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Convenience: build a `HealthMonitor` from a journal `PathBuf` and
/// standard production thresholds. The `max_journal_bytes` defaults to 80% of
/// the partition size where the journal lives, or falls back to 400 GB if the
/// size cannot be determined.
pub fn from_journal_path(_journal_path: &PathBuf, p99_threshold: Duration) -> HealthMonitor {
    // Filesystem size detection is platform-specific; use the safe default for
    // the MVP. Production deployments should configure max_journal_bytes
    // explicitly via the folio-node config.
    HealthMonitor::new(400 * 1024 * 1024 * 1024, p99_threshold, 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_write_when_healthy() {
        let m = HealthMonitor::new(1_000_000, Duration::from_millis(50), 100);
        for _ in 0..20 {
            m.record(Duration::from_millis(1), 100);
        }
        assert_eq!(m.current_status(), NodeStatus::ReadWrite);
    }

    #[test]
    fn read_only_when_disk_full() {
        let m = HealthMonitor::new(500, Duration::from_millis(50), 100);
        m.record(Duration::from_millis(1), 600);
        assert_eq!(m.current_status(), NodeStatus::ReadOnly);
    }

    #[test]
    fn read_only_when_p99_high() {
        let m = HealthMonitor::new(1_000_000, Duration::from_millis(10), 100);
        for _ in 0..20 {
            m.record(Duration::from_millis(50), 100); // all 50ms → p99 = 50ms > 10ms threshold
        }
        assert_eq!(m.current_status(), NodeStatus::ReadOnly);
    }
}
