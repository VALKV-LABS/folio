//! Scrubber: periodic CRC32 integrity verification across all replicas (FR-RS-06).
//!
//! On each pass, the Scrubber samples every non-deleted ledger, reads every entry
//! from each ensemble member, and compares the `digest` field that was set at write
//! time via `Entry::new` (crc32fast). Mismatches are logged as errors; the Scrubber
//! does not attempt self-healing — that is the Worker's responsibility.

use folio_core::error::Result;
use folio_core::metadata::MetadataStore;
use folio_core::metrics::RECOVERY;
use folio_core::protocol::LedgerState;
use folio_core::resolver::NodeResolver;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub struct Scrubber<M: MetadataStore> {
    metadata: Arc<M>,
    nodes: NodeResolver,
    interval: Duration,
}

impl<M: MetadataStore> Scrubber<M> {
    pub fn new(metadata: Arc<M>, nodes: NodeResolver, interval: Duration) -> Self {
        Self {
            metadata,
            nodes,
            interval,
        }
    }

    /// Runs forever: sleeps for `interval`, then performs one full scrub pass.
    pub async fn run(&self) -> Result<()> {
        loop {
            sleep(self.interval).await;
            if let Err(e) = self.scrub_pass().await {
                tracing::error!("scrubber: pass failed: {e}");
            }
        }
    }

    async fn scrub_pass(&self) -> Result<()> {
        let ledgers = self.metadata.list_ledgers().await?;
        let mut crc_errors: u64 = 0;

        for meta in &ledgers {
            if meta.state == LedgerState::Deleted {
                continue;
            }
            let last_entry = match meta.last_entry_id {
                Some(id) => id,
                None => continue,
            };
            let Some(fragment) = meta.fragments.last() else {
                continue;
            };

            for entry_id in 0..=last_entry {
                let mut replica_digests: Vec<(String, u32)> = Vec::new();

                for node_id in &fragment.ensemble {
                    let client = match self.nodes.resolve(node_id) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!(
                                "scrubber: cannot resolve node {node_id} for ledger={} entry={entry_id}: {e}",
                                meta.id
                            );
                            continue;
                        }
                    };
                    match client.read_entry(meta.id, entry_id).await {
                        Ok(result) => replica_digests.push((node_id.clone(), result.entry.digest)),
                        Err(e) => tracing::warn!(
                            "scrubber: read error ledger={} entry={entry_id} node={node_id}: {e}",
                            meta.id
                        ),
                    }
                }

                if replica_digests.len() < 2 {
                    continue;
                }
                let (_, expected) = &replica_digests[0];
                for (node_id, digest) in &replica_digests[1..] {
                    if digest != expected {
                        crc_errors += 1;
                        if let Some(m) = RECOVERY.get() {
                            m.scrub_crc_errors_total.inc();
                        }
                        tracing::error!(
                            "scrubber: CRC32 MISMATCH ledger={} entry={} node={} \
                             got={:#010x} expected={:#010x}",
                            meta.id,
                            entry_id,
                            node_id,
                            digest,
                            expected,
                        );
                    }
                }
            }
        }

        if crc_errors > 0 {
            tracing::error!("scrubber: pass complete — {crc_errors} CRC error(s) detected");
        } else {
            tracing::debug!("scrubber: pass complete — no errors");
        }
        Ok(())
    }
}
