//! Speculative (hedged) read execution — FR-CS-10.

use folio_core::error::{FolioError, Result};
use folio_core::protocol::ReadResult;
use folio_core::resolver::StorageNodeClient;
use futures::future::select_ok;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

type ReadFut = Pin<Box<dyn Future<Output = Result<ReadResult>> + Send>>;

pub async fn speculative_read(
    clients: &[Arc<dyn StorageNodeClient>],
    ledger_id: u64,
    entry_id: u64,
    hedge_delay: Duration,
) -> Result<ReadResult> {
    match clients {
        [] => {
            return Err(FolioError::EntryNotFound {
                ledger_id,
                entry_id,
            });
        }
        [single] => return single.read_entry(ledger_id, entry_id).await,
        _ => {}
    }

    if hedge_delay.is_zero() {
        let mut last = Err(FolioError::EntryNotFound {
            ledger_id,
            entry_id,
        });
        for client in clients {
            last = client.read_entry(ledger_id, entry_id).await;
            if last.is_ok() {
                return last;
            }
        }
        return last;
    }

    let futures: Vec<ReadFut> = clients
        .iter()
        .enumerate()
        .map(|(i, client)| -> ReadFut {
            let client = client.clone();
            let delay = hedge_delay * i as u32;
            Box::pin(async move {
                if i > 0 {
                    tokio::time::sleep(delay).await;
                }
                client.read_entry(ledger_id, entry_id).await
            })
        })
        .collect();

    select_ok(futures).await.map(|(result, _)| result)
}
