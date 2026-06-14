//! tonic gRPC client — implements `StorageNodeClient` over a real gRPC channel.

use async_trait::async_trait;
use folio_core::error::{FolioError, Result};
use folio_core::proto::journal_service_client::JournalServiceClient;
use folio_core::proto::{
    AddEntryRequest, FenceLedgerRequest, GetLacRequest, ProtoEntry, ReadEntryRequest,
};
use folio_core::protocol::{AppendAck, Entry, ReadResult};
use folio_core::resolver::StorageNodeClient;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

fn grpc_err(e: tonic::Status) -> FolioError {
    match e.code() {
        tonic::Code::FailedPrecondition => parse_fenced(e.message()),
        tonic::Code::NotFound => parse_not_found(e.message()),
        tonic::Code::DeadlineExceeded => FolioError::LacTimeout,
        tonic::Code::Aborted => FolioError::CasConflict,
        _ => FolioError::Storage(format!("gRPC {}: {}", e.code(), e.message())),
    }
}

/// "ledger {id} is fenced" → LedgerFenced(id), otherwise Storage(msg).
fn parse_fenced(msg: &str) -> FolioError {
    if let Some(rest) = msg.strip_prefix("ledger ")
        && let Some((id_str, _)) = rest.split_once(" is fenced")
        && let Ok(id) = id_str.parse::<u64>()
    {
        return FolioError::LedgerFenced(id);
    }
    FolioError::Storage(format!("precondition: {msg}"))
}

/// "entry {entry_id} not found in ledger {ledger_id}" → EntryNotFound.
/// Anything else (e.g. LedgerNotFound) falls back to Storage.
fn parse_not_found(msg: &str) -> FolioError {
    if let Some(rest) = msg.strip_prefix("entry ")
        && let Some((eid_str, tail)) = rest.split_once(" not found in ledger ")
        && let (Ok(entry_id), Ok(ledger_id)) = (eid_str.parse::<u64>(), tail.parse::<u64>())
    {
        return FolioError::EntryNotFound {
            ledger_id,
            entry_id,
        };
    }
    FolioError::Storage(format!("not found: {msg}"))
}

fn to_proto(e: &Entry) -> ProtoEntry {
    ProtoEntry {
        ledger_id: e.ledger_id,
        entry_id: e.entry_id,
        lac: e.lac,
        digest: e.digest,
        data: e.data.clone(),
        master_key: e
            .master_key
            .as_ref()
            .map(|k| k.to_vec())
            .unwrap_or_default(),
        hmac: e.hmac.as_ref().map(|h| h.to_vec()).unwrap_or_default(),
    }
}

fn from_proto_entry(pe: ProtoEntry) -> Entry {
    let master_key = if pe.master_key.len() == 32 {
        let mut k = [0u8; 32];
        k.copy_from_slice(&pe.master_key);
        Some(k)
    } else {
        None
    };
    let hmac = if pe.hmac.len() == 32 {
        let mut h = [0u8; 32];
        h.copy_from_slice(&pe.hmac);
        Some(h)
    } else {
        None
    };
    Entry {
        ledger_id: pe.ledger_id,
        entry_id: pe.entry_id,
        lac: pe.lac,
        digest: pe.digest,
        data: pe.data,
        master_key,
        hmac,
    }
}

#[derive(Clone)]
pub struct JournalGrpcClient {
    inner: JournalServiceClient<Channel>,
    _node_id: String,
}

impl JournalGrpcClient {
    pub fn new(channel: Channel, node_id: impl Into<String>) -> Self {
        Self {
            inner: JournalServiceClient::new(channel),
            _node_id: node_id.into(),
        }
    }

    pub async fn connect(addr: impl Into<String>, node_id: impl Into<String>) -> Result<Self> {
        let input_addr = addr.into();
        let addr = normalize_grpc_addr(&input_addr);
        let err_addr = addr.clone();
        let channel = Endpoint::from_shared(addr.clone())
            .map_err(|e| FolioError::Storage(format!("invalid address [{err_addr}]: {e}")))?
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .keep_alive_while_idle(true)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| {
                FolioError::Storage(format!(
                    "connection failed [{}] (input [{}]): {e}",
                    err_addr, input_addr
                ))
            })?;
        Ok(Self::new(channel, node_id))
    }
}

fn normalize_grpc_addr(addr: &str) -> String {
    let addr = addr.trim();
    if let Some(rest) = addr.strip_prefix("http://localhost") {
        return format!("http://127.0.0.1{rest}");
    }
    if let Some(rest) = addr.strip_prefix("https://localhost") {
        return format!("https://127.0.0.1{rest}");
    }
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

#[async_trait]
impl StorageNodeClient for JournalGrpcClient {
    async fn add_entry(&self, entry: Entry) -> Result<AppendAck> {
        let resp = self
            .inner
            .clone()
            .add_entry(AddEntryRequest {
                entry: Some(to_proto(&entry)),
            })
            .await
            .map_err(grpc_err)?;
        let r = resp.into_inner();
        Ok(AppendAck {
            ledger_id: r.ledger_id,
            entry_id: r.entry_id,
            local_lac: r.local_lac,
            node_id: r.node_id,
        })
    }

    async fn fence_ledger(&self, ledger_id: u64) -> Result<u64> {
        let resp = self
            .inner
            .clone()
            .fence_ledger(FenceLedgerRequest { ledger_id })
            .await
            .map_err(grpc_err)?;
        Ok(resp.into_inner().lac)
    }

    async fn read_entry(&self, ledger_id: u64, entry_id: u64) -> Result<ReadResult> {
        let resp = self
            .inner
            .clone()
            .read_entry(ReadEntryRequest {
                ledger_id,
                entry_id,
            })
            .await
            .map_err(grpc_err)?;
        let r = resp.into_inner();
        let pe = r
            .entry
            .ok_or_else(|| FolioError::Storage("empty read_entry response".into()))?;
        Ok(ReadResult {
            entry: from_proto_entry(pe),
            node_id: r.node_id,
            appended_at_ms: r.appended_at_ms,
        })
    }

    async fn get_lac(&self, ledger_id: u64, wait_timeout: Option<Duration>) -> Result<u64> {
        let wait_timeout_ms = wait_timeout.map(|d| d.as_millis() as u64).unwrap_or(0);
        let resp = self
            .inner
            .clone()
            .get_lac(GetLacRequest {
                ledger_id,
                wait_timeout_ms,
            })
            .await
            .map_err(grpc_err)?;
        Ok(resp.into_inner().lac)
    }
}
