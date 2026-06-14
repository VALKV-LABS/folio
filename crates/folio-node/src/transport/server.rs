//! tonic gRPC server — wraps `JournalService` and maps proto ↔ domain types.

use crate::storage::JournalService;
use folio_core::error::FolioError;
use folio_core::proto::journal_service_server::{
    JournalService as JournalServiceTrait, JournalServiceServer,
};
use folio_core::proto::{
    AddEntryRequest, AddEntryResponse, FenceLedgerRequest, FenceLedgerResponse, GetLacRequest,
    GetLacResponse, GetNodeStatusRequest, GetNodeStatusResponse, NodeState, ProtoEntry,
    ReadEntryRequest, ReadEntryResponse,
};
use folio_core::protocol::now_ms;
use folio_core::protocol::{AppendAck, Entry, NodeStatus, ReadResult};
use folio_core::transport::TlsPaths;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

fn to_status(e: FolioError) -> Status {
    match e {
        FolioError::LedgerFenced(id) => {
            Status::failed_precondition(format!("ledger {id} is fenced"))
        }
        FolioError::LedgerNotFound(id) => Status::not_found(format!("ledger {id} not found")),
        FolioError::EntryNotFound {
            ledger_id,
            entry_id,
        } => Status::not_found(format!("entry {entry_id} not found in ledger {ledger_id}")),
        FolioError::QuorumNotMet { required, received } => Status::unavailable(format!(
            "quorum not met: required={required} received={received}"
        )),
        FolioError::InvalidEntryId { expected, entry_id } => {
            Status::invalid_argument(format!("invalid entry_id {entry_id}: expected {expected}"))
        }
        FolioError::CasConflict => Status::aborted("compare-and-swap conflict"),
        FolioError::LacTimeout => Status::deadline_exceeded("LAC wait timed out"),
        FolioError::StateConflict { expected, found } => Status::failed_precondition(format!(
            "state conflict: expected {expected:?} found {found:?}"
        )),
        other => Status::internal(other.to_string()),
    }
}

fn proto_to_entry(p: ProtoEntry) -> Entry {
    let master_key = if p.master_key.len() == 32 {
        let mut k = [0u8; 32];
        k.copy_from_slice(&p.master_key);
        Some(k)
    } else {
        None
    };
    let hmac = if p.hmac.len() == 32 {
        let mut h = [0u8; 32];
        h.copy_from_slice(&p.hmac);
        Some(h)
    } else {
        None
    };
    Entry {
        ledger_id: p.ledger_id,
        entry_id: p.entry_id,
        lac: p.lac,
        digest: p.digest,
        data: p.data,
        master_key,
        hmac,
    }
}

fn entry_to_proto(e: &Entry) -> ProtoEntry {
    ProtoEntry {
        ledger_id: e.ledger_id,
        entry_id: e.entry_id,
        lac: e.lac,
        digest: e.digest,
        data: e.data.clone(),
        master_key: e.master_key.map(|k| k.to_vec()).unwrap_or_default(),
        hmac: e.hmac.map(|h| h.to_vec()).unwrap_or_default(),
    }
}

fn ack_to_resp(a: AppendAck) -> AddEntryResponse {
    AddEntryResponse {
        ledger_id: a.ledger_id,
        entry_id: a.entry_id,
        local_lac: a.local_lac,
        node_id: a.node_id,
    }
}

fn read_to_resp(r: ReadResult) -> ReadEntryResponse {
    ReadEntryResponse {
        entry: Some(entry_to_proto(&r.entry)),
        node_id: r.node_id,
        appended_at_ms: r.appended_at_ms,
    }
}

#[allow(dead_code)]
fn status_to_proto(s: NodeStatus) -> NodeState {
    match s {
        NodeStatus::ReadWrite => NodeState::ReadWrite,
        NodeStatus::ReadOnly => NodeState::ReadOnly,
        NodeStatus::Recovering => NodeState::Recovering,
        NodeStatus::Unavailable => NodeState::Unavailable,
    }
}

pub struct JournalGrpcService {
    inner: Arc<JournalService>,
}

impl JournalGrpcService {
    pub fn new(service: Arc<JournalService>) -> Self {
        Self { inner: service }
    }

    pub async fn serve(
        self,
        addr: SocketAddr,
        tls: Option<TlsPaths>,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let svc = JournalServiceServer::new(self);
        let mut builder = tonic::transport::Server::builder();
        if let Some(paths) = tls {
            let tls_cfg = paths.server_tls_config().await?;
            builder = builder.tls_config(tls_cfg)?;
        }
        builder
            .add_service(svc)
            .serve_with_shutdown(addr, shutdown)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl JournalServiceTrait for JournalGrpcService {
    async fn add_entry(
        &self,
        req: Request<AddEntryRequest>,
    ) -> Result<Response<AddEntryResponse>, Status> {
        let proto = req
            .into_inner()
            .entry
            .ok_or_else(|| Status::invalid_argument("missing entry"))?;
        let ack = self
            .inner
            .add_entry(proto_to_entry(proto))
            .await
            .map_err(to_status)?;
        Ok(Response::new(ack_to_resp(ack)))
    }

    async fn read_entry(
        &self,
        req: Request<ReadEntryRequest>,
    ) -> Result<Response<ReadEntryResponse>, Status> {
        let r = req.into_inner();
        let result = self
            .inner
            .read_entry(r.ledger_id, r.entry_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(read_to_resp(result)))
    }

    async fn fence_ledger(
        &self,
        req: Request<FenceLedgerRequest>,
    ) -> Result<Response<FenceLedgerResponse>, Status> {
        let lac = self
            .inner
            .fence_ledger(req.into_inner().ledger_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(FenceLedgerResponse { lac }))
    }

    async fn get_lac(
        &self,
        req: Request<GetLacRequest>,
    ) -> Result<Response<GetLacResponse>, Status> {
        let r = req.into_inner();
        let timeout = if r.wait_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(r.wait_timeout_ms))
        };
        let lac = self
            .inner
            .get_lac(r.ledger_id, timeout)
            .await
            .map_err(to_status)?;
        Ok(Response::new(GetLacResponse { lac }))
    }

    async fn get_node_status(
        &self,
        _req: Request<GetNodeStatusRequest>,
    ) -> Result<Response<GetNodeStatusResponse>, Status> {
        Ok(Response::new(GetNodeStatusResponse {
            node_id: self.inner.node_id().to_string(),
            state: NodeState::ReadWrite as i32,
            last_heartbeat_ms: now_ms(),
        }))
    }
}
