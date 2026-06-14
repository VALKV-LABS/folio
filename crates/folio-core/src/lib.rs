pub mod error;
pub mod metadata;
pub mod metrics;
pub mod protocol;
pub mod resolver;
pub mod transport;

/// Generated tonic gRPC stubs for the JournalService (bookie data-plane).
pub mod proto {
    tonic::include_proto!("journal");
}

/// Generated tonic gRPC stubs for the TableService (range-server data-plane).
pub mod table_proto {
    tonic::include_proto!("folio.table.v1");
}
