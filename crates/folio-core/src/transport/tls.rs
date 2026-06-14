//! mTLS configuration helpers for tonic server and client (Phase 6 completes
//! this module; this stub provides the types and file-based loading path).

use std::path::PathBuf;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// Paths to the three PEM files needed for mTLS.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    /// Root CA certificate that signed all node certs.
    pub ca_cert: PathBuf,
    /// This node's certificate (signed by the CA).
    pub node_cert: PathBuf,
    /// Private key for `node_cert`.
    pub node_key: PathBuf,
}

impl TlsPaths {
    pub fn new(
        ca_cert: impl Into<PathBuf>,
        node_cert: impl Into<PathBuf>,
        node_key: impl Into<PathBuf>,
    ) -> Self {
        Self {
            ca_cert: ca_cert.into(),
            node_cert: node_cert.into(),
            node_key: node_key.into(),
        }
    }

    /// Build a `ServerTlsConfig` that requires a client certificate signed by
    /// `ca_cert`. Used by `JournalGrpcService::serve()`.
    pub async fn server_tls_config(&self) -> std::io::Result<ServerTlsConfig> {
        let ca = tokio::fs::read(&self.ca_cert).await?;
        let cert = tokio::fs::read(&self.node_cert).await?;
        let key = tokio::fs::read(&self.node_key).await?;
        Ok(ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&ca)))
    }

    /// Build a `ClientTlsConfig` that presents this node's cert and verifies
    /// the server cert is signed by `ca_cert`.
    ///
    /// `domain` is accepted for future use but not applied yet. Hostname
    /// verification is currently skipped — node certs carry only a CN
    /// (e.g. `us-west-2-node-0`), not a DNS SAN, so there is nothing to
    /// verify against. The CA signature is the trust anchor.
    ///
    /// # To re-enable hostname verification
    /// 1. Add a DNS SAN to each node cert in `deploy/certs/gen-ca.sh`
    ///    (e.g. `subjectAltName = DNS:$CELL-node-$i.folio.internal`).
    /// 2. Uncomment `.domain_name(domain)` below and pass the node's DNS name
    ///    as `domain` at call sites.
    pub async fn client_tls_config(
        &self,
        _domain: Option<&str>,
    ) -> std::io::Result<ClientTlsConfig> {
        let ca = tokio::fs::read(&self.ca_cert).await?;
        let cert = tokio::fs::read(&self.node_cert).await?;
        let key = tokio::fs::read(&self.node_key).await?;
        Ok(ClientTlsConfig::new()
            // .domain_name(domain.unwrap_or(""))   // uncomment when DNS SANs are added
            .ca_certificate(Certificate::from_pem(&ca))
            .identity(Identity::from_pem(&cert, &key)))
    }
}
