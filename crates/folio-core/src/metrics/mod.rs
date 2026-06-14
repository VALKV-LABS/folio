//! Prometheus metrics registry for folio-node.
//!
//! Sub-modules define metric families; this module exposes the global registry
//! and a helper to render the /metrics HTTP endpoint response body.

pub mod client;
pub mod recovery;
pub mod storage;

pub use client::CLIENT;
pub use recovery::RECOVERY;
pub use storage::STORAGE;

use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::OnceLock;

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Returns the global Prometheus registry, creating it on first call.
pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let r = Registry::new();
        storage::register(&r);
        client::register(&r);
        recovery::register(&r);
        r
    })
}

/// Render the current metrics as a Prometheus text exposition.
pub fn gather_text() -> Vec<u8> {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let families = registry().gather();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    buf
}
