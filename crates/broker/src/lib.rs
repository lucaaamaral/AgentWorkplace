//! The AgentWorkplace broker daemon library.
//!
//! Owns the store (ADR-0005, ADR-0017), the channel/principal state, and the
//! RPC surface (docs/architecture/rpc-surface.md), served as newline-
//! delimited JSON-RPC 2.0 over TCP (ADR-0014, ADR-0016).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub mod core;
pub mod server;
pub mod store;

pub use core::Broker;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub listens: Vec<SocketAddr>,
    /// None = in-memory store (tests only).
    pub db_path: Option<PathBuf>,
    pub message_size_limit: usize,
    /// Re-attach grace window after broker start before held deliveries to
    /// absent recipients are failed.
    pub grace_window: Duration,
    /// Version string surfaced in session/hello and daemon/status (sourced
    /// from the top-level VERSION file by the binary).
    pub version: String,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            listens: vec![SocketAddr::from(([127, 0, 0, 1], protocol::DEFAULT_PORT))],
            db_path: None,
            message_size_limit: 8 * 1024 * 1024,
            grace_window: Duration::from_secs(60),
            version: include_str!("../../../VERSION").trim().into(),
        }
    }
}

/// Build a broker and serve it on the configured binds until shutdown.
pub async fn run(cfg: BrokerConfig) -> anyhow::Result<()> {
    let broker = Broker::new(cfg)?;
    server::run(broker).await
}
