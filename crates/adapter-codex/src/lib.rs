//! Codex attach-mode delivery engine (docs/adapters/codex/requirements.md).
//!
//! The broker connects to the shared `codex app-server --listen` used by the
//! human-owned interactive session. Idle delivery starts a new turn; active
//! delivery steers the bus input into the running turn. The attach client
//! never answers approvals, and transport ambiguity at either delivery commit
//! point is terminal so the broker cannot double-inject a message.

pub mod attach;
pub use attach::CodexAttach;

/// The outcome of delivering one message through the Codex app-server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivered {
    /// The harness completed a turn that included the message.
    Processed,
    /// Delivery or its containing turn failed. Terminal — do not retry.
    Failed(String),
    /// The app-server was unreachable before any delivery commit point.
    /// Retriable while the recipient's bus session remains present.
    Unreachable(String),
}

/// App-server call failure classification. Transport failures are retriable
/// only before a delivery commit point; RPC errors are definitive answers.
#[derive(Debug, Clone)]
pub enum CallError {
    Transport(String),
    Rpc(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Transport(e) | CallError::Rpc(e) => write!(f, "{e}"),
        }
    }
}
