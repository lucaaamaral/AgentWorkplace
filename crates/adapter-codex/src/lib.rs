//! Codex CLI delivery engine (docs/adapters/codex/requirements.md).
//!
//! The broker is a *client* of each agent's `codex app-server` (ADR-0003).
//! Delivery is `turn/start` on the agent's thread, **serialized on idle**:
//! `turn/start` while a turn is in progress is accepted but never runs
//! (spike-confirmed), so the engine waits for the thread to be idle before
//! starting the next turn. A `turn/completed` for the delivered turn is the
//! `processed` ack — real on Codex, unlike Claude. `turn/steer` is not used
//! for delivery (findings.md). `thread/resume` reattaches a thread that has
//! unloaded after idle (CX-8) before the next `turn/start`.
//!
//! This module drives one `codex app-server` process over newline-delimited
//! JSON-RPC 2.0 on stdio and exposes a serialized `deliver` call.
//!
//! Approvals are notify-only (ADR-0012 / CX-11): server-initiated approval
//! requests are never answered here — they belong to the human's own client.

pub mod attach;
pub use attach::CodexAttach;

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use protocol::{Id, Message, Request, RpcError};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex as AsyncMutex, oneshot};

const TURN_TIMEOUT: Duration = Duration::from_secs(300);

/// The outcome of delivering one message as a turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivered {
    /// `turn/start` accepted and the turn ran to `turn/completed`.
    Processed,
    /// `turn/start` errored or the thread is gone. Terminal — do not retry.
    Failed(String),
    /// The app-server is unreachable (connect/transport failure) and the
    /// message was NOT injected. Retriable: the broker holds and re-attempts
    /// while the recipient's session is still present (CX-5).
    Unreachable(String),
}

/// Call failure classification: transport errors are retriable (the message
/// never reached the app-server); RPC errors are protocol answers.
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

type SharedStdin = Arc<AsyncMutex<tokio::process::ChildStdin>>;
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>>;

/// One `codex app-server` process and its single thread.
pub struct CodexSession {
    thread_id: String,
    next_id: AtomicU64,
    stdin: SharedStdin,
    pending: Pending,
    /// Current thread status (idle/active) and completed turn ids, updated by
    /// the reader task.
    state: Arc<SessionState>,
    /// Serializes delivery: only one turn is started at a time, and only when
    /// the thread is idle.
    turn_lock: AsyncMutex<()>,
    _child: Child,
}

#[derive(Default)]
struct SessionState {
    idle: Mutex<bool>,
    idle_notify: tokio::sync::Notify,
    completed: Mutex<Vec<String>>,
    completed_notify: tokio::sync::Notify,
}

impl CodexSession {
    /// Spawn `codex app-server`, initialize, and start a thread in `cwd`.
    pub async fn spawn(cwd: &str) -> anyhow::Result<CodexSession> {
        let mut child = Command::new("codex")
            .arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdin: SharedStdin = Arc::new(AsyncMutex::new(child.stdin.take().expect("stdin")));
        let stdout = child.stdout.take().expect("stdout");

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let state = Arc::new(SessionState {
            idle: Mutex::new(true),
            ..Default::default()
        });

        // Reader task: correlate responses, track status and completed turns.
        let reader_pending = pending.clone();
        let reader_state = state.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let is_response = v.get("id").is_some()
                    && (v.get("result").is_some() || v.get("error").is_some());
                if is_response {
                    if let Some(id) = v.get("id").and_then(Value::as_u64)
                        && let Some(tx) = reader_pending.lock().unwrap().remove(&id)
                    {
                        let _ = tx.send(match v.get("error") {
                            Some(e) => Err(serde_json::from_value(e.clone()).unwrap_or(RpcError {
                                code: -1,
                                message: e.to_string(),
                                data: None,
                            })),
                            None => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
                        });
                    }
                    continue;
                }
                let Some(method) = v.get("method").and_then(Value::as_str) else {
                    continue;
                };
                // Server-initiated request (approvals): never answered
                // (ADR-0012) — approvals belong to the human's own client.
                // In this single-client mode there is no human attached, so
                // the turn stalls until its timeout; that is the documented
                // consequence, never a silent denial.
                if v.get("id").filter(|i| !i.is_null()).is_some() {
                    tracing::warn!(
                        method,
                        "app-server approval request left unanswered (notify-only, ADR-0012)"
                    );
                    continue;
                }
                // Notifications: track thread status and completed turns.
                match method {
                    "thread/status/changed" => {
                        let idle = v
                            .pointer("/params/status/type")
                            .and_then(Value::as_str)
                            .map(|t| t == "idle")
                            .unwrap_or(false);
                        *reader_state.idle.lock().unwrap() = idle;
                        if idle {
                            reader_state.idle_notify.notify_waiters();
                        }
                    }
                    "turn/completed" => {
                        if let Some(id) = v.pointer("/params/turn/id").and_then(Value::as_str) {
                            reader_state.completed.lock().unwrap().push(id.to_string());
                            reader_state.completed_notify.notify_waiters();
                        }
                        *reader_state.idle.lock().unwrap() = true;
                        reader_state.idle_notify.notify_waiters();
                    }
                    _ => {}
                }
            }
        });

        let mut session = CodexSession {
            thread_id: String::new(),
            next_id: AtomicU64::new(1),
            stdin,
            pending,
            state,
            turn_lock: AsyncMutex::new(()),
            _child: child,
        };

        session.call("initialize", json!({ "clientInfo": { "name": "workplace", "version": env!("CARGO_PKG_VERSION") } })).await
            .map_err(|e| anyhow::anyhow!("initialize failed: {}", e.message))?;
        let started = session
            .call("thread/start", json!({ "cwd": cwd }))
            .await
            .map_err(|e| anyhow::anyhow!("thread/start failed: {}", e.message))?;
        session.thread_id = started
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("thread/start returned no thread id"))?
            .to_string();
        Ok(session)
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let req = Message::Request(Request {
            jsonrpc: "2.0".into(),
            id: Id::Num(id),
            method: method.into(),
            params: Some(params),
        });
        {
            let mut guard = self.stdin.lock().await;
            if guard
                .write_all(format!("{}\n", req.to_line()).as_bytes())
                .await
                .is_err()
            {
                return Err(RpcError {
                    code: -1,
                    message: "app-server stdin closed".into(),
                    data: None,
                });
            }
            let _ = guard.flush().await;
        }
        match tokio::time::timeout(TURN_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(RpcError {
                code: -1,
                message: "app-server closed".into(),
                data: None,
            }),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(RpcError {
                    code: -1,
                    message: "app-server call timed out".into(),
                    data: None,
                })
            }
        }
    }

    async fn wait_idle(&self) {
        loop {
            if *self.state.idle.lock().unwrap() {
                return;
            }
            self.state.idle_notify.notified().await;
        }
    }

    /// Deliver one rendered message as a turn, serialized on idle. Blocks
    /// until the turn completes (the `processed` ack) or fails.
    pub async fn deliver(&self, text: &str) -> Delivered {
        // Serialize: one delivery at a time, and only when the thread is idle
        // (turn/start while busy is silently dropped — CX-2).
        let _guard = self.turn_lock.lock().await;
        self.wait_idle().await;

        let before = self.state.completed.lock().unwrap().len();
        let result = self
            .call(
                "turn/start",
                json!({ "threadId": self.thread_id, "input": [{ "type": "text", "text": text }] }),
            )
            .await;
        let turn_id = match result {
            Ok(v) => match v.pointer("/turn/id").and_then(Value::as_str) {
                Some(id) => id.to_string(),
                None => return Delivered::Failed("turn/start returned no turn id".into()),
            },
            Err(e) => return Delivered::Failed(e.message),
        };
        *self.state.idle.lock().unwrap() = false;

        // Wait for this turn's completion (the processed ack).
        loop {
            if self
                .state
                .completed
                .lock()
                .unwrap()
                .get(before..)
                .map(|s| s.contains(&turn_id))
                .unwrap_or(false)
            {
                return Delivered::Processed;
            }
            tokio::select! {
                _ = self.state.completed_notify.notified() => {}
                _ = tokio::time::sleep(TURN_TIMEOUT) => {
                    return Delivered::Failed("turn did not complete".into());
                }
            }
        }
    }
}
