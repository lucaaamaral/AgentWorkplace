//! Attach-mode delivery: the human owns the thread, the adapter attaches.
//!
//! The human runs a shared `codex app-server --listen ws://…` and their Codex
//! client creates the thread — so *they* receive the full event stream
//! (native monitoring). The adapter connects as a second WebSocket client and
//! injects deliveries with `turn/start` by thread id. Because turn events
//! route to the thread owner (spike-confirmed), the adapter observes
//! `processed` by polling `thread/read {includeTurns}` for its turn's status.
//!
//! Approvals are notify-only (ADR-0012 / CX-11): the adapter never answers a
//! server-initiated approval request. The human's attached client owns them;
//! an unanswered approval stalls that turn until the human handles it in
//! their own UI.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::{CallError, Delivered};

const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Error text positively identifying a thread that unloaded after idle
/// (CX-8) and can be re-loaded with thread/resume. Deliberately narrow:
/// resuming a *loaded* thread clears its goal state (findings.md), so an
/// unidentified error must never trigger a resume.
fn unload_hint(e: &str) -> bool {
    let e = e.to_ascii_lowercase();
    e.contains("not loaded") || e.contains("unloaded") || e.contains("unknown thread")
}

/// Error text meaning the thread no longer exists at all ("no rollout found",
/// findings.md) — maps to session-disconnected, terminal.
fn thread_gone(e: &str) -> bool {
    e.to_ascii_lowercase().contains("no rollout")
}
const TURN_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(1500);
/// Bound on a single inbound app-server frame/message — a transport memory
/// cap, not a body-size policy (that is the broker's message_size_limit).
const MAX_WS_MESSAGE: usize = 16 * 1024 * 1024;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, CallError>>>>>;

/// A non-owner client attached to a shared `codex app-server --listen`.
pub struct CodexAttach {
    out: mpsc::UnboundedSender<WsMessage>,
    next_id: AtomicU64,
    pending: Pending,
    /// Serialization is per *thread* (CX-2): one delivery at a time into a
    /// given thread, while unrelated threads on the same app-server proceed
    /// concurrently.
    turn_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl CodexAttach {
    /// Connect and initialize against `ws://host:port`. `token` is the
    /// app-server capability token (`--ws-auth capability-token`), presented
    /// as `Authorization: Bearer` on the WebSocket upgrade — never in the URL
    /// or argv.
    pub async fn connect(url: &str, token: Option<&str>) -> anyhow::Result<CodexAttach> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let ws_cfg = WebSocketConfig::default()
            .max_message_size(Some(MAX_WS_MESSAGE))
            .max_frame_size(Some(MAX_WS_MESSAGE));
        let mut request = url.into_client_request()?;
        if let Some(token) = token {
            let mut value = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&format!(
                "Bearer {token}"
            ))?;
            value.set_sensitive(true);
            request.headers_mut().insert(
                tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
                value,
            );
        }
        let (ws, _) =
            tokio_tungstenite::connect_async_with_config(request, Some(ws_cfg), false).await?;
        let (mut sink, mut stream) = ws.split();

        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<WsMessage>();
        tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let reader_pending = pending.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = stream.next().await {
                let WsMessage::Text(text) = msg else { continue };
                let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                let is_response = v.get("id").is_some()
                    && (v.get("result").is_some() || v.get("error").is_some());
                if is_response {
                    if let Some(id) = v.get("id").and_then(Value::as_u64)
                        && let Some(tx) = reader_pending.lock().unwrap().remove(&id)
                    {
                        let _ = tx.send(match v.get("error") {
                            Some(e) => Err(CallError::Rpc(
                                e.get("message")
                                    .and_then(Value::as_str)
                                    .unwrap_or("app-server error")
                                    .to_string(),
                            )),
                            None => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
                        });
                    }
                } else if v.get("method").is_some() && v.get("id").is_some() {
                    // Server-initiated request (approvals): never answered
                    // (ADR-0012) — the human's client owns approvals. Only
                    // surface that one is pending.
                    let method = v.get("method").and_then(Value::as_str).unwrap_or("");
                    tracing::warn!(
                        method,
                        "app-server approval request left for the owning client (notify-only)"
                    );
                }
                // Notifications: owner-routed; nothing for a non-owner here.
            }
            // Connection gone: fail every in-flight call immediately so
            // deliveries report unreachable instead of waiting out timeouts.
            let mut p = reader_pending.lock().unwrap();
            for (_, tx) in p.drain() {
                let _ = tx.send(Err(CallError::Transport(
                    "app-server connection closed".into(),
                )));
            }
        });

        let attach = CodexAttach {
            out: out_tx,
            next_id: AtomicU64::new(1),
            pending,
            turn_locks: Mutex::new(HashMap::new()),
        };
        attach
            .call("initialize", json!({ "clientInfo": { "name": "workplace", "version": env!("CARGO_PKG_VERSION") } }))
            .await
            .map_err(|e| anyhow::anyhow!("initialize failed: {e}"))?;
        Ok(attach)
    }

    fn thread_lock(&self, thread_id: &str) -> Arc<AsyncMutex<()>> {
        self.turn_locks
            .lock()
            .unwrap()
            .entry(thread_id.to_string())
            .or_default()
            .clone()
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, CallError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        if self
            .out
            .send(WsMessage::Text(req.to_string().into()))
            .is_err()
        {
            self.pending.lock().unwrap().remove(&id);
            return Err(CallError::Transport("app-server connection closed".into()));
        }
        match tokio::time::timeout(CALL_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(CallError::Transport("app-server connection closed".into())),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(CallError::Transport("app-server call timed out".into()))
            }
        }
    }

    /// Create a thread on this connection (tests / owned setups; in the
    /// attach model the human's client owns the thread).
    pub async fn start_thread(&self, cwd: &str) -> anyhow::Result<String> {
        let v = self
            .call("thread/start", json!({ "cwd": cwd }))
            .await
            .map_err(|e| anyhow::anyhow!("thread/start failed: {e}"))?;
        v.pointer("/thread/id")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| anyhow::anyhow!("thread/start returned no thread id"))
    }

    /// `thread/resume` with `threadId` only: partial config overrides would
    /// silently change the agent's model/sandbox (findings.md). Used to
    /// reattach a thread that unloaded after idle (CX-8).
    async fn resume_thread(&self, thread_id: &str) -> Result<(), CallError> {
        self.call("thread/resume", json!({ "threadId": thread_id }))
            .await
            .map(|_| ())
    }

    /// Read the thread's idle/active status. `thread/read` errors before the
    /// thread's first user message ("not materialized yet") — treated as idle.
    async fn thread_idle(&self, thread_id: &str) -> Result<bool, CallError> {
        match self
            .call(
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": false }),
            )
            .await
        {
            Ok(v) => Ok(v
                .pointer("/thread/status/type")
                .and_then(Value::as_str)
                .map(|t| t == "idle")
                .unwrap_or(true)),
            Err(CallError::Rpc(e)) if e.contains("not materialized") => Ok(true),
            Err(e) => Err(e),
        }
    }

    async fn turn_status(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<String>, CallError> {
        match self
            .call(
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": true }),
            )
            .await
        {
            Ok(v) => Ok(v
                .pointer("/thread/turns")
                .and_then(Value::as_array)
                .and_then(|turns| {
                    turns
                        .iter()
                        .find(|t| t.get("id").and_then(Value::as_str) == Some(turn_id))
                        .and_then(|t| t.get("status").and_then(Value::as_str))
                        .map(String::from)
                })),
            Err(CallError::Rpc(e)) if e.contains("not materialized") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Deliver one rendered message into the (human-owned) thread. Serialized
    /// on idle per thread (turn/start while busy is silently dropped, CX-2);
    /// `processed` is observed by polling this turn's status. An unloaded
    /// thread is resumed transparently (CX-8) — once — before giving up.
    pub async fn deliver(&self, thread_id: &str, text: &str) -> Delivered {
        let lock = self.thread_lock(thread_id);
        let _guard = lock.lock().await;

        // Wait for idle: the human may be mid-turn — their session, their
        // priority. The broker holds; we poll.
        let mut resumed = false;
        let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
        loop {
            match self.thread_idle(thread_id).await {
                Ok(true) => break,
                Ok(false) => {
                    if tokio::time::Instant::now() > deadline {
                        return Delivered::Failed("thread stayed busy past timeout".into());
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                Err(CallError::Transport(e)) => return Delivered::Unreachable(e),
                Err(CallError::Rpc(e)) => {
                    // Resume ONLY on a positively identified unload: resuming
                    // a loaded thread clears its goal state (findings.md), so
                    // an unrelated RPC error must fail, not resume. "no
                    // rollout" means the thread is gone entirely — terminal.
                    if thread_gone(&e) {
                        return Delivered::Failed(format!("thread gone: {e}"));
                    }
                    if resumed || !unload_hint(&e) {
                        return Delivered::Failed(format!("thread/read failed: {e}"));
                    }
                    resumed = true;
                    match self.resume_thread(thread_id).await {
                        Ok(()) => continue,
                        Err(CallError::Transport(t)) => return Delivered::Unreachable(t),
                        Err(CallError::Rpc(r)) => {
                            return Delivered::Failed(format!("thread gone: {r}"));
                        }
                    }
                }
            }
        }

        // COMMIT POINT: once turn/start has been sent, a lost response no
        // longer proves the turn did not run — so any TRANSPORT failure from
        // here on is terminal ("completion unknown"), never Unreachable/
        // retriable. An RPC *error response* is different: the server
        // answered, the turn definitively did not start, so the unload-resume
        // retry stays safe.
        let turn_id = loop {
            match self
                .call(
                    "turn/start",
                    json!({ "threadId": thread_id, "input": [{ "type": "text", "text": text }] }),
                )
                .await
            {
                Ok(v) => match v.pointer("/turn/id").and_then(Value::as_str) {
                    Some(id) => break id.to_string(),
                    None => return Delivered::Failed("turn/start returned no turn id".into()),
                },
                Err(CallError::Transport(e)) => {
                    return Delivered::Failed(format!(
                        "transport failure during turn/start; completion unknown: {e}"
                    ));
                }
                Err(CallError::Rpc(e)) => {
                    if thread_gone(&e) {
                        return Delivered::Failed(format!("thread gone: {e}"));
                    }
                    if resumed || !unload_hint(&e) {
                        return Delivered::Failed(e);
                    }
                    resumed = true;
                    match self.resume_thread(thread_id).await {
                        Ok(()) => continue,
                        Err(CallError::Transport(t)) => {
                            return Delivered::Failed(format!(
                                "transport failure during resume after turn/start attempt; \
                                 completion unknown: {t}"
                            ));
                        }
                        Err(CallError::Rpc(r)) => {
                            return Delivered::Failed(format!("thread gone: {r}"));
                        }
                    }
                }
            }
        };

        // Poll for this turn's completion (the processed ack). A transport
        // loss here is NOT retriable: the turn already started, so retrying
        // the delivery would inject the message twice — report failed with
        // the completion unknown instead.
        let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            match self.turn_status(thread_id, &turn_id).await {
                Ok(Some(status)) if status == "completed" => return Delivered::Processed,
                Ok(Some(status)) if status == "failed" || status == "error" => {
                    return Delivered::Failed(format!("turn ended with status {status}"));
                }
                Ok(_) => {}
                Err(CallError::Transport(e)) => {
                    return Delivered::Failed(format!(
                        "app-server connection lost after turn/start; completion unknown: {e}"
                    ));
                }
                Err(CallError::Rpc(e)) => return Delivered::Failed(format!("poll failed: {e}")),
            }
            if tokio::time::Instant::now() > deadline {
                return Delivered::Failed("turn did not complete within timeout".into());
            }
        }
    }
}
