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

/// Whether a `thread/resume` result means this connection *may* now hold the
/// implicit subscription, so it must attempt a best-effort release. `Ok` did
/// subscribe; a `Transport` error is ambiguous — a timed-out response may have
/// subscribed on a still-live socket (`call` does not close the connection on
/// timeout) — so treat it as "maybe". A definitive RPC error means the server
/// rejected the resume: nothing was subscribed and nothing to release.
fn resume_may_have_subscribed(result: &Result<(), CallError>) -> bool {
    !matches!(result, Err(CallError::Rpc(_)))
}

const TURN_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(1500);
/// Bound on a single inbound app-server frame/message — a transport memory
/// cap, not a body-size policy (that is the broker's message_size_limit).
const MAX_WS_MESSAGE: usize = 16 * 1024 * 1024;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, CallError>>>>>;

enum ThreadState {
    Idle,
    /// Active/system-busy thread; the id is present when thread/read exposes
    /// an in-progress turn that can be used as turn/steer's precondition.
    Busy(Option<String>),
}

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

    /// `thread/unsubscribe` this connection from a thread, releasing the
    /// listener that `thread/resume` implicitly added (findings.md). Without
    /// it, a persistent attach pins every thread it resumes loaded forever,
    /// blocking Codex's no-subscriber idle-unload — a permanent bus-presence
    /// ghost. Best-effort at the call sites: never authoritative over a
    /// committed delivery outcome.
    async fn unsubscribe_thread(&self, thread_id: &str) -> Result<(), CallError> {
        self.call("thread/unsubscribe", json!({ "threadId": thread_id }))
            .await
            .map(|_| ())
    }

    /// Read thread status and, while active, the in-progress turn id required
    /// by turn/steer. `thread/read` errors before the first user message
    /// ("not materialized yet") — treated as idle.
    async fn thread_state(&self, thread_id: &str) -> Result<ThreadState, CallError> {
        match self
            .call(
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": true }),
            )
            .await
        {
            Ok(v) => {
                let status = v
                    .pointer("/thread/status/type")
                    .and_then(Value::as_str)
                    .unwrap_or("idle");
                if status == "idle" {
                    return Ok(ThreadState::Idle);
                }
                let active_turn = v
                    .pointer("/thread/turns")
                    .and_then(Value::as_array)
                    .and_then(|turns| {
                        turns.iter().rev().find_map(|turn| {
                            (turn.get("status").and_then(Value::as_str) == Some("inProgress"))
                                .then(|| turn.get("id").and_then(Value::as_str).map(String::from))
                                .flatten()
                        })
                    });
                Ok(ThreadState::Busy(active_turn))
            }
            Err(CallError::Rpc(e)) if e.contains("not materialized") => Ok(ThreadState::Idle),
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

    /// Deliver one rendered message into the human-owned thread. Idle threads
    /// get a fresh turn/start; active steerable turns get turn/steer so a bus
    /// message reaches Codex immediately. A steer RPC rejection is definitive
    /// non-delivery and safely falls back to turn/start once idle. Processing
    /// is observed by polling the turn that included the message.
    pub async fn deliver(&self, thread_id: &str, text: &str) -> Delivered {
        let lock = self.thread_lock(thread_id);
        let _guard = lock.lock().await;

        // `thread/resume` implicitly subscribes THIS connection to the thread
        // (findings.md). A persistent attach that resumes and never releases
        // pins the thread loaded forever, blocking Codex's no-subscriber idle
        // unload — a permanent bus-presence ghost. Track whether a resume here
        // may have taken that subscription so we can drop it below.
        let mut maybe_subscribed = false;
        let committed = self
            .deliver_inner(thread_id, text, &mut maybe_subscribed)
            .await;

        // Release the resume subscription immediately after the commit point,
        // before completion polling: the just-started turn keeps the thread
        // active (so it cannot idle-unload mid-delivery anyway) and thread/read
        // polling is subscription-independent, so this minimizes the pin
        // window. Only when a resume here may have taken the subscription —
        // turn/start on a loaded thread does not subscribe, and a thread/start
        // subscription belongs to whoever made it; an ambiguous transport
        // timeout counts, because the socket may still be live and subscribed.
        // Best-effort: a failure is logged and MUST NOT rewrite the committed
        // delivery outcome; on a genuinely closed connection the unsubscribe
        // simply errors harmlessly.
        if maybe_subscribed && let Err(e) = self.unsubscribe_thread(thread_id).await {
            tracing::warn!(
                thread_id,
                "thread/unsubscribe after delivery failed (best-effort): {e}"
            );
        }

        match committed {
            Ok((turn_id, mode)) => self.poll_completion(thread_id, &turn_id, mode).await,
            Err(outcome) => outcome,
        }
    }

    /// Drive the thread to the delivery COMMIT POINT: an accepted `turn/start`
    /// (idle thread) or `turn/steer` (busy thread). Returns the committed
    /// `(turn id, mode)`, or a terminal [`Delivered`] for a pre-commit failure.
    /// Sets `*maybe_subscribed` when a `thread/resume` here may have taken a
    /// listener subscription the caller should release best-effort.
    async fn deliver_inner(
        &self,
        thread_id: &str,
        text: &str,
        maybe_subscribed: &mut bool,
    ) -> Result<(String, &'static str), Delivered> {
        let mut resumed = false;
        let mut steer_refused = false;
        let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
        loop {
            match self.thread_state(thread_id).await {
                Ok(ThreadState::Busy(Some(active_turn))) if !steer_refused => {
                    // COMMIT POINT, same discipline as turn/start: after the
                    // steer request is sent, a transport loss is ambiguous and
                    // retrying could inject the bus message twice.
                    match self
                        .call(
                            "turn/steer",
                            json!({
                                "threadId": thread_id,
                                "expectedTurnId": active_turn,
                                "input": [{ "type": "text", "text": text }],
                            }),
                        )
                        .await
                    {
                        Ok(v) => match v.get("turnId").and_then(Value::as_str) {
                            Some(id) => {
                                tracing::info!(
                                    thread_id,
                                    turn_id = id,
                                    "bus delivery steered into active turn"
                                );
                                return Ok((id.to_string(), "turn/steer"));
                            }
                            None => {
                                return Err(Delivered::Failed(
                                    "turn/steer returned no turn id".into(),
                                ));
                            }
                        },
                        Err(CallError::Transport(e)) => {
                            return Err(Delivered::Failed(format!(
                                "transport failure during turn/steer; completion unknown: {e}"
                            )));
                        }
                        Err(CallError::Rpc(e)) => {
                            // The host turn may have completed between read and
                            // steer, or it may be a non-steerable review/compact
                            // turn. The response proves nothing was injected;
                            // wait for idle and use a fresh turn exactly once.
                            tracing::debug!(
                                thread_id,
                                error = e,
                                "turn/steer rejected; falling back to turn/start on idle"
                            );
                            steer_refused = true;
                        }
                    }
                }
                Ok(ThreadState::Busy(_)) => {
                    if tokio::time::Instant::now() > deadline {
                        return Err(Delivered::Failed("thread stayed busy past timeout".into()));
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                Ok(ThreadState::Idle) => {
                    // turn/start while busy is accepted but dropped by the app
                    // server; it is used only after an idle read.
                    match self
                        .call(
                            "turn/start",
                            json!({ "threadId": thread_id, "input": [{ "type": "text", "text": text }] }),
                        )
                        .await
                    {
                        Ok(v) => match v.pointer("/turn/id").and_then(Value::as_str) {
                            Some(id) => return Ok((id.to_string(), "turn/start")),
                            None => {
                                return Err(Delivered::Failed(
                                    "turn/start returned no turn id".into(),
                                ));
                            }
                        },
                        Err(CallError::Transport(e)) => {
                            return Err(Delivered::Failed(format!(
                                "transport failure during turn/start; completion unknown: {e}"
                            )));
                        }
                        Err(CallError::Rpc(e)) => {
                            if thread_gone(&e) {
                                return Err(Delivered::Failed(format!("thread gone: {e}")));
                            }
                            if resumed || !unload_hint(&e) {
                                return Err(Delivered::Failed(e));
                            }
                            resumed = true;
                            let resumed_result = self.resume_thread(thread_id).await;
                            if resume_may_have_subscribed(&resumed_result) {
                                *maybe_subscribed = true;
                            }
                            match resumed_result {
                                Ok(()) => continue,
                                Err(CallError::Transport(t)) => {
                                    return Err(Delivered::Failed(format!(
                                        "transport failure during resume after turn/start attempt; \
                                         completion unknown: {t}"
                                    )));
                                }
                                Err(CallError::Rpc(r)) => {
                                    return Err(Delivered::Failed(format!("thread gone: {r}")));
                                }
                            }
                        }
                    }
                }
                Err(CallError::Transport(e)) => return Err(Delivered::Unreachable(e)),
                Err(CallError::Rpc(e)) => {
                    // Resume ONLY on a positively identified unload: resuming
                    // a loaded thread clears its goal state (findings.md), so
                    // an unrelated RPC error must fail, not resume. "no
                    // rollout" means the thread is gone entirely — terminal.
                    if thread_gone(&e) {
                        return Err(Delivered::Failed(format!("thread gone: {e}")));
                    }
                    if resumed || !unload_hint(&e) {
                        return Err(Delivered::Failed(format!("thread/read failed: {e}")));
                    }
                    resumed = true;
                    let resumed_result = self.resume_thread(thread_id).await;
                    if resume_may_have_subscribed(&resumed_result) {
                        *maybe_subscribed = true;
                    }
                    match resumed_result {
                        Ok(()) => continue,
                        Err(CallError::Transport(t)) => return Err(Delivered::Unreachable(t)),
                        Err(CallError::Rpc(r)) => {
                            return Err(Delivered::Failed(format!("thread gone: {r}")));
                        }
                    }
                }
            }
        }
    }

    /// Poll `thread/read` for the delivering turn's completion (the `processed`
    /// ack). A transport loss here is NOT retriable: the turn already started,
    /// so retrying the delivery would inject the message twice — report Failed
    /// with "completion unknown" instead.
    async fn poll_completion(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: &'static str,
    ) -> Delivered {
        let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            match self.turn_status(thread_id, turn_id).await {
                Ok(Some(status)) if status == "completed" => return Delivered::Processed,
                Ok(Some(status))
                    if status == "failed" || status == "error" || status == "interrupted" =>
                {
                    return Delivered::Failed(format!("turn ended with status {status}"));
                }
                Ok(_) => {}
                Err(CallError::Transport(e)) => {
                    return Delivered::Failed(format!(
                        "app-server connection lost after {mode}; completion unknown: {e}"
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
