//! Claude Code channel shim (docs/adapters/claude/requirements.md).
//!
//! One process per Claude Code session, spawned by the harness through the
//! Claude Code channels mechanism as a stdio MCP server. Bridges two sides:
//!
//! - stdin/stdout: MCP (newline-delimited JSON-RPC) toward Claude Code —
//!   declares the `claude/channel` capability, exposes the bus tool surface,
//!   pushes deliveries as `notifications/claude/channel` events.
//! - TCP: the broker connection (ADR-0016). Deliveries arrive as
//!   `message/deliver` requests whose response is the `relayed` ack.
//!
//! The shim never queues (CL-3) and reconnects to the broker with capped
//! exponential backoff, re-asserting its principal binding (CL-6, session
//! lifecycle "Broker restart").

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use protocol::methods as m;
use protocol::{
    ClientInfo, DeliverParams, DeliverResult, HelloParams, Id, Message, Response, RpcError, request,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

const INSTRUCTIONS: &str = "This session is connected to AgentWorkplace, a message bus shared \
with other coding agents and a human manager. Register once with the `register` tool when asked \
to join, then `subscribe` to the channels you are told to work with (prefer existing channels — \
check `who` first). Bus messages arrive as <channel source=\"workplace\"> events carrying the \
bus channel, sender, and thread_id in their attributes; reply with the `send` tool, passing that \
thread_id and addressing the originating channel and sender. Keep bus messages compact and \
self-contained; a delivered message does not oblige a reply unless one is requested.";

pub struct ShimConfig {
    pub broker_addr: String,
    pub version: String,
    /// When serving a Codex session: the shared `codex app-server --listen`
    /// endpoint, attached to registrations that carry a thread_id so the
    /// broker can deliver via the attach engine (CX-7).
    pub codex_app_server: Option<String>,
    /// Token presented to the broker in session/hello ([client].auth_token).
    pub auth_token: Option<String>,
}

pub async fn run(cfg: ShimConfig) -> anyhow::Result<()> {
    // stdout writer: the only thing allowed to touch stdout (MCP protocol).
    let (mcp_tx, mut mcp_rx) = mpsc::unbounded_channel::<Value>();
    let stdout_task = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(v) = mcp_rx.recv().await {
            let mut line = v.to_string();
            line.push('\n');
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    });

    let broker = BrokerLink::spawn(cfg, mcp_tx.clone());

    // stdin loop: MCP requests from Claude Code.
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            tracing::warn!("unparseable MCP line");
            continue;
        };
        let method = v
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let id = v.get("id").cloned().filter(|i| !i.is_null());
        let params = v.get("params").cloned().unwrap_or(Value::Null);
        match (id, method.as_str()) {
            (Some(id), "initialize") => {
                let requested = params
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("2025-06-18");
                mcp_respond(
                    &mcp_tx,
                    id,
                    json!({
                        "protocolVersion": requested,
                        "capabilities": { "experimental": { "claude/channel": {} }, "tools": {} },
                        "serverInfo": { "name": "workplace", "version": broker.version() },
                        "instructions": INSTRUCTIONS,
                    }),
                );
            }
            (Some(id), "tools/list") => {
                mcp_respond(&mcp_tx, id, json!({ "tools": tool_definitions() }))
            }
            (Some(id), "tools/call") => {
                let broker = broker.clone();
                let mcp_tx = mcp_tx.clone();
                tokio::spawn(async move {
                    let result = handle_tool_call(&broker, &params).await;
                    mcp_respond(&mcp_tx, id, result);
                });
            }
            (Some(id), "ping") => mcp_respond(&mcp_tx, id, json!({})),
            (Some(id), other) => {
                let _ = mcp_tx.send(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32601, "message": format!("method not found: {other}") },
                }));
            }
            (None, _) => { /* notifications (initialized, cancelled, ...) — nothing to do */ }
        }
    }

    // stdin closed: the session is gone (CL-3, no queueing). Exit immediately —
    // returning tears down the process, which closes the broker TCP connection
    // and frees the principal name for re-registration. (Awaiting the stdout
    // task here would hang forever: the broker link holds a sender clone, so
    // the channel never drains to closure.)
    stdout_task.abort();
    Ok(())
}

fn mcp_respond(mcp_tx: &mpsc::UnboundedSender<Value>, id: Value, result: Value) {
    let _ = mcp_tx.send(json!({ "jsonrpc": "2.0", "id": id, "result": result }));
}

// ---------------------------------------------------------------------------
// Broker link: owns the TCP connection, reconnects, re-registers.
// ---------------------------------------------------------------------------

struct Call {
    method: String,
    params: Value,
    resp: oneshot::Sender<Result<Value, RpcError>>,
}

#[derive(Clone)]
struct BrokerLink {
    calls: mpsc::UnboundedSender<Call>,
    version: Arc<String>,
    codex_app_server: Arc<Option<String>>,
}

impl BrokerLink {
    fn version(&self) -> String {
        self.version.as_ref().clone()
    }

    fn spawn(cfg: ShimConfig, mcp_tx: mpsc::UnboundedSender<Value>) -> BrokerLink {
        let (calls_tx, calls_rx) = mpsc::unbounded_channel::<Call>();
        let version = Arc::new(cfg.version.clone());
        let codex_app_server = Arc::new(cfg.codex_app_server.clone());
        tokio::spawn(broker_loop(cfg, mcp_tx, calls_rx));
        BrokerLink {
            calls: calls_tx,
            version,
            codex_app_server,
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let (tx, rx) = oneshot::channel();
        let call = Call {
            method: method.to_string(),
            params,
            resp: tx,
        };
        if self.calls.send(call).is_err() {
            return Err(unreachable_err());
        }
        match tokio::time::timeout(CALL_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            _ => Err(unreachable_err()),
        }
    }
}

fn unreachable_err() -> RpcError {
    RpcError {
        code: -32000,
        message: "AgentWorkplace broker is unreachable; the message was not sent".into(),
        data: None,
    }
}

async fn broker_loop(
    cfg: ShimConfig,
    mcp_tx: mpsc::UnboundedSender<Value>,
    mut calls_rx: mpsc::UnboundedReceiver<Call>,
) {
    // The full register params this shim carries across reconnects (name +
    // optional codex coordinates).
    // Backoff resets only after a connection has proven stable: a listener
    // that accepts and immediately drops must not produce a hot reconnect
    // loop.
    const STABLE_CONNECTION: Duration = Duration::from_secs(5);
    let mut binding: Option<Value> = None;
    let mut backoff = RECONNECT_MIN;
    loop {
        let stream = match TcpStream::connect(&cfg.broker_addr).await {
            Ok(s) => {
                tracing::debug!("connected to broker at {}", cfg.broker_addr);
                s
            }
            Err(e) => {
                tracing::warn!(
                    "broker connect failed ({}): {e}; retrying in {:?}",
                    cfg.broker_addr,
                    backoff
                );
                // While disconnected, fail tool calls immediately (never queue).
                drain_calls_until(&mut calls_rx, backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        let connected_at = tokio::time::Instant::now();
        if let Err(e) = connected_loop(&cfg, stream, &mcp_tx, &mut calls_rx, &mut binding).await {
            tracing::warn!("broker connection lost: {e}");
        }
        if connected_at.elapsed() >= STABLE_CONNECTION {
            backoff = RECONNECT_MIN;
        } else {
            drain_calls_until(&mut calls_rx, backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }
}

/// While disconnected, every tool call errors explicitly (message model:
/// broker unreachable is an error to the model, never silent loss).
async fn drain_calls_until(calls_rx: &mut mpsc::UnboundedReceiver<Call>, wait: Duration) {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        tokio::select! {
            call = calls_rx.recv() => match call {
                Some(call) => { let _ = call.resp.send(Err(unreachable_err())); }
                None => { tokio::time::sleep_until(deadline).await; return; }
            },
            _ = tokio::time::sleep_until(deadline) => return,
        }
    }
}

/// Binding side-effect to apply when a tracked call succeeds.
enum Effect {
    Register(Value),
    Deregister,
}

struct PendingCall {
    resp: Option<oneshot::Sender<Result<Value, RpcError>>>,
    effect: Option<Effect>,
}

async fn connected_loop(
    cfg: &ShimConfig,
    stream: TcpStream,
    mcp_tx: &mpsc::UnboundedSender<Value>,
    calls_rx: &mut mpsc::UnboundedReceiver<Call>,
    binding: &mut Option<Value>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    let next_id = AtomicU64::new(1);
    let mut pending: HashMap<u64, PendingCall> = HashMap::new();

    macro_rules! send_line {
        ($msg:expr) => {{
            let mut line = $msg.to_line();
            line.push('\n');
            write_half.write_all(line.as_bytes()).await?;
        }};
    }

    // Handshake, then re-assert the binding this shim carries.
    let hello = HelloParams {
        client_info: ClientInfo {
            harness: Some("claude-code".into()),
            version: cfg.version.clone(),
            pid: std::process::id(),
            cwd: std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        },
        auth_token: cfg.auth_token.clone(),
    };
    let hello_id = next_id.fetch_add(1, Ordering::Relaxed);
    send_line!(Message::Request(request(
        hello_id,
        m::SESSION_HELLO,
        &hello
    )));
    pending.insert(
        hello_id,
        PendingCall {
            resp: None,
            effect: None,
        },
    );
    if let Some(params) = binding.clone() {
        let reg_id = next_id.fetch_add(1, Ordering::Relaxed);
        send_line!(Message::Request(protocol::Request {
            jsonrpc: "2.0".into(),
            id: Id::Num(reg_id),
            method: m::PRINCIPAL_REGISTER.into(),
            params: Some(params.clone()),
        }));
        pending.insert(
            reg_id,
            PendingCall {
                resp: None,
                effect: Some(Effect::Register(params)),
            },
        );
    }

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { anyhow::bail!("broker closed the connection") };
                if line.trim().is_empty() { continue; }
                match Message::parse(&line) {
                    Ok(Message::Request(req)) => {
                        // Broker-originated request: delivery.
                        if req.method == m::MESSAGE_DELIVER {
                            let resp = handle_deliver(mcp_tx, req.params.unwrap_or(Value::Null));
                            let msg = match resp {
                                Ok(v) => protocol::ok_response(req.id, v),
                                Err(e) => protocol::err_response(req.id, e),
                            };
                            send_line!(Message::Response(msg));
                        } else {
                            send_line!(Message::Response(protocol::err_response(req.id, RpcError {
                                code: -32601,
                                message: format!("shim does not serve {}", req.method),
                                data: None,
                            })));
                        }
                    }
                    Ok(Message::Response(resp)) => {
                        let key = match &resp.id { Id::Num(n) => *n, Id::Str(_) | Id::Null => continue };
                        if let Some(call) = pending.remove(&key) {
                            let result = response_to_result(resp);
                            if result.is_ok() {
                                match call.effect {
                                    Some(Effect::Register(params)) => *binding = Some(params),
                                    Some(Effect::Deregister) => *binding = None,
                                    None => {}
                                }
                            }
                            if let Some(tx) = call.resp {
                                let _ = tx.send(result);
                            }
                        }
                    }
                    Ok(Message::Notification(_)) => { /* watch events: shim never watches */ }
                    Err(e) => tracing::warn!("unparseable broker line: {e}"),
                }
            }
            call = calls_rx.recv() => {
                let Some(call) = call else { anyhow::bail!("shim shutting down") };
                let effect = if call.method == m::PRINCIPAL_REGISTER {
                    Some(Effect::Register(call.params.clone()))
                } else if call.method == m::PRINCIPAL_DEREGISTER {
                    Some(Effect::Deregister)
                } else {
                    None
                };
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                send_line!(Message::Request(protocol::Request {
                    jsonrpc: "2.0".into(),
                    id: Id::Num(id),
                    method: call.method.clone(),
                    params: Some(call.params.clone()),
                }));
                pending.insert(id, PendingCall { resp: Some(call.resp), effect });
            }
        }
    }
}

fn response_to_result(resp: Response) -> Result<Value, RpcError> {
    match resp.error {
        Some(e) => Err(e),
        None => Ok(resp.result.unwrap_or(Value::Null)),
    }
}

/// Translate a broker delivery into a Claude Code channel event (CL-1) and
/// acknowledge relayed once it is on the session's stdio (CL-2).
fn handle_deliver(mcp_tx: &mpsc::UnboundedSender<Value>, params: Value) -> Result<Value, RpcError> {
    let p: DeliverParams = serde_json::from_value(params).map_err(|e| RpcError {
        code: -32602,
        message: format!("bad deliver params: {e}"),
        data: None,
    })?;
    let e = &p.envelope;
    let content = protocol::render_delivery(e);
    let mut meta = serde_json::Map::new();
    meta.insert("sender".into(), json!(e.sender));
    meta.insert("thread_id".into(), json!(e.thread_id.to_string()));
    meta.insert("message_id".into(), json!(e.message_id.to_string()));
    if let Some(first) = e.recipients.channels.first() {
        meta.insert("channel".into(), json!(first));
    }
    let event = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": { "content": content, "meta": Value::Object(meta) },
    });
    if mcp_tx.send(event).is_err() {
        return Err(RpcError {
            code: -32000,
            message: "session stdio closed".into(),
            data: None,
        });
    }
    serde_json::to_value(DeliverResult {
        status: "relayed".into(),
    })
    .map_err(|e| RpcError {
        code: -32603,
        message: e.to_string(),
        data: None,
    })
}

// ---------------------------------------------------------------------------
// Bus tools exposed to the model (tool contract, message-model.md)
// ---------------------------------------------------------------------------

fn tool_definitions() -> Value {
    json!([
        {
            "name": "register",
            "description": "Register this session on the AgentWorkplace bus as a named principal (e.g. @sec-reviewer). Required before any other bus action. If you are running in Codex, first run `echo $CODEX_THREAD_ID` in your shell and pass the value as thread_id so the bus can deliver messages into this session.",
            "inputSchema": { "type": "object", "properties": {
                "name": { "type": "string", "description": "Principal name: '@' + lowercase alphanumeric/dashes" },
                "thread_id": { "type": "string", "description": "Codex only: the value of $CODEX_THREAD_ID from your shell environment" }
            }, "required": ["name"] }
        },
        {
            "name": "deregister",
            "description": "Release this session's principal name on the bus.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "send",
            "description": "Send a message on the bus. Address one or more channels, one or more principals, or both (both = delivered to the listed principals subscribed to the listed channels). Pass thread_id to reply in an existing thread.",
            "inputSchema": { "type": "object", "properties": {
                "channels": { "type": "array", "items": { "type": "string" }, "description": "Channel names, '#'-prefixed" },
                "principals": { "type": "array", "items": { "type": "string" }, "description": "Principal names, '@'-prefixed" },
                "body": { "type": "string", "description": "Message body (markdown). Keep it compact and self-contained." },
                "thread_id": { "type": "integer", "description": "Existing thread to reply into; omit to start a new thread" }
            }, "required": ["body"] }
        },
        {
            "name": "subscribe",
            "description": "Subscribe this principal to a channel. Messages published after subscribing will be delivered into this session.",
            "inputSchema": { "type": "object", "properties": {
                "channel": { "type": "string" }
            }, "required": ["channel"] }
        },
        {
            "name": "unsubscribe",
            "description": "Unsubscribe this principal from a channel.",
            "inputSchema": { "type": "object", "properties": {
                "channel": { "type": "string" }
            }, "required": ["channel"] }
        },
        {
            "name": "create_channel",
            "description": "Create a new bus channel. Prefer an existing channel — check who first.",
            "inputSchema": { "type": "object", "properties": {
                "name": { "type": "string", "description": "Channel name: '#' + lowercase alphanumeric/dashes" }
            }, "required": ["name"] }
        },
        {
            "name": "history",
            "description": "Read past messages of a channel or a DM thread (pull-only; nothing is delivered retroactively).",
            "inputSchema": { "type": "object", "properties": {
                "channel": { "type": "string", "description": "Channel to read" },
                "dm_with": { "type": "string", "description": "Principal whose DM thread with you to read" },
                "before_message_id": { "type": "integer", "description": "Cursor: return records before this id" },
                "limit": { "type": "integer", "description": "Max records (default 50)" }
            } }
        },
        {
            "name": "who",
            "description": "Directory: all channels with their subscribers, and all principals with their active state.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

async fn handle_tool_call(broker: &BrokerLink, params: &Value) -> Value {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    let result = match name {
        "register" => {
            // A Codex agent self-reports its thread id (findings.md); the
            // shim contributes the app-server endpoint from its own config.
            let mut params = json!({ "name": args.get("name") });
            if let (Some(thread_id), Some(app_server)) = (
                args.get("thread_id").and_then(Value::as_str),
                broker.codex_app_server.as_ref().as_deref(),
            ) {
                params["codex"] = json!({ "app_server": app_server, "thread_id": thread_id });
            }
            broker.call(m::PRINCIPAL_REGISTER, params).await
        }
        "deregister" => broker.call(m::PRINCIPAL_DEREGISTER, json!({})).await,
        "send" => broker.call(m::MESSAGE_SEND, args).await,
        "subscribe" => broker.call(m::CHANNEL_SUBSCRIBE, args).await,
        "unsubscribe" => broker.call(m::CHANNEL_UNSUBSCRIBE, args).await,
        "create_channel" => broker.call(m::CHANNEL_CREATE, args).await,
        "history" => {
            let scope = if let Some(c) = args.get("channel") {
                json!({ "channel": c })
            } else if let Some(p) = args.get("dm_with") {
                json!({ "dm_with": p })
            } else {
                return tool_error("history needs channel or dm_with");
            };
            broker
                .call(
                    m::HISTORY_GET,
                    json!({
                        "scope": scope,
                        "before_message_id": args.get("before_message_id"),
                        "limit": args.get("limit").and_then(Value::as_u64).unwrap_or(50),
                    }),
                )
                .await
        }
        "who" => broker.call(m::DIRECTORY_WHO, json!({})).await,
        other => return tool_error(&format!("unknown tool: {other}")),
    };
    match result {
        Ok(v) => json!({ "content": [{ "type": "text", "text": v.to_string() }] }),
        Err(e) => tool_error(&e.message),
    }
}

fn tool_error(msg: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": format!("Error: {msg}") }], "isError": true })
}
