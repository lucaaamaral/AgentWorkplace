//! Broker → Codex delivery routing, against a fake in-process app-server.
//!
//! A WebSocket server mimics `codex app-server --listen`. A session registers
//! with codex coordinates pointing at it; deliveries to that principal must
//! route via the attach engine (NOT message/deliver on the registering
//! connection), with the CX-2/5/8/11 behaviors: active threads receive through
//! turn/steer while idle threads use turn/start, unloaded threads are resumed
//! first, an unreachable app-server holds the delivery while the session is
//! present, and approval requests are never answered by the adapter.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
use std::time::Duration;

use broker::{Broker, BrokerConfig, server};
use futures_util::{SinkExt, StreamExt};
use protocol::methods as m;
use protocol::*;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message as WsMessage;

const T: Duration = Duration::from_secs(10);

/// Knobs for the fake `codex app-server --listen`.
#[derive(Clone, Default)]
struct FakeBehavior {
    /// thread/read reports an active turn this many times before going idle.
    busy_reads: usize,
    /// Optional failure at the active-turn steer commit point.
    steer_failure: SteerFailure,
    /// thread/read and turn/start error with a not-loaded hint until the
    /// client has called thread/resume (CX-8).
    require_resume: bool,
    /// thread/read initially reports idle, but turn/start observes that the
    /// thread unloaded in the intervening race window.
    unload_at_turn_start: bool,
    /// Return successful turn/start or turn/steer responses without the
    /// required turn identifier.
    omit_start_turn_id: bool,
    omit_steer_turn_id: bool,
    /// Once a delivery has been injected, make the completion poll fail with
    /// an RPC error.
    poll_rpc_error: bool,
    /// Every thread call errors "no rollout found" — the thread is gone
    /// entirely (terminal, must NOT trigger a resume).
    gone: bool,
    /// Send a server-initiated approval request to each client right after
    /// its initialize (CX-11: the adapter must never answer it).
    send_approval: bool,
    /// Expected capability token: the handshake records whether the client
    /// presented `Authorization: Bearer <this>`.
    expect_bearer: Option<String>,
}

#[derive(Clone, Copy, Default)]
enum SteerFailure {
    #[default]
    None,
    /// The host turn completed between thread/read and turn/steer.
    NoActiveTurn,
    /// Review/compact turns reject same-turn steering.
    NotSteerable,
    /// The socket drops after the steer request arrives, before its response.
    TransportDrop,
}

/// Counters observed by tests.
#[derive(Default)]
struct FakeStats {
    resumes: AtomicUsize,
    /// Client messages carrying result/error for the approval request id.
    approval_answers: AtomicUsize,
    turns_started: AtomicUsize,
    steers: AtomicUsize,
    /// Handshakes that presented the expected Bearer token.
    bearer_ok: AtomicUsize,
}

const APPROVAL_ID: u64 = 424242;

/// Minimal fake `codex app-server --listen`: answers just enough of the
/// protocol for CodexAttach (initialize, thread/read, thread/resume,
/// turn/start, turn/steer).
#[allow(clippy::result_large_err)] // tungstenite's handshake callback signature
async fn fake_app_server(behavior: FakeBehavior) -> (String, Arc<FakeStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stats = Arc::new(FakeStats::default());
    let server_stats = stats.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let behavior = behavior.clone();
            let stats = server_stats.clone();
            tokio::spawn(async move {
                let hs_stats = stats.clone();
                let expect_bearer = behavior.expect_bearer.clone();
                let callback =
                    move |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                          resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        if let Some(expect) = &expect_bearer {
                            let got = req
                                .headers()
                                .get("authorization")
                                .and_then(|v| v.to_str().ok());
                            if got == Some(format!("Bearer {expect}").as_str()) {
                                hs_stats.bearer_ok.fetch_add(1, AtOrd::SeqCst);
                            }
                        }
                        Ok(resp)
                    };
                let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
                    return;
                };
                let (mut sink, mut source) = ws.split();
                let mut turn_started = false;
                let mut steered = false;
                let mut resumed = false;
                let mut reads_seen = 0usize;
                while let Some(Ok(msg)) = source.next().await {
                    let WsMessage::Text(text) = msg else { continue };
                    let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else {
                        continue;
                    };
                    // An answer to our approval request would carry its id
                    // with a result/error and no method.
                    if v.get("method").is_none()
                        && v.get("id").and_then(Value::as_u64) == Some(APPROVAL_ID)
                    {
                        stats.approval_answers.fetch_add(1, AtOrd::SeqCst);
                        continue;
                    }
                    let (Some(id), Some(method)) = (
                        v.get("id").cloned(),
                        v.get("method").and_then(Value::as_str),
                    ) else {
                        continue;
                    };
                    let result = match method {
                        "initialize" => {
                            if behavior.send_approval {
                                let approval = json!({
                                    "jsonrpc": "2.0", "id": APPROVAL_ID,
                                    "method": "item/commandExecution/requestApproval",
                                    "params": { "command": "rm -rf /" },
                                });
                                let _ = sink
                                    .send(WsMessage::Text(approval.to_string().into()))
                                    .await;
                            }
                            json!({ "ok": true })
                        }
                        "thread/resume" => {
                            resumed = true;
                            stats.resumes.fetch_add(1, AtOrd::SeqCst);
                            json!({ "thread": { "id": "th-1" } })
                        }
                        "turn/start" => {
                            if behavior.gone {
                                let resp = json!({ "jsonrpc": "2.0", "id": id, "error": {
                                    "code": -32600, "message": "no rollout found for thread id th-1" } });
                                if sink
                                    .send(WsMessage::Text(resp.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                            if (behavior.require_resume || behavior.unload_at_turn_start)
                                && !resumed
                            {
                                let resp = json!({ "jsonrpc": "2.0", "id": id, "error": {
                                    "code": -32600, "message": "thread th-1 is not loaded" } });
                                if sink
                                    .send(WsMessage::Text(resp.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                            turn_started = true;
                            stats.turns_started.fetch_add(1, AtOrd::SeqCst);
                            if behavior.omit_start_turn_id {
                                json!({ "turn": { "status": "inProgress" } })
                            } else {
                                json!({ "turn": { "id": "turn-1", "status": "inProgress" } })
                            }
                        }
                        "turn/steer" => {
                            stats.steers.fetch_add(1, AtOrd::SeqCst);
                            assert_eq!(
                                v.pointer("/params/threadId").and_then(Value::as_str),
                                Some("th-1")
                            );
                            assert_eq!(
                                v.pointer("/params/expectedTurnId").and_then(Value::as_str),
                                Some("host-turn")
                            );
                            assert_eq!(
                                v.pointer("/params/input/0/type").and_then(Value::as_str),
                                Some("text")
                            );
                            assert!(
                                v.pointer("/params/input/0/text")
                                    .and_then(Value::as_str)
                                    .is_some_and(|text| text.contains("Bus message")),
                                "steer input must carry the rendered bus delivery"
                            );
                            match behavior.steer_failure {
                                SteerFailure::TransportDrop => return,
                                SteerFailure::NoActiveTurn | SteerFailure::NotSteerable => {
                                    reads_seen = behavior.busy_reads;
                                    let message = match behavior.steer_failure {
                                        SteerFailure::NoActiveTurn => "no active turn to steer",
                                        SteerFailure::NotSteerable => {
                                            "active turn cannot accept same-turn steering"
                                        }
                                        _ => unreachable!(),
                                    };
                                    let resp = json!({ "jsonrpc": "2.0", "id": id, "error": {
                                        "code": -32600, "message": message } });
                                    if sink
                                        .send(WsMessage::Text(resp.to_string().into()))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                    continue;
                                }
                                SteerFailure::None => {
                                    steered = true;
                                    if behavior.omit_steer_turn_id {
                                        json!({})
                                    } else {
                                        json!({ "turnId": "host-turn" })
                                    }
                                }
                            }
                        }
                        "thread/read" => {
                            if behavior.gone {
                                let resp = json!({ "jsonrpc": "2.0", "id": id, "error": {
                                    "code": -32600, "message": "no rollout found for thread id th-1" } });
                                if sink
                                    .send(WsMessage::Text(resp.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                            if behavior.require_resume && !resumed {
                                let resp = json!({ "jsonrpc": "2.0", "id": id, "error": {
                                    "code": -32600, "message": "thread th-1 is not loaded" } });
                                if sink
                                    .send(WsMessage::Text(resp.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                            if behavior.poll_rpc_error && (turn_started || steered) {
                                let resp = json!({ "jsonrpc": "2.0", "id": id, "error": {
                                    "code": -32603, "message": "completion poll failed" } });
                                if sink
                                    .send(WsMessage::Text(resp.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                            let busy =
                                reads_seen < behavior.busy_reads && !turn_started && !steered;
                            reads_seen += 1;
                            let status = if busy { "active" } else { "idle" };
                            let turns: Vec<Value> = if busy {
                                vec![json!({ "id": "host-turn", "status": "inProgress" })]
                            } else if steered {
                                vec![json!({ "id": "host-turn", "status": "completed" })]
                            } else if turn_started {
                                vec![json!({ "id": "turn-1", "status": "completed" })]
                            } else {
                                vec![]
                            };
                            json!({ "thread": { "status": { "type": status }, "turns": turns } })
                        }
                        _ => json!({}),
                    };
                    let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
                    if sink
                        .send(WsMessage::Text(resp.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    (format!("ws://{addr}"), stats)
}

struct Client {
    reader: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
    next_id: u64,
    queue: VecDeque<Message>,
}

impl Client {
    async fn connect_hello(addr: std::net::SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (r, w) = stream.into_split();
        let mut c = Client {
            reader: BufReader::new(r).lines(),
            writer: w,
            next_id: 1,
            queue: VecDeque::new(),
        };
        c.call(
            m::SESSION_HELLO,
            json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" } }),
        )
        .await
        .unwrap();
        c
    }

    async fn read_socket(&mut self) -> Message {
        loop {
            let line = tokio::time::timeout(T, self.reader.next_line())
                .await
                .expect("read timeout")
                .unwrap()
                .expect("connection closed");
            if line.trim().is_empty() {
                continue;
            }
            return Message::parse(&line).unwrap();
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id;
        self.next_id += 1;
        let mut line = Message::Request(Request {
            jsonrpc: "2.0".into(),
            id: Id::Num(id),
            method: method.into(),
            params: Some(params),
        })
        .to_line();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
        loop {
            match self.read_socket().await {
                Message::Response(resp) if resp.id == Id::Num(id) => {
                    return match resp.error {
                        Some(e) => Err(e),
                        None => Ok(resp.result.unwrap_or(Value::Null)),
                    };
                }
                other => self.queue.push_back(other),
            }
        }
    }

    /// Assert no message/deliver request arrives on this connection within a
    /// short window (codex-registered sessions are delivered via attach).
    async fn assert_no_deliver(&mut self, window: Duration) {
        let deadline = tokio::time::Instant::now() + window;
        // Drain the queue first.
        assert!(
            !self
                .queue
                .iter()
                .any(|m| matches!(m, Message::Request(r) if r.method == m::MESSAGE_DELIVER)),
            "unexpected message/deliver on codex session connection"
        );
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline - tokio::time::Instant::now();
            match tokio::time::timeout(remaining, self.reader.next_line()).await {
                Ok(Ok(Some(line))) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let msg = Message::parse(&line).unwrap();
                    if let Message::Request(r) = &msg {
                        assert_ne!(
                            r.method,
                            m::MESSAGE_DELIVER,
                            "codex session must not receive message/deliver"
                        );
                    }
                    self.queue.push_back(msg);
                }
                _ => break,
            }
        }
    }
}

async fn start_broker() -> (std::net::SocketAddr, Broker) {
    start_broker_with_token(None).await
}

async fn start_broker_with_token(
    codex_token_file: Option<std::path::PathBuf>,
) -> (std::net::SocketAddr, Broker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = Broker::new(BrokerConfig {
        listens: vec![],
        db_path: None,
        message_size_limit: 1024,
        grace_window: Duration::from_secs(60),
        version: "test".into(),
        auth_token: None,
        codex_token_file,
        max_out_queue: 8192,
        admin_token: Some("test-admin".into()),
    })
    .unwrap();
    tokio::spawn(server::serve_listener(broker.clone(), listener));
    (addr, broker)
}

/// Register a codex-coordinates principal, DM it, and return (sender's send
/// result, admin client) for ack polling.
async fn register_and_send(
    addr: std::net::SocketAddr,
    app_server: &str,
) -> (Client, SendResult, Client) {
    let mut codex_agent = Client::connect_hello(addr).await;
    codex_agent
        .call(
            m::PRINCIPAL_REGISTER,
            json!({ "name": "@codex-1", "codex": { "app_server": app_server, "thread_id": "th-1" } }),
        )
        .await
        .unwrap();

    let mut sender = Client::connect_hello(addr).await;
    sender
        .call(m::PRINCIPAL_REGISTER, json!({ "name": "@sender" }))
        .await
        .unwrap();
    let sent: SendResult = serde_json::from_value(
        sender
            .call(
                m::MESSAGE_SEND,
                json!({ "principals": ["@codex-1"], "body": "ping codex" }),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sent.delivery.delivered, vec!["@codex-1"]);

    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@mgr", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();
    (codex_agent, sent, admin)
}

async fn wait_for_ack(
    admin: &mut Client,
    message_id: u64,
    recipient: &str,
    want: AckState,
    tries: u32,
) -> Option<AckStatus> {
    for _ in 0..tries {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let v = admin
            .call(m::MESSAGE_STATUS, json!({ "message_id": message_id }))
            .await
            .unwrap();
        let r: MessageStatusResult = serde_json::from_value(v).unwrap();
        if let Some(ack) = r.acks.iter().find(|a| a.recipient == recipient)
            && ack.state == want
        {
            return Some(ack.clone());
        }
    }
    None
}

async fn wait_for_failed_reason(admin: &mut Client, message_id: u64, context: &str) -> String {
    wait_for_ack(admin, message_id, "@codex-1", AckState::Failed, 120)
        .await
        .unwrap_or_else(|| panic!("{context}"))
        .reason
        .unwrap_or_default()
}

#[tokio::test]
async fn codex_registered_principal_is_delivered_via_attach() {
    let (app_server, stats) = fake_app_server(FakeBehavior::default()).await;
    let (addr, _broker) = start_broker().await;
    let (mut codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    // Delivery must go via the fake app-server, never via this connection.
    codex_agent.assert_no_deliver(Duration::from_secs(3)).await;

    // Ack converges to processed (attach engine polled turn-1 to completed).
    let ack = wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        80,
    )
    .await
    .expect("codex delivery did not reach processed via the attach engine");
    assert!(ack.relayed_at.is_some());
    assert!(ack.processed_at.is_some());
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 0);
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 1);
}

#[tokio::test]
async fn busy_thread_is_steered_without_waiting_for_idle() {
    // A message arriving mid-turn must be steered into that active turn
    // immediately; turn/start remains reserved for idle threads.
    let (app_server, stats) = fake_app_server(FakeBehavior {
        busy_reads: 3,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        120,
    )
    .await
    .expect("delivery steered into an active turn should process");
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 0);
}

#[tokio::test]
async fn steer_success_without_turn_id_fails_without_retry() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        busy_reads: 1,
        omit_steer_turn_id: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    let reason = wait_for_failed_reason(
        &mut admin,
        sent.message_id,
        "steer response without a turn id should fail",
    )
    .await;
    assert!(
        reason.contains("turn/steer returned no turn id"),
        "{reason}"
    );
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 0);
}

#[tokio::test]
async fn steer_completion_race_falls_back_to_turn_start() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        busy_reads: 1,
        steer_failure: SteerFailure::NoActiveTurn,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        120,
    )
    .await
    .expect("a completed host turn should fall back to a fresh turn");
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 1);
}

#[tokio::test]
async fn nonsteerable_turn_falls_back_without_losing_the_message() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        busy_reads: 1,
        steer_failure: SteerFailure::NotSteerable,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        120,
    )
    .await
    .expect("a non-steerable host turn should fall back after it becomes idle");
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 1);
}

#[tokio::test]
async fn transport_drop_at_steer_is_terminal_completion_unknown() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        busy_reads: 1,
        steer_failure: SteerFailure::TransportDrop,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    let ack = wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Failed,
        120,
    )
    .await
    .expect("ambiguous steer transport loss must fail without retry");
    assert!(
        ack.reason
            .as_deref()
            .unwrap_or_default()
            .contains("completion unknown")
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 1, "steer must not retry");
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 0);
}

#[tokio::test]
async fn unloaded_thread_is_resumed_before_delivery() {
    // Every call errors "no rollout found" until thread/resume: CX-8 says the
    // adapter resumes transparently and then delivers.
    let (app_server, stats) = fake_app_server(FakeBehavior {
        require_resume: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        120,
    )
    .await
    .expect("delivery to an unloaded thread should resume then process");
    assert_eq!(
        stats.resumes.load(AtOrd::SeqCst),
        1,
        "exactly one thread/resume expected"
    );
}

#[tokio::test]
async fn unload_between_idle_read_and_turn_start_resumes_once() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        unload_at_turn_start: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        120,
    )
    .await
    .expect("turn/start unload race should resume and process the delivery");
    assert_eq!(stats.resumes.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.steers.load(AtOrd::SeqCst), 0);
}

#[tokio::test]
async fn turn_start_success_without_turn_id_fails_without_retry() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        omit_start_turn_id: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    let reason = wait_for_failed_reason(
        &mut admin,
        sent.message_id,
        "turn/start response without a turn id should fail",
    )
    .await;
    assert!(
        reason.contains("turn/start returned no turn id"),
        "{reason}"
    );
    assert_eq!(stats.turns_started.load(AtOrd::SeqCst), 1);
    assert_eq!(stats.resumes.load(AtOrd::SeqCst), 0);
}

#[tokio::test]
async fn completion_poll_rpc_error_is_terminal() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        poll_rpc_error: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    let reason = wait_for_failed_reason(
        &mut admin,
        sent.message_id,
        "completion poll RPC error should terminate the committed delivery",
    )
    .await;
    assert!(
        reason.contains("poll failed: completion poll failed"),
        "{reason}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stats.turns_started.load(AtOrd::SeqCst),
        1,
        "committed delivery must not retry after a poll failure"
    );
    assert_eq!(stats.resumes.load(AtOrd::SeqCst), 0);
}

#[tokio::test]
async fn adapter_never_answers_approval_requests() {
    let (app_server, stats) = fake_app_server(FakeBehavior {
        send_approval: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        80,
    )
    .await
    .expect("delivery should process regardless of the pending approval");
    // Grace period for any (incorrect) late answer to land.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        stats.approval_answers.load(AtOrd::SeqCst),
        0,
        "the adapter must never answer an approval request (ADR-0012/CX-11)"
    );
}

#[tokio::test]
async fn unreachable_app_server_holds_while_session_present() {
    // Reserve a port with nothing listening on it.
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap();
    drop(dead);
    let app_server = format!("ws://{dead_addr}");

    let (addr, _broker) = start_broker().await;
    let (codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    // CX-5: while the session is present, the delivery stays held (the
    // broker is retrying), never failed.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let v = admin
        .call(m::MESSAGE_STATUS, json!({ "message_id": sent.message_id }))
        .await
        .unwrap();
    let r: MessageStatusResult = serde_json::from_value(v).unwrap();
    let ack = r
        .acks
        .iter()
        .find(|a| a.recipient == "@codex-1")
        .expect("ack row");
    assert_eq!(
        ack.state,
        AckState::Held,
        "unreachable app-server must hold, not fail"
    );

    // Session disconnects: no store-and-forward — the delivery fails.
    drop(codex_agent);
    let ack = wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Failed,
        80,
    )
    .await
    .expect("delivery should fail once the session is gone");
    assert_eq!(ack.reason.as_deref(), Some("disconnected"));
}

#[tokio::test]
async fn attach_presents_bearer_token_on_handshake() {
    let token_file =
        std::env::temp_dir().join(format!("workplace-codex-token-{}", std::process::id()));
    std::fs::write(&token_file, "tok-cap-123\n").unwrap();

    let (app_server, stats) = fake_app_server(FakeBehavior {
        expect_bearer: Some("tok-cap-123".into()),
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker_with_token(Some(token_file.clone())).await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Processed,
        80,
    )
    .await
    .expect("delivery with capability token should process");
    assert!(
        stats.bearer_ok.load(AtOrd::SeqCst) >= 1,
        "the attach client must present Authorization: Bearer on the upgrade"
    );
    let _ = std::fs::remove_file(&token_file);
}

#[tokio::test]
async fn gone_thread_fails_without_resume() {
    // "no rollout found" means the thread is gone entirely: terminal failure,
    // and resuming (which would clear a loaded thread's goal state) must not
    // be attempted.
    let (app_server, stats) = fake_app_server(FakeBehavior {
        gone: true,
        ..Default::default()
    })
    .await;
    let (addr, _broker) = start_broker().await;
    let (_codex_agent, sent, mut admin) = register_and_send(addr, &app_server).await;

    let ack = wait_for_ack(
        &mut admin,
        sent.message_id,
        "@codex-1",
        AckState::Failed,
        80,
    )
    .await
    .expect("delivery to a gone thread should fail");
    assert!(
        ack.reason
            .as_deref()
            .unwrap_or_default()
            .contains("thread gone"),
        "failure reason should identify the gone thread: {:?}",
        ack.reason
    );
    assert_eq!(
        stats.resumes.load(AtOrd::SeqCst),
        0,
        "gone threads must not be resumed"
    );
}

#[tokio::test]
async fn non_loopback_app_server_is_rejected_at_registration() {
    let (addr, _broker) = start_broker().await;
    let mut agent = Client::connect_hello(addr).await;
    let err = agent
        .call(
            m::PRINCIPAL_REGISTER,
            json!({ "name": "@codex-evil", "codex": { "app_server": "ws://10.1.2.3:9701", "thread_id": "th-1" } }),
        )
        .await
        .unwrap_err();
    assert!(
        err.message.contains("loopback"),
        "SSRF guard should name the constraint: {}",
        err.message
    );
}
