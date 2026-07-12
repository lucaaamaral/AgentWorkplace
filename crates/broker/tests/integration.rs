//! End-to-end broker tests over real TCP connections: the RPC surface,
//! delivery-as-request acks, watch streaming, channel lifecycle, and
//! restart re-evaluation of held state.

use std::collections::VecDeque;
use std::time::Duration;

use broker::{server, Broker, BrokerConfig};
use protocol::methods as m;
use protocol::*;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

const T: Duration = Duration::from_secs(5);

async fn start_broker(cfg: BrokerConfig) -> (std::net::SocketAddr, Broker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = Broker::new(cfg).unwrap();
    tokio::spawn(server::serve_listener(broker.clone(), listener));
    (addr, broker)
}

fn test_cfg() -> BrokerConfig {
    BrokerConfig {
        listens: vec![],
        db_path: None,
        message_size_limit: 64,
        grace_window: Duration::from_secs(60),
        version: "test".into(),
    }
}

struct Client {
    reader: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
    next_id: u64,
    queue: VecDeque<Message>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (r, w) = stream.into_split();
        Client { reader: BufReader::new(r).lines(), writer: w, next_id: 1, queue: VecDeque::new() }
    }

    async fn connect_hello(addr: std::net::SocketAddr) -> Client {
        let mut c = Client::connect(addr).await;
        c.call(
            m::SESSION_HELLO,
            json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" } }),
        )
        .await
        .unwrap();
        c
    }

    async fn write(&mut self, msg: Message) {
        let mut line = msg.to_line();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    /// Read straight from the socket, bypassing the local queue. Callers that
    /// are waiting for something specific scan the queue themselves first and
    /// park everything else there — never re-reading the queue in the same
    /// wait loop (that would spin forever without touching the socket).
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
        self.write(Message::Request(Request {
            jsonrpc: "2.0".into(),
            id: Id::Num(id),
            method: method.into(),
            params: Some(params),
        }))
        .await;
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

    /// Read (or dig out of the queue) the next message/deliver request.
    async fn next_deliver(&mut self, respond: bool) -> DeliverParams {
        // First check the queue.
        for i in 0..self.queue.len() {
            if let Message::Request(r) = &self.queue[i] {
                if r.method == m::MESSAGE_DELIVER {
                    let Message::Request(r) = self.queue.remove(i).unwrap() else { unreachable!() };
                    return self.finish_deliver(r, respond).await;
                }
            }
        }
        loop {
            match self.read_socket().await {
                Message::Request(r) if r.method == m::MESSAGE_DELIVER => {
                    return self.finish_deliver(r, respond).await;
                }
                other => self.queue.push_back(other),
            }
        }
    }

    async fn finish_deliver(&mut self, r: Request, respond: bool) -> DeliverParams {
        let params: DeliverParams = serde_json::from_value(r.params.unwrap()).unwrap();
        if respond {
            self.write(Message::Response(ok_response(r.id, json!({ "status": "relayed" }))))
                .await;
        }
        params
    }

    /// Read the next watch/event, skipping anything else.
    async fn next_watch(&mut self) -> WatchEvent {
        for i in 0..self.queue.len() {
            if let Message::Notification(n) = &self.queue[i] {
                if n.method == m::WATCH_EVENT {
                    let Message::Notification(n) = self.queue.remove(i).unwrap() else {
                        unreachable!()
                    };
                    return serde_json::from_value(n.params.unwrap()).unwrap();
                }
            }
        }
        loop {
            match self.read_socket().await {
                Message::Notification(n) if n.method == m::WATCH_EVENT => {
                    return serde_json::from_value(n.params.unwrap()).unwrap();
                }
                other => self.queue.push_back(other),
            }
        }
    }

    async fn register(&mut self, name: &str) {
        self.call(m::PRINCIPAL_REGISTER, json!({ "name": name })).await.unwrap();
    }
}

fn err_name(e: &RpcError) -> String {
    e.data.as_ref().and_then(|d| d["code"].as_str()).unwrap_or_default().to_string()
}

#[tokio::test]
async fn register_subscribe_send_deliver_ack() {
    let (addr, _broker) = start_broker(test_cfg()).await;

    let mut admin = Client::connect_hello(addr).await;
    admin.call(m::ADMIN_REGISTER, json!({ "name": "@manager" })).await.unwrap();
    admin.call(m::WATCH_START, json!({})).await.unwrap();

    let mut alice = Client::connect_hello(addr).await;
    alice.register("@alice").await;
    alice.call(m::CHANNEL_CREATE, json!({ "name": "#general" })).await.unwrap();
    alice.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#general" })).await.unwrap();

    let mut bob = Client::connect_hello(addr).await;
    bob.register("@bob").await;
    bob.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#general" })).await.unwrap();

    let sent: SendResult = serde_json::from_value(
        alice
            .call(m::MESSAGE_SEND, json!({ "channels": ["#general"], "body": "hello bob" }))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sent.delivery.delivered, vec!["@bob"]);
    assert!(sent.delivery.failed.is_empty());
    assert_eq!(sent.thread_id, sent.message_id);

    // Bob receives the delivery and acks relayed.
    let d = bob.next_deliver(true).await;
    assert_eq!(d.recipient, "@bob");
    assert_eq!(d.envelope.body, "hello bob");
    assert_eq!(d.envelope.sender, "@alice");

    // The admin tap sees it too (uncounted).
    let tap = admin.next_deliver(true).await;
    assert_eq!(tap.envelope.message_id, d.envelope.message_id);

    // Bob reports processed.
    bob.write(Message::Notification(notification(
        m::MESSAGE_PROCESSED,
        json!({ "message_id": d.envelope.message_id, "recipient": "@bob" }),
    )))
    .await;

    // Ack state converges to processed with per-state timestamps.
    let mut state = String::new();
    for _ in 0..50 {
        let v = admin
            .call(m::MESSAGE_STATUS, json!({ "message_id": d.envelope.message_id }))
            .await
            .unwrap();
        let r: MessageStatusResult = serde_json::from_value(v).unwrap();
        assert_eq!(r.acks.len(), 1); // the admin tap is not audience
        if matches!(r.acks[0].state, AckState::Processed) {
            assert!(r.acks[0].held_at.is_some());
            assert!(r.acks[0].relayed_at.is_some());
            assert!(r.acks[0].processed_at.is_some());
            state = "processed".into();
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state, "processed");

    // The watch stream carried the message record and an ack transition.
    // (Which transition is timing-dependent: acks are monotonic, so when
    // `processed` lands before the delivery task records `relayed`, the
    // relayed transition is suppressed as a stale straggler.)
    let mut saw_message = false;
    let mut saw_ack = false;
    for _ in 0..20 {
        match admin.next_watch().await {
            WatchEvent::Record(Record::Message { envelope }) => {
                assert_eq!(envelope.body, "hello bob");
                saw_message = true;
            }
            WatchEvent::Ack(a)
                if matches!(a.state, AckState::Relayed | AckState::Processed) =>
            {
                saw_ack = true;
            }
            _ => {}
        }
        if saw_message && saw_ack {
            break;
        }
    }
    assert!(saw_message && saw_ack);
}

#[tokio::test]
async fn intersection_addressing_and_empty_audiences() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#sec" })).await.unwrap();

    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;
    b.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#sec" })).await.unwrap();

    let mut c = Client::connect_hello(addr).await;
    c.register("@c").await;

    // channel ∩ principal: @b is subscribed → delivered; @c is not → excluded.
    let sent: SendResult = serde_json::from_value(
        a.call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#sec"], "principals": ["@b", "@c"], "body": "x" }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(sent.delivery.delivered, vec!["@b"]);
    b.next_deliver(true).await;

    // Intersection resolving to nobody: reported, stored, not an error.
    let sent: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "channels": ["#sec"], "principals": ["@c"], "body": "y" }))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(sent.delivery.delivered.is_empty());
    assert_eq!(sent.delivery.empty_audience, Some(EmptyAudience::EmptyIntersection));

    // Unknown names are errors and store nothing.
    let err = a
        .call(m::MESSAGE_SEND, json!({ "channels": ["#nope"], "body": "z" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");

    // Disconnected recipients fail per-recipient.
    drop(c);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let sent: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "principals": ["@c"], "body": "dm" })).await.unwrap(),
    )
    .unwrap();
    assert_eq!(sent.delivery.failed.len(), 1);
    assert_eq!(sent.delivery.failed[0].reason, "disconnected");
}

#[tokio::test]
async fn history_cursors_threads_and_dm_scope() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#g" })).await.unwrap();
    a.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#g" })).await.unwrap();

    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;

    let first: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "channels": ["#g"], "body": "m1" })).await.unwrap(),
    )
    .unwrap();
    // Reply in-thread.
    let reply: SendResult = serde_json::from_value(
        a.call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#g"], "body": "m2", "thread_id": first.message_id }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(reply.thread_id, first.message_id);
    a.call(m::MESSAGE_SEND, json!({ "channels": ["#g"], "body": "m3" })).await.unwrap();

    // Cursor paging, newest-last.
    let page: HistoryResult = serde_json::from_value(
        b.call(m::HISTORY_GET, json!({ "scope": { "channel": "#g" }, "limit": 2 }))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(page.records.len(), 2);
    assert!(page.next_cursor.is_some());
    let older: HistoryResult = serde_json::from_value(
        b.call(
            m::HISTORY_GET,
            json!({ "scope": { "channel": "#g" }, "limit": 2, "before_message_id": page.next_cursor }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(!older.records.is_empty());
    assert!(older.records.iter().all(|r| r.id() < page.records[0].id()));

    // DMs: participants can read their own; dm_between is admin-only.
    a.call(m::MESSAGE_SEND, json!({ "principals": ["@b"], "body": "psst" })).await.unwrap();
    b.next_deliver(true).await;
    let dm: HistoryResult = serde_json::from_value(
        b.call(m::HISTORY_GET, json!({ "scope": { "dm_with": "@a" }, "limit": 10 }))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(dm.records.len(), 1);
    let err = b
        .call(m::HISTORY_GET, json!({ "scope": { "dm_between": ["@a", "@b"] }, "limit": 10 }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "SCOPE_DENIED");
}

#[tokio::test]
async fn body_truncation_not_rejection() {
    let (addr, _broker) = start_broker(test_cfg()).await; // 64-byte limit
    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#g" })).await.unwrap();
    let long = "x".repeat(200);
    let sent: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "channels": ["#g"], "body": long })).await.unwrap(),
    )
    .unwrap();
    let hist: HistoryResult = serde_json::from_value(
        a.call(m::HISTORY_GET, json!({ "scope": { "channel": "#g" }, "limit": 10 }))
            .await
            .unwrap(),
    )
    .unwrap();
    let Record::Message { envelope } =
        hist.records.iter().find(|r| r.id() == sent.message_id).unwrap()
    else {
        panic!("expected message record");
    };
    assert!(envelope.truncated);
    assert_eq!(envelope.body.len(), 64);
}

#[tokio::test]
async fn name_claims_and_registration_lifecycle() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut a = Client::connect_hello(addr).await;
    a.register("@dev").await;

    let mut imposter = Client::connect_hello(addr).await;
    let err = imposter.call(m::PRINCIPAL_REGISTER, json!({ "name": "@dev" })).await.unwrap_err();
    assert_eq!(err_name(&err), "NAME_TAKEN");

    let err =
        imposter.call(m::PRINCIPAL_REGISTER, json!({ "name": "not-a-name" })).await.unwrap_err();
    assert_eq!(err_name(&err), "INVALID_NAME");

    // Deregistration frees the name.
    a.call(m::PRINCIPAL_DEREGISTER, json!({})).await.unwrap();
    imposter.register("@dev").await;

    // Admin verbs on a non-admin session are refused.
    let err = imposter
        .call(m::ADMIN_SUBSCRIBE, json!({ "principal": "@dev", "channel": "#g" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "NOT_ADMIN");
}

#[tokio::test]
async fn channel_lifecycle_archive_delete() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut admin = Client::connect_hello(addr).await;
    admin.call(m::ADMIN_REGISTER, json!({ "name": "@manager" })).await.unwrap();

    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#tmp" })).await.unwrap();
    a.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#tmp" })).await.unwrap();
    a.call(m::MESSAGE_SEND, json!({ "channels": ["#tmp"], "body": "doomed" })).await.unwrap();

    // Archive: hidden, refuses subscriptions and sends; name reserved.
    admin.call(m::CHANNEL_ARCHIVE, json!({ "channel": "#tmp" })).await.unwrap();
    let err =
        a.call(m::MESSAGE_SEND, json!({ "channels": ["#tmp"], "body": "x" })).await.unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");
    let err = a.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#tmp" })).await.unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");
    let err = a.call(m::CHANNEL_CREATE, json!({ "name": "#tmp" })).await.unwrap_err();
    assert_eq!(err_name(&err), "NAME_TAKEN");
    let who: WhoResult =
        serde_json::from_value(a.call(m::DIRECTORY_WHO, json!({})).await.unwrap()).unwrap();
    assert!(!who.channels.iter().any(|c| c.channel == "#tmp"));

    // Archived history is admin-only.
    let err = a
        .call(m::HISTORY_GET, json!({ "scope": { "channel": "#tmp" }, "limit": 10 }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "SCOPE_DENIED");
    let hist: HistoryResult = serde_json::from_value(
        admin
            .call(m::HISTORY_GET, json!({ "scope": { "channel": "#tmp" }, "limit": 10 }))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(hist.records.iter().any(|r| matches!(r, Record::Message { .. })));

    // Unarchive is unconditional.
    admin.call(m::CHANNEL_UNARCHIVE, json!({ "channel": "#tmp" })).await.unwrap();

    // Guarded deletion: bad token refused; wrong-session token refused.
    let tok: ChannelDeleteResult = serde_json::from_value(
        admin.call(m::CHANNEL_DELETE, json!({ "channel": "#tmp" })).await.unwrap(),
    )
    .unwrap();
    let err = admin
        .call(
            m::CHANNEL_DELETE_CONFIRM,
            json!({ "channel": "#tmp", "confirmation_token": "bogus" }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "BAD_CONFIRMATION");
    admin
        .call(
            m::CHANNEL_DELETE_CONFIRM,
            json!({ "channel": "#tmp", "confirmation_token": tok.confirmation_token }),
        )
        .await
        .unwrap();

    // Token is single-use, the channel is gone, and the name is free again.
    let err = admin
        .call(m::HISTORY_GET, json!({ "scope": { "channel": "#tmp" }, "limit": 10 }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");
    a.call(m::CHANNEL_CREATE, json!({ "name": "#tmp" })).await.unwrap();
}

#[tokio::test]
async fn restart_reevaluates_held_state() {
    let dir = std::env::temp_dir().join(format!("workplace-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join(format!("restart-{}.db", protocol::PROTOCOL_VERSION));
    let _ = std::fs::remove_file(&db);

    let mut cfg = test_cfg();
    cfg.db_path = Some(db.clone());
    let (addr, _broker) = start_broker(cfg).await;

    let mut admin = Client::connect_hello(addr).await;
    admin.call(m::ADMIN_REGISTER, json!({ "name": "@manager" })).await.unwrap();

    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;

    // Send a DM to @b; @b never acknowledges → the ack row stays held.
    let sent: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "principals": ["@b"], "body": "limbo" })).await.unwrap(),
    )
    .unwrap();
    let _unanswered = b.next_deliver(false).await;

    // "Restart": a new broker over the same database, 1s grace window; @b
    // does not re-attach.
    let mut cfg2 = test_cfg();
    cfg2.db_path = Some(db.clone());
    cfg2.grace_window = Duration::from_secs(1);
    let (addr2, _broker2) = start_broker(cfg2).await;

    let mut admin2 = Client::connect_hello(addr2).await;
    admin2.call(m::ADMIN_REGISTER, json!({ "name": "@manager2" })).await.unwrap();

    let mut failed = false;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let v = admin2.call(m::MESSAGE_STATUS, json!({ "message_id": sent.message_id })).await;
        if let Ok(v) = v {
            let r: MessageStatusResult = serde_json::from_value(v).unwrap();
            if let Some(ack) = r.acks.iter().find(|x| x.recipient == "@b") {
                if matches!(ack.state, AckState::Failed) {
                    assert_eq!(ack.reason.as_deref(), Some("disconnected"));
                    failed = true;
                    break;
                }
            }
        }
    }
    assert!(failed, "held delivery was not failed after the grace window");
    let _ = std::fs::remove_file(&db);
}
