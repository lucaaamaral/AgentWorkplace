//! End-to-end broker tests over real TCP connections: the RPC surface,
//! delivery-as-request acks, watch streaming, channel lifecycle, and
//! restart re-evaluation of held state.

use std::time::Duration;

use broker::BrokerConfig;
use protocol::methods as m;
use protocol::*;
use serde_json::json;
use tokio::io::AsyncWriteExt;

mod common;
use common::{Client, start_broker};

const T: Duration = Duration::from_secs(5);

fn test_cfg() -> BrokerConfig {
    BrokerConfig {
        listens: vec![],
        db_path: None,
        message_size_limit: 64,
        grace_window: Duration::from_secs(60),
        version: "test".into(),
        auth_token: None,
        codex_token_file: None,
        max_out_queue: 8192,
        admin_token: Some("test-admin".into()),
    }
}

impl Client {
    /// Read (or dig out of the queue) the next message/deliver request.
    async fn next_deliver(&mut self, respond: bool) -> DeliverParams {
        // First check the queue.
        for i in 0..self.queue.len() {
            if let Message::Request(r) = &self.queue[i]
                && r.method == m::MESSAGE_DELIVER
            {
                let Message::Request(r) = self.queue.remove(i).unwrap() else {
                    unreachable!()
                };
                return self.finish_deliver(r, respond).await;
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
            self.write(Message::Response(ok_response(
                r.id,
                json!({ "status": "relayed" }),
            )))
            .await;
        }
        params
    }

    /// Read the next watch/event, skipping anything else.
    async fn next_watch(&mut self) -> WatchEvent {
        for i in 0..self.queue.len() {
            if let Message::Notification(n) = &self.queue[i]
                && n.method == m::WATCH_EVENT
            {
                let Message::Notification(n) = self.queue.remove(i).unwrap() else {
                    unreachable!()
                };
                return serde_json::from_value(n.params.unwrap()).unwrap();
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

    /// Assert that no watch notification arrives during a short window while
    /// preserving unrelated protocol traffic for subsequent helpers.
    async fn assert_no_watch(&mut self, window: Duration) {
        assert!(
            !self
                .queue
                .iter()
                .any(|m| matches!(m, Message::Notification(n) if n.method == m::WATCH_EVENT)),
            "unexpected queued watch event"
        );
        let deadline = tokio::time::Instant::now() + window;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline - tokio::time::Instant::now();
            match tokio::time::timeout(remaining, self.reader.next_line()).await {
                Ok(Ok(Some(line))) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let msg = Message::parse(&line).unwrap();
                    assert!(
                        !matches!(&msg, Message::Notification(n) if n.method == m::WATCH_EVENT),
                        "unexpected watch event: {msg:?}"
                    );
                    self.queue.push_back(msg);
                }
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => return,
            }
        }
    }

    async fn register(&mut self, name: &str) {
        self.call(m::PRINCIPAL_REGISTER, json!({ "name": name }))
            .await
            .unwrap();
    }
}

fn err_name(e: &RpcError) -> String {
    e.data
        .as_ref()
        .and_then(|d| d["code"].as_str())
        .unwrap_or_default()
        .to_string()
}

async fn wait_for_ack_state(admin: &mut Client, message_id: u64, expected: AckState) -> AckStatus {
    for _ in 0..50 {
        let value = admin
            .call(m::MESSAGE_STATUS, json!({ "message_id": message_id }))
            .await
            .unwrap();
        let result: MessageStatusResult = serde_json::from_value(value).unwrap();
        let ack = result.acks.into_iter().next().expect("ack row");
        if ack.state == expected {
            return ack;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("ack for message {message_id} did not reach {expected:?}");
}

#[tokio::test]
async fn register_subscribe_send_deliver_ack() {
    let (addr, _broker) = start_broker(test_cfg()).await;

    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();
    admin.call(m::WATCH_START, json!({})).await.unwrap();

    let mut alice = Client::connect_hello(addr).await;
    alice.register("@alice").await;
    alice
        .call(m::CHANNEL_CREATE, json!({ "name": "#general" }))
        .await
        .unwrap();
    alice
        .call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#general" }))
        .await
        .unwrap();

    let mut bob = Client::connect_hello(addr).await;
    bob.register("@bob").await;
    bob.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#general" }))
        .await
        .unwrap();

    let sent: SendResult = serde_json::from_value(
        alice
            .call(
                m::MESSAGE_SEND,
                json!({ "channels": ["#general"], "body": "hello bob" }),
            )
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
            .call(
                m::MESSAGE_STATUS,
                json!({ "message_id": d.envelope.message_id }),
            )
            .await
            .unwrap();
        let r: MessageStatusResult = serde_json::from_value(v).unwrap();
        assert_eq!(r.acks.len(), 1); // the admin tap is not audience
        // Wait for processed AND the relayed timestamp: the relayed write is
        // a straggler when bob's processed notification lands first (acks are
        // monotonic; the timestamp is still recorded, a beat later).
        if matches!(r.acks[0].state, AckState::Processed) && r.acks[0].relayed_at.is_some() {
            assert!(r.acks[0].held_at.is_some());
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
            WatchEvent::Ack(a) if matches!(a.state, AckState::Relayed | AckState::Processed) => {
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
    a.call(m::CHANNEL_CREATE, json!({ "name": "#sec" }))
        .await
        .unwrap();

    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;
    b.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#sec" }))
        .await
        .unwrap();

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
        a.call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#sec"], "principals": ["@c"], "body": "y" }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(sent.delivery.delivered.is_empty());
    assert_eq!(
        sent.delivery.empty_audience,
        Some(EmptyAudience::EmptyIntersection)
    );

    // Unknown names are errors and store nothing.
    let err = a
        .call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#nope"], "body": "z" }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");

    // Disconnected recipients fail per-recipient.
    drop(c);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let sent: SendResult = serde_json::from_value(
        a.call(
            m::MESSAGE_SEND,
            json!({ "principals": ["@c"], "body": "dm" }),
        )
        .await
        .unwrap(),
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
    a.call(m::CHANNEL_CREATE, json!({ "name": "#g" }))
        .await
        .unwrap();
    a.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#g" }))
        .await
        .unwrap();

    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;

    let first: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "channels": ["#g"], "body": "m1" }))
            .await
            .unwrap(),
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
    a.call(m::MESSAGE_SEND, json!({ "channels": ["#g"], "body": "m3" }))
        .await
        .unwrap();

    // Cursor paging, newest-last.
    let page: HistoryResult = serde_json::from_value(
        b.call(
            m::HISTORY_GET,
            json!({ "scope": { "channel": "#g" }, "limit": 2 }),
        )
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
    a.call(
        m::MESSAGE_SEND,
        json!({ "principals": ["@b"], "body": "psst" }),
    )
    .await
    .unwrap();
    b.next_deliver(true).await;
    let dm: HistoryResult = serde_json::from_value(
        b.call(
            m::HISTORY_GET,
            json!({ "scope": { "dm_with": "@a" }, "limit": 10 }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(dm.records.len(), 1);
    let err = b
        .call(
            m::HISTORY_GET,
            json!({ "scope": { "dm_between": ["@a", "@b"] }, "limit": 10 }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "SCOPE_DENIED");
}

#[tokio::test]
async fn body_truncation_not_rejection() {
    let (addr, _broker) = start_broker(test_cfg()).await; // 64-byte limit
    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#g" }))
        .await
        .unwrap();
    let long = "x".repeat(200);
    let sent: SendResult = serde_json::from_value(
        a.call(m::MESSAGE_SEND, json!({ "channels": ["#g"], "body": long }))
            .await
            .unwrap(),
    )
    .unwrap();
    let hist: HistoryResult = serde_json::from_value(
        a.call(
            m::HISTORY_GET,
            json!({ "scope": { "channel": "#g" }, "limit": 10 }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    let Record::Message { envelope } = hist
        .records
        .iter()
        .find(|r| r.id() == sent.message_id)
        .unwrap()
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
    let err = imposter
        .call(m::PRINCIPAL_REGISTER, json!({ "name": "@dev" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "NAME_TAKEN");

    let err = imposter
        .call(m::PRINCIPAL_REGISTER, json!({ "name": "not-a-name" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "INVALID_NAME");

    // Deregistration frees the name.
    a.call(m::PRINCIPAL_DEREGISTER, json!({})).await.unwrap();
    imposter.register("@dev").await;

    // Admin verbs on a non-admin session are refused.
    let err = imposter
        .call(
            m::ADMIN_SUBSCRIBE,
            json!({ "principal": "@dev", "channel": "#g" }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "NOT_ADMIN");
}

#[tokio::test]
async fn channel_lifecycle_archive_delete() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();

    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#tmp" }))
        .await
        .unwrap();
    a.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#tmp" }))
        .await
        .unwrap();
    a.call(
        m::MESSAGE_SEND,
        json!({ "channels": ["#tmp"], "body": "doomed" }),
    )
    .await
    .unwrap();

    // Archive: hidden, refuses subscriptions and sends; name reserved.
    admin
        .call(m::CHANNEL_ARCHIVE, json!({ "channel": "#tmp" }))
        .await
        .unwrap();
    let err = a
        .call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#tmp"], "body": "x" }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");
    let err = a
        .call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#tmp" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");
    let err = a
        .call(m::CHANNEL_CREATE, json!({ "name": "#tmp" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "NAME_TAKEN");
    let who: WhoResult =
        serde_json::from_value(a.call(m::DIRECTORY_WHO, json!({})).await.unwrap()).unwrap();
    assert!(!who.channels.iter().any(|c| c.channel == "#tmp"));

    // Archived history is admin-only.
    let err = a
        .call(
            m::HISTORY_GET,
            json!({ "scope": { "channel": "#tmp" }, "limit": 10 }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "SCOPE_DENIED");
    let hist: HistoryResult = serde_json::from_value(
        admin
            .call(
                m::HISTORY_GET,
                json!({ "scope": { "channel": "#tmp" }, "limit": 10 }),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        hist.records
            .iter()
            .any(|r| matches!(r, Record::Message { .. }))
    );

    // Unarchive is unconditional.
    admin
        .call(m::CHANNEL_UNARCHIVE, json!({ "channel": "#tmp" }))
        .await
        .unwrap();

    // Guarded deletion: bad token refused; wrong-session token refused.
    let tok: ChannelDeleteResult = serde_json::from_value(
        admin
            .call(m::CHANNEL_DELETE, json!({ "channel": "#tmp" }))
            .await
            .unwrap(),
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
        .call(
            m::HISTORY_GET,
            json!({ "scope": { "channel": "#tmp" }, "limit": 10 }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNKNOWN_NAME");
    a.call(m::CHANNEL_CREATE, json!({ "name": "#tmp" }))
        .await
        .unwrap();
}

#[tokio::test]
async fn restart_reevaluates_held_state() {
    let dir = std::env::temp_dir().join(format!("workplace-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join(format!("restart-{}.db", protocol::PROTOCOL_VERSION));
    let _ = std::fs::remove_file(&db);

    let mut cfg = test_cfg();
    cfg.db_path = Some(db.clone());
    let (addr, broker1) = start_broker(cfg).await;

    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();

    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;

    // Send a DM to @b; @b never acknowledges → the ack row stays held.
    let sent: SendResult = serde_json::from_value(
        a.call(
            m::MESSAGE_SEND,
            json!({ "principals": ["@b"], "body": "limbo" }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    let _unanswered = b.next_deliver(false).await;

    // True restart: shut broker 1 down and drop every handle to it before
    // broker 2 opens the same database.
    admin.call(m::DAEMON_SHUTDOWN, json!({})).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(admin);
    drop(a);
    drop(b);
    drop(broker1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Broker 2 over the same database, 1s grace window; @b does not re-attach.
    let mut cfg2 = test_cfg();
    cfg2.db_path = Some(db.clone());
    cfg2.grace_window = Duration::from_secs(1);
    let (addr2, _broker2) = start_broker(cfg2).await;

    let mut admin2 = Client::connect_hello(addr2).await;
    admin2
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager2", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();

    let mut failed = false;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let v = admin2
            .call(m::MESSAGE_STATUS, json!({ "message_id": sent.message_id }))
            .await;
        if let Ok(v) = v {
            let r: MessageStatusResult = serde_json::from_value(v).unwrap();
            if let Some(ack) = r.acks.iter().find(|x| x.recipient == "@b")
                && matches!(ack.state, AckState::Failed)
            {
                assert_eq!(ack.reason.as_deref(), Some("disconnected"));
                failed = true;
                break;
            }
        }
    }
    assert!(
        failed,
        "held delivery was not failed after the grace window"
    );
    let _ = std::fs::remove_file(&db);
}

#[tokio::test]
async fn admin_overrides_beat_self_service() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();

    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#ops" }))
        .await
        .unwrap();

    // Forced subscription: the agent cannot drop it.
    admin
        .call(
            m::ADMIN_SUBSCRIBE,
            json!({ "principal": "@a", "channel": "#ops" }),
        )
        .await
        .unwrap();
    let err = a
        .call(m::CHANNEL_UNSUBSCRIBE, json!({ "channel": "#ops" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "OVERRIDE_DENIED");

    // Cancelled subscription: the agent cannot rejoin.
    admin
        .call(
            m::ADMIN_UNSUBSCRIBE,
            json!({ "principal": "@a", "channel": "#ops" }),
        )
        .await
        .unwrap();
    let err = a
        .call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#ops" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "OVERRIDE_DENIED");
    let who: WhoResult =
        serde_json::from_value(a.call(m::DIRECTORY_WHO, json!({})).await.unwrap()).unwrap();
    let ops = who.channels.iter().find(|c| c.channel == "#ops").unwrap();
    assert!(
        ops.subscribers.is_empty(),
        "cancelled agent must not be able to rejoin"
    );

    // Forcing again re-subscribes and re-pins.
    admin
        .call(
            m::ADMIN_SUBSCRIBE,
            json!({ "principal": "@a", "channel": "#ops" }),
        )
        .await
        .unwrap();
    let who: WhoResult =
        serde_json::from_value(a.call(m::DIRECTORY_WHO, json!({})).await.unwrap()).unwrap();
    let ops = who.channels.iter().find(|c| c.channel == "#ops").unwrap();
    assert_eq!(ops.subscribers, vec!["@a"]);
}

#[tokio::test]
async fn forcing_existing_member_is_still_logged() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();

    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    a.call(m::CHANNEL_CREATE, json!({ "name": "#pin" }))
        .await
        .unwrap();
    a.call(m::CHANNEL_SUBSCRIBE, json!({ "channel": "#pin" }))
        .await
        .unwrap();

    // Forcing an existing member changes policy, not membership — the
    // override must still be audited (every human override is logged).
    admin
        .call(
            m::ADMIN_SUBSCRIBE,
            json!({ "principal": "@a", "channel": "#pin" }),
        )
        .await
        .unwrap();
    let hist: HistoryResult = serde_json::from_value(
        admin
            .call(
                m::HISTORY_GET,
                json!({ "scope": { "channel": "#pin" }, "limit": 50 }),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let admin_sub_logged = hist.records.iter().any(|r| {
        matches!(
            r,
            Record::System {
                event: SystemEvent::Subscribed { principal, by_admin: true, .. },
                ..
            } if principal == "@a"
        )
    });
    assert!(
        admin_sub_logged,
        "policy-only override must produce a system record"
    );

    // And the pin is effective: self-service unsubscribe is refused.
    let err = a
        .call(m::CHANNEL_UNSUBSCRIBE, json!({ "channel": "#pin" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "OVERRIDE_DENIED");
}

#[tokio::test]
async fn duplicate_recipients_get_one_delivery() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut a = Client::connect_hello(addr).await;
    a.register("@a").await;
    let mut b = Client::connect_hello(addr).await;
    b.register("@b").await;

    // The same principal addressed twice: one recipient, one delivery.
    let sent: SendResult = serde_json::from_value(
        a.call(
            m::MESSAGE_SEND,
            json!({ "principals": ["@b", "@b"], "body": "once" }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(sent.delivery.delivered, vec!["@b"]);
    b.next_deliver(true).await;

    // No second copy arrives.
    let second = tokio::time::timeout(Duration::from_millis(500), b.next_deliver(true)).await;
    assert!(
        second.is_err(),
        "duplicate recipient must not trigger a second delivery"
    );
}

#[tokio::test]
async fn concurrent_claims_have_one_winner() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        tasks.push(tokio::spawn(async move {
            let mut c = Client::connect_hello(addr).await;
            let won = c
                .call(m::PRINCIPAL_REGISTER, json!({ "name": "@highlander" }))
                .await
                .is_ok();
            // Keep the winning session alive long enough for every racer to
            // observe the claim.
            if won {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            won
        }));
    }
    let mut winners = 0;
    for t in tasks {
        if t.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one session may claim a principal name");
}

#[tokio::test]
async fn admin_registration_requires_the_admin_token() {
    let (addr, _broker) = start_broker(test_cfg()).await;

    // A watcher (with the credential) to observe the audit trail.
    let mut watcher = Client::connect_hello(addr).await;
    watcher
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();
    watcher.call(m::WATCH_START, json!({})).await.unwrap();

    // No token → UNAUTHORIZED; wrong token → UNAUTHORIZED. The session can
    // still register as a plain agent afterwards.
    let mut agent = Client::connect_hello(addr).await;
    let err = agent
        .call(m::ADMIN_REGISTER, json!({ "name": "@sneaky" }))
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNAUTHORIZED");
    let err = agent
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@sneaky", "admin_token": "guess" }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNAUTHORIZED");
    assert!(
        !err.message.contains("guess"),
        "the supplied credential must never be echoed"
    );
    agent.register("@honest").await;

    // Both denials are audited, without the supplied value.
    let mut denials = 0;
    for _ in 0..20 {
        if let WatchEvent::Record(Record::System {
            event: SystemEvent::RegistrationDenied { name, reason },
            ..
        }) = watcher.next_watch().await
        {
            assert_eq!(name, "@sneaky");
            assert_eq!(reason, "invalid admin credential");
            denials += 1;
            if denials == 2 {
                break;
            }
        }
    }
    assert_eq!(denials, 2, "denied admin attempts must be audited");
}

#[tokio::test]
async fn auth_token_gates_hello() {
    let mut cfg = test_cfg();
    cfg.auth_token = Some("s3cret".into());
    let (addr, _broker) = start_broker(cfg).await;

    // Missing token: refused at hello.
    let mut anon = Client::connect(addr).await;
    let err = anon
        .call(
            m::SESSION_HELLO,
            json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" } }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNAUTHORIZED");
    // And nothing else works before a successful hello.
    let err = anon
        .call(m::PRINCIPAL_REGISTER, json!({ "name": "@x" }))
        .await
        .unwrap_err();
    assert_eq!(err.code, -32600);

    // Wrong token: refused.
    let mut wrong = Client::connect(addr).await;
    let err = wrong
        .call(
            m::SESSION_HELLO,
            json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" }, "auth_token": "nope" }),
        )
        .await
        .unwrap_err();
    assert_eq!(err_name(&err), "UNAUTHORIZED");

    // Correct token: full service.
    let mut ok = Client::connect(addr).await;
    ok.call(
        m::SESSION_HELLO,
        json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" }, "auth_token": "s3cret" }),
    )
    .await
    .unwrap();
    ok.register("@authed").await;
}

#[tokio::test]
async fn channel_watch_filters_exclude_other_channels_and_dms() {
    let (addr, _broker) = start_broker(test_cfg()).await;

    let mut sender = Client::connect_hello(addr).await;
    sender.register("@sender").await;
    sender
        .call(m::CHANNEL_CREATE, json!({ "name": "#wanted" }))
        .await
        .unwrap();
    sender
        .call(m::CHANNEL_CREATE, json!({ "name": "#other" }))
        .await
        .unwrap();
    let mut recipient = Client::connect_hello(addr).await;
    recipient.register("@recipient").await;

    // Start both watches only after setup, so the assertions below observe
    // exclusively the records produced by these sends.
    let mut filtered = Client::connect_hello(addr).await;
    filtered
        .call(m::WATCH_START, json!({ "channels": ["#wanted"] }))
        .await
        .unwrap();
    let mut all = Client::connect_hello(addr).await;
    all.call(m::WATCH_START, json!({})).await.unwrap();

    sender
        .call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#wanted"], "body": "visible" }),
        )
        .await
        .unwrap();
    let WatchEvent::Record(Record::Message { envelope }) = filtered.next_watch().await else {
        panic!("filtered watcher should receive the selected channel")
    };
    assert_eq!(envelope.body, "visible");
    let _ = all.next_watch().await;

    sender
        .call(
            m::MESSAGE_SEND,
            json!({ "channels": ["#other"], "body": "hidden-channel" }),
        )
        .await
        .unwrap();
    let WatchEvent::Record(Record::Message { envelope }) = all.next_watch().await else {
        panic!("unfiltered watcher should receive the other channel")
    };
    assert_eq!(envelope.body, "hidden-channel");

    sender
        .call(
            m::MESSAGE_SEND,
            json!({ "principals": ["@recipient"], "body": "hidden-dm" }),
        )
        .await
        .unwrap();
    let WatchEvent::Record(Record::Message { envelope }) = all.next_watch().await else {
        panic!("unfiltered watcher should receive DMs")
    };
    assert_eq!(envelope.body, "hidden-dm");
    filtered.assert_no_watch(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn only_bound_recipient_can_report_processed() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();
    let mut sender = Client::connect_hello(addr).await;
    sender.register("@sender").await;
    let mut recipient = Client::connect_hello(addr).await;
    recipient.register("@recipient").await;
    let mut imposter = Client::connect_hello(addr).await;
    imposter.register("@imposter").await;

    let sent: SendResult = serde_json::from_value(
        sender
            .call(
                m::MESSAGE_SEND,
                json!({ "principals": ["@recipient"], "body": "guard me" }),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    recipient.next_deliver(true).await;
    wait_for_ack_state(&mut admin, sent.message_id, AckState::Relayed).await;

    // A different bound session cannot advance @recipient's acknowledgment.
    imposter
        .write(Message::Notification(notification(
            m::MESSAGE_PROCESSED,
            json!({ "message_id": sent.message_id, "recipient": "@recipient" }),
        )))
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let ack = wait_for_ack_state(&mut admin, sent.message_id, AckState::Relayed).await;
    assert!(ack.processed_at.is_none());

    recipient
        .write(Message::Notification(notification(
            m::MESSAGE_PROCESSED,
            json!({ "message_id": sent.message_id, "recipient": "@recipient" }),
        )))
        .await;
    wait_for_ack_state(&mut admin, sent.message_id, AckState::Processed).await;
}

#[tokio::test]
async fn rename_works_for_live_and_archived_channels() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();
    let mut agent = Client::connect_hello(addr).await;
    agent.register("@agent").await;
    agent
        .call(m::CHANNEL_CREATE, json!({ "name": "#old" }))
        .await
        .unwrap();

    admin
        .call(
            m::CHANNEL_RENAME,
            json!({ "channel": "#old", "new_name": "#live" }),
        )
        .await
        .unwrap();
    let who: WhoResult =
        serde_json::from_value(agent.call(m::DIRECTORY_WHO, json!({})).await.unwrap()).unwrap();
    assert!(who.channels.iter().any(|c| c.channel == "#live"));
    assert!(!who.channels.iter().any(|c| c.channel == "#old"));

    admin
        .call(m::CHANNEL_ARCHIVE, json!({ "channel": "#live" }))
        .await
        .unwrap();
    admin
        .call(
            m::CHANNEL_RENAME,
            json!({ "channel": "#live", "new_name": "#archived" }),
        )
        .await
        .unwrap();

    // Renaming an archived channel frees its former name while preserving
    // the archived channel and its history under the new name.
    agent
        .call(m::CHANNEL_CREATE, json!({ "name": "#live" }))
        .await
        .unwrap();
    let history: HistoryResult = serde_json::from_value(
        admin
            .call(
                m::HISTORY_GET,
                json!({ "scope": { "channel": "#archived" }, "limit": 20 }),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(history.records.iter().any(|record| matches!(
        record,
        Record::System {
            event: SystemEvent::ChannelRenamed { old_name, new_name, .. },
            ..
        } if old_name == "#live" && new_name == "#archived"
    )));
}

#[tokio::test]
async fn rpc_guards_status_and_watch_stop() {
    let (addr, _broker) = start_broker(test_cfg()).await;

    let mut observer = Client::connect(addr).await;
    let err = observer
        .call(m::DIRECTORY_WHO, json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code, -32600);
    assert!(err.message.contains("session/hello"));
    observer
        .call(
            m::SESSION_HELLO,
            json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" } }),
        )
        .await
        .unwrap();
    let err = observer
        .call("no/such-method", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code, -32601);

    observer.call(m::WATCH_START, json!({})).await.unwrap();
    observer.call(m::WATCH_STOP, json!({})).await.unwrap();
    let mut agent = Client::connect_hello(addr).await;
    agent.register("@agent").await;
    observer.assert_no_watch(Duration::from_millis(200)).await;

    let err = agent.call(m::DAEMON_STATUS, json!({})).await.unwrap_err();
    assert_eq!(err_name(&err), "NOT_ADMIN");
    let mut admin = Client::connect_hello(addr).await;
    admin
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": "@manager", "admin_token": "test-admin" }),
        )
        .await
        .unwrap();
    let status: DaemonStatusResult =
        serde_json::from_value(admin.call(m::DAEMON_STATUS, json!({})).await.unwrap()).unwrap();
    assert_eq!(status.broker_version, "test");
    assert_eq!(status.principal_count, 2);
    assert_eq!(status.channel_count, 0);
    assert!(
        status
            .sessions
            .iter()
            .any(|s| s.principal.as_deref() == Some("@manager") && s.admin)
    );
    assert!(
        status
            .sessions
            .iter()
            .any(|s| s.principal.as_deref() == Some("@agent") && !s.admin)
    );
}

#[tokio::test]
async fn jsonrpc_violations_get_spec_errors() {
    let (addr, _broker) = start_broker(test_cfg()).await;
    let mut c = Client::connect_hello(addr).await;

    // Unparseable line → -32700 with a null id.
    c.writer.write_all(b"this is not json\n").await.unwrap();
    let msg = c.read_socket().await;
    let Message::Response(resp) = msg else {
        panic!("expected a parse-error response")
    };
    assert_eq!(resp.id, Id::Null);
    assert_eq!(resp.error.unwrap().code, -32700);

    // Wrong jsonrpc version → -32600, request not dispatched.
    c.writer
        .write_all(b"{\"jsonrpc\":\"1.0\",\"id\":77,\"method\":\"directory/who\",\"params\":{}}\n")
        .await
        .unwrap();
    let msg = c.read_socket().await;
    let Message::Response(resp) = msg else {
        panic!("expected an invalid-request response")
    };
    assert_eq!(resp.id, Id::Num(77));
    assert_eq!(resp.error.unwrap().code, -32600);
}

#[tokio::test]
async fn slow_reader_overflow_disconnects() {
    // The overflow guard trips when messages queue IN the broker, which
    // requires the writer to be blocked on full kernel socket buffers. Linux
    // loopback buffers auto-tune to several MB, so tiny events never build
    // backpressure there: use a small queue bound plus quarter-megabyte
    // events so the flood (~37MB) dwarfs any platform's buffering.
    let mut cfg = test_cfg();
    cfg.message_size_limit = 512 * 1024;
    cfg.max_out_queue = 64;
    let (addr, _broker) = start_broker(cfg).await;

    // A watcher that subscribes to everything and then never reads.
    let mut watcher = Client::connect_hello(addr).await;
    watcher.call(m::WATCH_START, json!({})).await.unwrap();

    let mut sender = Client::connect_hello(addr).await;
    sender.register("@spammer").await;
    sender
        .call(m::CHANNEL_CREATE, json!({ "name": "#flood" }))
        .await
        .unwrap();

    let body = "x".repeat(256 * 1024);
    for _ in 0..150 {
        sender
            .call(
                m::MESSAGE_SEND,
                json!({ "channels": ["#flood"], "body": body }),
            )
            .await
            .unwrap();
    }

    // The broker must tear the watcher down (bounded memory), closing its
    // socket after the overflow. Drain whatever was buffered, then expect
    // EOF or a reset.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut closed = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), watcher.reader.next_line()).await {
            Ok(Ok(Some(_))) => continue, // buffered backlog
            Ok(Ok(None)) | Ok(Err(_)) => {
                closed = true; // EOF or reset: connection torn down
                break;
            }
            Err(_) => break, // no data and no close: would mean still attached
        }
    }
    assert!(
        closed,
        "slow reader must be disconnected after outbound overflow"
    );
}

#[tokio::test]
async fn oversized_lines_close_the_connection() {
    let (addr, _broker) = start_broker(test_cfg()).await; // 64-byte body limit
    let mut c = Client::connect_hello(addr).await;

    // Way past limit + envelope slack: refused and disconnected, bounding
    // per-connection transport memory.
    let huge = format!("{}\n", "x".repeat(64 + 64 * 1024 + 100));
    c.writer.write_all(huge.as_bytes()).await.unwrap();
    let msg = c.read_socket().await;
    let Message::Response(resp) = msg else {
        panic!("expected an oversized-line response")
    };
    assert_eq!(resp.id, Id::Null);
    assert_eq!(resp.error.unwrap().code, -32600);
    let closed = tokio::time::timeout(T, c.reader.next_line())
        .await
        .expect("read timeout");
    assert!(
        matches!(closed, Ok(None)),
        "connection should be closed after an oversized line"
    );
}
