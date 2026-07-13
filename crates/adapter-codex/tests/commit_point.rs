//! C1 — characterization of the turn/start COMMIT POINT (attach.rs).
//!
//! Once turn/start has been sent, a transport loss no longer proves the turn
//! did not run: retrying would inject the message twice. These tests pin the
//! boundary: transport drops BEFORE turn/start are retriable (Unreachable);
//! drops AT or AFTER turn/start are terminal (Failed, "completion unknown")
//! and must never come back as Unreachable, which the broker would retry.

use adapter_codex::{CodexAttach, Delivered};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Which client call the fake app-server hangs up on (closing the socket
/// without responding).
#[derive(Clone, Copy, PartialEq)]
enum DropAt {
    /// The idle check before delivery — nothing injected yet.
    FirstRead,
    /// The turn/start request itself — the ambiguous commit point.
    TurnStart,
    /// The completion poll after turn/start returned a turn id.
    CompletionPoll,
}

async fn dropping_app_server(drop_at: DropAt) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        let (mut sink, mut source) = ws.split();
        let mut reads = 0usize;
        let mut turn_started = false;
        while let Some(Ok(msg)) = source.next().await {
            let WsMessage::Text(text) = msg else { continue };
            let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else {
                continue;
            };
            let (Some(id), Some(method)) = (
                v.get("id").cloned(),
                v.get("method").and_then(Value::as_str),
            ) else {
                continue;
            };
            let result = match method {
                "initialize" => json!({ "ok": true }),
                "thread/read" => {
                    reads += 1;
                    if drop_at == DropAt::FirstRead && reads == 1 {
                        return; // close without responding
                    }
                    if drop_at == DropAt::CompletionPoll && turn_started {
                        return; // close during the completion poll
                    }
                    json!({ "thread": { "status": { "type": "idle" }, "turns": [] } })
                }
                "turn/start" => {
                    if drop_at == DropAt::TurnStart {
                        return; // close exactly at the commit point
                    }
                    turn_started = true;
                    json!({ "turn": { "id": "turn-1", "status": "inProgress" } })
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
    format!("ws://{addr}")
}

#[tokio::test]
async fn drop_before_turn_start_is_retriable() {
    let url = dropping_app_server(DropAt::FirstRead).await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    match attach.deliver("th-1", "hello").await {
        Delivered::Unreachable(_) => {} // nothing injected: the broker may retry
        other => panic!("pre-commit transport drop must be Unreachable, got {other:?}"),
    }
}

#[tokio::test]
async fn drop_at_turn_start_is_terminal_completion_unknown() {
    let url = dropping_app_server(DropAt::TurnStart).await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    match attach.deliver("th-1", "hello").await {
        Delivered::Failed(reason) => {
            assert!(
                reason.contains("completion unknown"),
                "terminal reason must state the ambiguity: {reason}"
            );
        }
        other => panic!(
            "transport drop AT turn/start must be terminal Failed (a retry could inject twice), got {other:?}"
        ),
    }
}

#[tokio::test]
async fn drop_during_completion_poll_is_terminal_completion_unknown() {
    let url = dropping_app_server(DropAt::CompletionPoll).await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    match attach.deliver("th-1", "hello").await {
        Delivered::Failed(reason) => {
            assert!(
                reason.contains("completion unknown"),
                "terminal reason must state the ambiguity: {reason}"
            );
        }
        other => panic!("transport drop after turn/start must be terminal Failed, got {other:?}"),
    }
}
