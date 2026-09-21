//! Layer-1 fix: the adapter must release the `thread/resume` subscription it
//! acquires. `thread/resume` implicitly subscribes this connection to the
//! thread; a persistent attach that never unsubscribes pins every thread it
//! resumes loaded forever, blocking Codex's no-subscriber idle-unload — a
//! permanent bus-presence ghost.
//!
//! These pin the cleanup contract: release ONLY when a resume here may have
//! taken the subscription (Ok or an ambiguous transport timeout — not a
//! definitive RPC rejection), exactly once, after the commit point and before
//! completion polling, and as a best-effort step that never rewrites a
//! committed delivery outcome — across both the turn/start and turn/steer
//! commit paths.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use adapter_codex::{CodexAttach, Delivered};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message as WsMessage;

#[derive(Clone, Copy, Default)]
struct Script {
    /// First `turn/start` returns an unload error, forcing a `thread/resume`.
    unload_first_turn_start: bool,
    /// First `thread/read` returns an unload error, forcing a `thread/resume`
    /// (used to reach the busy → turn/steer path with a live subscription).
    unload_first_read: bool,
    /// After a resume, `thread/read` reports the thread busy so delivery
    /// commits via `turn/steer` instead of `turn/start`.
    busy_steer: bool,
    /// The post-resume `turn/start` returns "no rollout" — a terminal
    /// pre-commit failure that happens *after* the subscription was acquired.
    second_turn_start_gone: bool,
    /// `thread/resume` itself fails — the subscription is never acquired.
    resume_fails: bool,
    /// `thread/unsubscribe` returns an RPC error (the best-effort cleanup path).
    unsubscribe_errors: bool,
}

/// A fake app-server that records the ordered method calls it receives and
/// responds per `script`. A `thread/unsubscribe` for the wrong thread id is
/// recorded distinctly so a malformed cleanup cannot pass the count asserts.
/// Returns `(ws url, shared ordered call log)`.
async fn fake_app_server(script: Script) -> (String, Arc<Mutex<Vec<String>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = calls.clone();
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
        let mut turn_starts = 0usize;
        let mut resumed_seen = false;
        let mut committed_turn: Option<&'static str> = None;
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
            let recorded = if method == "thread/unsubscribe" {
                match v.pointer("/params/threadId").and_then(Value::as_str) {
                    Some("th-1") => "thread/unsubscribe".to_string(),
                    other => format!("thread/unsubscribe(bad-thread-id:{other:?})"),
                }
            } else {
                method.to_string()
            };
            calls_srv.lock().unwrap().push(recorded);

            // Ok(result) or Err(rpc error message) for this method.
            let outcome: Result<Value, String> = match method {
                "initialize" => Ok(json!({ "ok": true })),
                "thread/read" => {
                    reads += 1;
                    if script.unload_first_read && reads == 1 {
                        Err("thread unloaded".into())
                    } else if let Some(turn) = committed_turn {
                        Ok(json!({ "thread": { "status": { "type": "idle" },
                            "turns": [{ "id": turn, "status": "completed" }] } }))
                    } else if script.busy_steer && resumed_seen {
                        Ok(json!({ "thread": { "status": { "type": "active" },
                            "turns": [{ "id": "turn-steer", "status": "inProgress" }] } }))
                    } else {
                        Ok(json!({ "thread": { "status": { "type": "idle" }, "turns": [] } }))
                    }
                }
                "turn/start" => {
                    turn_starts += 1;
                    if turn_starts == 1 && script.unload_first_turn_start {
                        Err("thread unloaded".into())
                    } else if turn_starts >= 2 && script.second_turn_start_gone {
                        Err("no rollout found for thread".into())
                    } else {
                        committed_turn = Some("turn-1");
                        Ok(json!({ "turn": { "id": "turn-1", "status": "inProgress" } }))
                    }
                }
                "turn/steer" => {
                    committed_turn = Some("turn-steer");
                    Ok(json!({ "turnId": "turn-steer" }))
                }
                "thread/resume" => {
                    if script.resume_fails {
                        Err("no rollout found for thread".into())
                    } else {
                        resumed_seen = true;
                        Ok(json!({}))
                    }
                }
                "thread/unsubscribe" => {
                    if script.unsubscribe_errors {
                        Err("unsubscribe rejected".into())
                    } else {
                        Ok(json!({}))
                    }
                }
                _ => Ok(json!({})),
            };
            let response = match outcome {
                Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                Err(message) => json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32603, "message": message },
                }),
            };
            if sink
                .send(WsMessage::Text(response.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    (format!("ws://{addr}"), calls)
}

/// A fake that forces a `thread/resume` (turn/start always reports unload),
/// signals once it has received the resume, and then *withholds* the response
/// while keeping the socket open — so the client's resume call times out on a
/// still-live connection. Everything else (including the follow-up
/// `thread/unsubscribe`) is answered normally.
async fn fake_withholding_resume() -> (String, Arc<Mutex<Vec<String>>>, oneshot::Receiver<()>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = calls.clone();
    let (resume_tx, resume_rx) = oneshot::channel();
    let mut resume_tx = Some(resume_tx);
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
            calls_srv.lock().unwrap().push(method.to_string());
            if method == "thread/resume" {
                if let Some(tx) = resume_tx.take() {
                    let _ = tx.send(());
                }
                continue; // withhold the response; keep the socket open
            }
            // initialize / thread/read / thread/unsubscribe: `{}` reads as idle
            // / ok; turn/start reports unload to force the resume.
            let outcome: Result<Value, String> = match method {
                "turn/start" => Err("thread unloaded".into()),
                _ => Ok(json!({})),
            };
            let response = match outcome {
                Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                Err(message) => json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32603, "message": message },
                }),
            };
            if sink
                .send(WsMessage::Text(response.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    (format!("ws://{addr}"), calls, resume_rx)
}

fn count(calls: &Arc<Mutex<Vec<String>>>, method: &str) -> usize {
    calls
        .lock()
        .unwrap()
        .iter()
        .filter(|m| m.as_str() == method)
        .count()
}

/// Happy resume path (turn/start commit): the subscription is released exactly
/// once, AFTER the committing turn/start and BEFORE the completion poll, and
/// the delivery still reaches Processed.
#[tokio::test]
async fn resume_delivery_releases_subscription_after_commit_before_poll() {
    let (url, calls) = fake_app_server(Script {
        unload_first_turn_start: true,
        ..Default::default()
    })
    .await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    assert_eq!(attach.deliver("th-1", "hello").await, Delivered::Processed);

    let log = calls.lock().unwrap().clone();
    assert_eq!(count(&calls, "thread/resume"), 1, "resume happened once");
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        1,
        "the acquired subscription is released exactly once for the right thread"
    );
    let commit = log.iter().rposition(|m| m == "turn/start").unwrap();
    let unsub = log.iter().position(|m| m == "thread/unsubscribe").unwrap();
    let last_read = log.iter().rposition(|m| m == "thread/read").unwrap();
    assert!(
        commit < unsub,
        "unsubscribe must follow the committed turn/start: {log:?}"
    );
    assert!(
        unsub < last_read,
        "unsubscribe must precede the completion poll (final thread/read): {log:?}"
    );
}

/// Steer commit path: a delivery that resumes and then commits via turn/steer
/// (busy thread) also releases the subscription exactly once, after the steer
/// and before the poll.
#[tokio::test]
async fn steer_commit_after_resume_releases_subscription() {
    let (url, calls) = fake_app_server(Script {
        unload_first_read: true,
        busy_steer: true,
        ..Default::default()
    })
    .await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    assert_eq!(attach.deliver("th-1", "hello").await, Delivered::Processed);

    let log = calls.lock().unwrap().clone();
    assert!(
        log.iter().any(|m| m == "turn/steer"),
        "delivery committed via turn/steer: {log:?}"
    );
    assert_eq!(count(&calls, "thread/resume"), 1);
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        1,
        "a steer-committed delivery also releases the resume subscription once"
    );
    let commit = log.iter().rposition(|m| m == "turn/steer").unwrap();
    let unsub = log.iter().position(|m| m == "thread/unsubscribe").unwrap();
    let last_read = log.iter().rposition(|m| m == "thread/read").unwrap();
    assert!(
        commit < unsub && unsub < last_read,
        "unsubscribe lands after the steer commit and before the poll: {log:?}"
    );
}

/// A best-effort unsubscribe failure after commit must never rewrite a
/// committed delivery: polling still runs and the result stays Processed.
#[tokio::test]
async fn unsubscribe_failure_does_not_rewrite_committed_delivery() {
    let (url, calls) = fake_app_server(Script {
        unload_first_turn_start: true,
        unsubscribe_errors: true,
        ..Default::default()
    })
    .await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    assert_eq!(
        attach.deliver("th-1", "hello").await,
        Delivered::Processed,
        "a failed best-effort unsubscribe must not override a committed, processed delivery"
    );
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        1,
        "unsubscribe is attempted exactly once even though it errors"
    );
}

/// A pre-commit terminal failure that happens after a successful resume still
/// releases the subscription exactly once, and the original failure
/// classification is preserved.
#[tokio::test]
async fn precommit_failure_after_resume_unsubscribes_once_and_preserves_failure() {
    let (url, calls) = fake_app_server(Script {
        unload_first_turn_start: true,
        second_turn_start_gone: true,
        ..Default::default()
    })
    .await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    match attach.deliver("th-1", "hello").await {
        Delivered::Failed(reason) => assert!(
            reason.contains("thread gone"),
            "the pre-commit failure classification must survive cleanup: {reason}"
        ),
        other => panic!("expected the pre-commit failure to be preserved, got {other:?}"),
    }
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        1,
        "cleanup is attempted exactly once after a successful resume"
    );
}

/// The idle happy path never resumes, so it must never unsubscribe a
/// subscription it did not acquire.
#[tokio::test]
async fn no_resume_means_no_unsubscribe() {
    let (url, calls) = fake_app_server(Script::default()).await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    assert_eq!(attach.deliver("th-1", "hello").await, Delivered::Processed);
    assert_eq!(
        count(&calls, "thread/resume"),
        0,
        "no resume on the idle path"
    );
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        0,
        "must not unsubscribe a subscription that was never acquired"
    );
}

/// A resume that is definitively rejected (RPC error) acquires no subscription,
/// so there is nothing to release — and the delivery is terminally Failed.
#[tokio::test]
async fn rejected_resume_does_not_unsubscribe() {
    let (url, calls) = fake_app_server(Script {
        unload_first_turn_start: true,
        resume_fails: true,
        ..Default::default()
    })
    .await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    match attach.deliver("th-1", "hello").await {
        Delivered::Failed(_) => {}
        other => panic!("a rejected resume is terminal Failed, got {other:?}"),
    }
    assert_eq!(count(&calls, "thread/resume"), 1, "resume was attempted");
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        0,
        "a definitively rejected resume acquires no subscription to release"
    );
}

/// The ambiguous case: a resume whose response times out on a still-live
/// socket may have subscribed, so cleanup must still be attempted — while the
/// terminal transport failure is preserved. Uses Tokio's paused clock so the
/// 30s call-timeout fires without a wall-clock wait.
#[tokio::test]
async fn resume_timeout_still_attempts_best_effort_unsubscribe() {
    let (url, calls, resume_rx) = fake_withholding_resume().await;
    let attach = CodexAttach::connect(&url, None).await.unwrap();
    let handle = tokio::spawn(async move { attach.deliver("th-1", "hello").await });

    // The pre-resume calls resolve in real time; wait until the fake holds the
    // resume request and is withholding its response.
    resume_rx.await.unwrap();
    // Only the resume call-timeout is pending now: advance virtual time past it
    // to fire the timeout, then return to real time so the follow-up
    // best-effort unsubscribe completes over real IO instead of auto-advancing
    // its own timeout before the fake can answer.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();

    match handle.await.unwrap() {
        Delivered::Failed(reason) => assert!(
            reason.contains("transport failure during resume"),
            "the resume timeout must be a terminal transport failure: {reason}"
        ),
        other => panic!("expected terminal Failed from the resume timeout, got {other:?}"),
    }
    assert_eq!(
        count(&calls, "thread/unsubscribe"),
        1,
        "an ambiguous resume timeout must still attempt best-effort cleanup"
    );
}
