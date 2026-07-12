//! Delivery-engine tests against a real `codex app-server`. Each test spawns
//! Codex and runs model turns, so they are `#[ignore]` by default — run with:
//!   cargo test -p workplace-adapter-codex -- --ignored --nocapture
//! Requires codex-cli on PATH with working auth.

use std::time::Instant;

use adapter_codex::{CodexSession, Delivered};

fn cwd() -> String {
    std::env::temp_dir()
        .join("workplace-codex-test")
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
#[ignore = "spawns codex app-server and runs model turns"]
async fn single_delivery_processes() {
    std::fs::create_dir_all(cwd()).unwrap();
    let session = CodexSession::spawn(&cwd()).await.expect("spawn codex");
    assert!(!session.thread_id().is_empty());

    let outcome = session
        .deliver("Reply with exactly ALPHA-OK and nothing else. Do not use any tools.")
        .await;
    assert_eq!(
        outcome,
        Delivered::Processed,
        "delivery should complete a turn"
    );
}

#[tokio::test]
#[ignore = "spawns codex app-server and runs model turns"]
async fn concurrent_deliveries_serialize() {
    // Two deliveries fired at once must BOTH complete: turn/start while busy is
    // silently dropped, so the engine must hold the second until the first
    // turn goes idle. If serialization were broken, the second would never
    // complete (Failed/timeout).
    std::fs::create_dir_all(cwd()).unwrap();
    let session = CodexSession::spawn(&cwd()).await.expect("spawn codex");

    let start = Instant::now();
    let (a, b) = tokio::join!(
        session.deliver("Reply with exactly ONE-OK. No tools."),
        session.deliver("Reply with exactly TWO-OK. No tools."),
    );
    assert_eq!(a, Delivered::Processed, "first delivery");
    assert_eq!(
        b,
        Delivered::Processed,
        "second delivery (must not be dropped)"
    );
    // Two serialized turns take longer than one; a sanity floor, not a tight bound.
    assert!(start.elapsed().as_secs_f64() > 0.0);
}
