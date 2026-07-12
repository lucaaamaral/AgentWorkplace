//! Attach-mode test against a real shared `codex app-server --listen ws://`.
//! One connection plays the human (owns the thread); a second CodexAttach is
//! the adapter (injects a delivery and polls for processed). `#[ignore]` by
//! default — run with: cargo test -p workplace-adapter-codex -- --ignored

use std::process::Stdio;
use std::time::Duration;

use adapter_codex::{CodexAttach, Delivered};

const WS: &str = "ws://127.0.0.1:9711";

#[tokio::test]
#[ignore = "spawns a shared codex app-server and runs a model turn"]
async fn attach_delivers_into_owned_thread() {
    let mut server = tokio::process::Command::new("codex")
        .args(["app-server", "--listen", "ws://127.0.0.1:9711"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn codex app-server --listen");

    // Wait for the listener.
    let mut owner = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(c) = CodexAttach::connect(WS, None).await {
            owner = Some(c);
            break;
        }
    }
    let owner = owner.expect("connect owner client");

    // The "human" owns the thread.
    let thread_id = owner
        .start_thread("/tmp")
        .await
        .expect("owner starts thread");

    // The adapter attaches separately and delivers into the human's thread.
    let adapter = CodexAttach::connect(WS, None)
        .await
        .expect("connect adapter client");
    let outcome = adapter
        .deliver(
            &thread_id,
            "Reply with exactly ATTACH-OK and nothing else. Do not use any tools.",
        )
        .await;
    assert_eq!(
        outcome,
        Delivered::Processed,
        "attached delivery should reach processed via polling"
    );

    let _ = server.kill().await;
}
