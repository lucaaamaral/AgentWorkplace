//! Shim tests: the MCP side is driven through in-memory pipes
//! (`run_with_io`), the broker side is a fake TCP listener. Covers the
//! delivery-path honesty rules: a registration without a working push path
//! must warn at register time and FAIL deliveries, never ack `relayed` for
//! a notification the harness will discard.

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

struct Harness {
    mcp_in: DuplexStream,
    mcp_out: tokio::io::Lines<BufReader<DuplexStream>>,
    broker_in: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    broker_out: OwnedWriteHalf,
    next_mcp_id: u64,
}

const T: std::time::Duration = std::time::Duration::from_secs(5);

impl Harness {
    /// Boot a shim against a fake broker and answer its session/hello.
    async fn start(codex_app_server: Option<&str>) -> Harness {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (mcp_in_test, mcp_in_shim) = tokio::io::duplex(1 << 16);
        let (mcp_out_shim, mcp_out_test) = tokio::io::duplex(1 << 16);
        let cfg = shim_claude::ShimConfig {
            broker_addr: addr.to_string(),
            version: "test".into(),
            codex_app_server: codex_app_server.map(String::from),
            auth_token: None,
        };
        tokio::spawn(shim_claude::run_with_io(cfg, mcp_in_shim, mcp_out_shim));
        let (stream, _) = listener.accept().await.unwrap();
        let (r, w) = stream.into_split();
        let mut h = Harness {
            mcp_in: mcp_in_test,
            mcp_out: BufReader::new(mcp_out_test).lines(),
            broker_in: BufReader::new(r).lines(),
            broker_out: w,
            next_mcp_id: 1,
        };
        let hello = h.broker_next().await;
        assert_eq!(hello["method"], "session/hello");
        h.broker_respond(&hello, json!({ "broker_version": "t", "session_id": 1 }))
            .await;
        h
    }

    async fn broker_next(&mut self) -> Value {
        loop {
            let line = tokio::time::timeout(T, self.broker_in.next_line())
                .await
                .expect("broker read timeout")
                .unwrap()
                .expect("shim closed broker connection");
            if line.trim().is_empty() {
                continue;
            }
            return serde_json::from_str(&line).unwrap();
        }
    }

    async fn broker_respond(&mut self, req: &Value, result: Value) {
        let resp = json!({ "jsonrpc": "2.0", "id": req["id"], "result": result });
        self.broker_out
            .write_all(format!("{resp}\n").as_bytes())
            .await
            .unwrap();
    }

    /// Send a broker-originated request (a delivery) and return the shim's
    /// response to it.
    async fn broker_request(&mut self, id: u64, method: &str, params: Value) -> Value {
        let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.broker_out
            .write_all(format!("{req}\n").as_bytes())
            .await
            .unwrap();
        loop {
            let v = self.broker_next().await;
            if v.get("method").is_none() && v["id"] == json!(id) {
                return v;
            }
        }
    }

    async fn mcp_send(&mut self, method: &str, params: Value) -> u64 {
        let id = self.next_mcp_id;
        self.next_mcp_id += 1;
        let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.mcp_in
            .write_all(format!("{req}\n").as_bytes())
            .await
            .unwrap();
        id
    }

    async fn mcp_read(&mut self, id: u64) -> Value {
        loop {
            let line = tokio::time::timeout(T, self.mcp_out.next_line())
                .await
                .expect("mcp read timeout")
                .unwrap()
                .expect("shim closed mcp output");
            let v: Value = serde_json::from_str(&line).unwrap();
            if v["id"] == json!(id) {
                return v;
            }
        }
    }

    async fn mcp_next_notification(&mut self) -> Value {
        loop {
            let line = tokio::time::timeout(T, self.mcp_out.next_line())
                .await
                .expect("mcp read timeout")
                .unwrap()
                .expect("shim closed mcp output");
            let v: Value = serde_json::from_str(&line).unwrap();
            if v.get("method").is_some() && v.get("id").is_none() {
                return v;
            }
        }
    }

    /// Drive a register tool call end-to-end: MCP request, broker roundtrip,
    /// MCP response. Returns (broker register params, tool result text).
    async fn register(&mut self, args: Value) -> (Value, String) {
        let id = self
            .mcp_send(
                "tools/call",
                json!({ "name": "register", "arguments": args }),
            )
            .await;
        let reg = self.broker_next().await;
        assert_eq!(reg["method"], "principal/register");
        let params = reg["params"].clone();
        self.broker_respond(&reg, json!({ "principal": "@x" }))
            .await;
        let resp = self.mcp_read(id).await;
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        (params, text)
    }
}

fn deliver_params(body: &str) -> Value {
    json!({
        "recipient": "@x",
        "envelope": {
            "message_id": 7, "thread_id": 7, "timestamp": 0, "sender": "@a",
            "recipients": { "channels": [], "principals": ["@x"] },
            "body": body, "truncated": false
        }
    })
}

#[tokio::test]
async fn claude_session_relays_deliveries() {
    let mut h = Harness::start(None).await;
    let (params, text) = h.register(json!({ "name": "@x" })).await;
    assert!(params.get("codex").is_none());
    assert!(
        !text.contains("WARNING"),
        "plain registration must not warn: {text}"
    );

    let resp = h
        .broker_request(500, "message/deliver", deliver_params("hi"))
        .await;
    assert_eq!(resp["result"]["status"], "relayed");
    let notif = h.mcp_next_notification().await;
    assert_eq!(notif["method"], "notifications/claude/channel");
    assert!(
        notif["params"]["content"].as_str().unwrap().contains("hi"),
        "channel event must carry the body"
    );
}

#[tokio::test]
async fn codex_registration_with_both_halves_attaches_coordinates() {
    let mut h = Harness::start(Some("ws://127.0.0.1:9701")).await;
    let (params, text) = h
        .register(json!({ "name": "@x", "thread_id": "th-1" }))
        .await;
    assert_eq!(params["codex"]["thread_id"], "th-1");
    assert_eq!(params["codex"]["app_server"], "ws://127.0.0.1:9701");
    assert!(
        !text.contains("WARNING"),
        "coordinate-bearing registration must not warn"
    );
}

#[tokio::test]
async fn codex_shim_without_thread_id_warns_and_fails_delivery() {
    let mut h = Harness::start(Some("ws://127.0.0.1:9701")).await;
    let (params, text) = h.register(json!({ "name": "@x" })).await;
    assert!(params.get("codex").is_none());
    assert!(
        text.contains("WARNING") && text.contains("CODEX_THREAD_ID"),
        "register result must instruct the fix: {text}"
    );

    // Deliveries must FAIL (honest ack), never relay into a discarding harness.
    let resp = h
        .broker_request(501, "message/deliver", deliver_params("lost?"))
        .await;
    let err = resp["error"]["message"].as_str().unwrap();
    assert!(
        err.contains("no push path"),
        "failure must name the cause: {err}"
    );
}

#[tokio::test]
async fn flagless_shim_ignoring_thread_id_warns_and_fails_delivery() {
    let mut h = Harness::start(None).await;
    let (params, text) = h
        .register(json!({ "name": "@x", "thread_id": "th-1" }))
        .await;
    assert!(params.get("codex").is_none(), "no endpoint, no coordinates");
    assert!(
        text.contains("WARNING") && text.contains("--codex-app-server"),
        "register result must name the missing flag: {text}"
    );

    let resp = h
        .broker_request(502, "message/deliver", deliver_params("lost?"))
        .await;
    let err = resp["error"]["message"].as_str().unwrap();
    assert!(
        err.contains("no push path"),
        "failure must name the cause: {err}"
    );
}
