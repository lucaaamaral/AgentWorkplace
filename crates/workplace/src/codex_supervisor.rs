//! Durable supervision for the shared Codex app-server endpoint.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const READY_PATH: &str = "/readyz";

#[derive(Clone, Copy)]
struct Timings {
    poll: Duration,
    probe_timeout: Duration,
    backoff_initial: Duration,
    backoff_max: Duration,
    stable_uptime: Duration,
    absent_debounce: usize,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            poll: Duration::from_secs(2),
            probe_timeout: Duration::from_secs(2),
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
            stable_uptime: Duration::from_secs(30),
            absent_debounce: 2,
        }
    }
}

#[derive(Clone)]
struct Endpoint {
    url: String,
    authority: String,
    connect_addr: String,
}

impl Endpoint {
    fn parse(url: String) -> anyhow::Result<Self> {
        let rest = url
            .strip_prefix("ws://")
            .ok_or_else(|| anyhow::anyhow!("codex app-server endpoint must use ws://"))?;
        let authority = rest.split('/').next().unwrap_or_default().to_string();
        if authority.is_empty() {
            anyhow::bail!("codex app-server endpoint has no authority");
        }
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (host, suffix) = v6
                .split_once(']')
                .ok_or_else(|| anyhow::anyhow!("invalid IPv6 app-server endpoint"))?;
            let port = suffix
                .strip_prefix(':')
                .ok_or_else(|| anyhow::anyhow!("app-server endpoint has no port"))?;
            (host.to_string(), port.parse::<u16>()?)
        } else {
            let (host, port) = authority
                .rsplit_once(':')
                .ok_or_else(|| anyhow::anyhow!("app-server endpoint has no port"))?;
            (host.to_string(), port.parse::<u16>()?)
        };
        if host.is_empty() {
            anyhow::bail!("app-server endpoint has no host");
        }
        let connect_addr = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        Ok(Self {
            url,
            authority,
            connect_addr,
        })
    }
}

enum ProbeState {
    Ready,
    Absent(String),
    Occupied(String),
}

enum ManagedChild {
    Real(tokio::process::Child),
    #[cfg(test)]
    Fake(tokio::sync::oneshot::Receiver<String>),
}

impl ManagedChild {
    async fn wait(&mut self) -> io::Result<String> {
        match self {
            Self::Real(child) => child.wait().await.map(|status| status.to_string()),
            #[cfg(test)]
            Self::Fake(exit) => Ok(exit.await.unwrap_or_else(|_| "fake child exited".into())),
        }
    }

    #[cfg(test)]
    fn fake(exit: tokio::sync::oneshot::Receiver<String>) -> Self {
        Self::Fake(exit)
    }
}

pub async fn supervise(listen: String, token_file: Option<PathBuf>) {
    let endpoint = match Endpoint::parse(listen) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            tracing::error!("cannot supervise codex app-server: {error}");
            return;
        }
    };
    supervise_with(endpoint, token_file, Timings::default(), spawn_codex).await;
}

async fn supervise_with<F>(
    endpoint: Endpoint,
    token_file: Option<PathBuf>,
    timings: Timings,
    mut spawn: F,
) where
    F: FnMut(&str, Option<&Path>) -> io::Result<ManagedChild>,
{
    let mut child: Option<ManagedChild> = None;
    let mut verified = false;
    let mut absent_reads = 0usize;
    let mut healthy_since: Option<tokio::time::Instant> = None;
    let mut backoff = timings.backoff_initial;
    let mut last_observation = "";

    loop {
        match ready_probe(&endpoint, timings.probe_timeout).await {
            ProbeState::Ready => {
                absent_reads = 0;
                if !verified {
                    match deep_verify(&endpoint.url, token_file.as_deref()).await {
                        Ok(()) => {
                            tracing::info!(endpoint = endpoint.url, "codex app-server adopted");
                            verified = true;
                            healthy_since = Some(tokio::time::Instant::now());
                            last_observation = "healthy";
                        }
                        Err(error) => {
                            if last_observation != "occupied" {
                                tracing::error!(
                                    endpoint = endpoint.url,
                                    "codex app-server endpoint is occupied but cannot be adopted: {error}"
                                );
                            }
                            last_observation = "occupied";
                            healthy_since = None;
                        }
                    }
                } else if healthy_since.is_some_and(|since| {
                    tokio::time::Instant::now().duration_since(since) >= timings.stable_uptime
                }) {
                    backoff = timings.backoff_initial;
                }
            }
            ProbeState::Occupied(reason) => {
                if last_observation != "occupied" {
                    tracing::error!(
                        endpoint = endpoint.url,
                        "codex app-server endpoint is occupied but unhealthy/foreign: {reason}"
                    );
                }
                last_observation = "occupied";
                absent_reads = 0;
                verified = false;
                healthy_since = None;
            }
            ProbeState::Absent(reason) => {
                if last_observation != "absent" {
                    tracing::warn!(endpoint = endpoint.url, "codex app-server absent: {reason}");
                }
                last_observation = "absent";
                verified = false;
                healthy_since = None;
                absent_reads = absent_reads.saturating_add(1);
                if absent_reads >= timings.absent_debounce && child.is_none() {
                    tracing::info!(endpoint = endpoint.url, "starting codex app-server");
                    match spawn(&endpoint.url, token_file.as_deref()) {
                        Ok(spawned) => {
                            child = Some(spawned);
                            absent_reads = 0;
                        }
                        Err(error) => {
                            tracing::warn!(
                                endpoint = endpoint.url,
                                "cannot start codex app-server ({error}); retrying in {backoff:?}"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(timings.backoff_max);
                            absent_reads = 0;
                            continue;
                        }
                    }
                }
            }
        }

        let delay = tokio::time::sleep(timings.poll);
        tokio::pin!(delay);
        if let Some(active) = child.as_mut() {
            tokio::select! {
                status = active.wait() => {
                    match status {
                        Ok(status) => tracing::warn!(
                            endpoint = endpoint.url,
                            "codex app-server exited ({status}); retrying in {backoff:?}"
                        ),
                        Err(error) => tracing::warn!(
                            endpoint = endpoint.url,
                            "codex app-server wait failed ({error}); retrying in {backoff:?}"
                        ),
                    }
                    child = None;
                    verified = false;
                    healthy_since = None;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(timings.backoff_max);
                }
                _ = &mut delay => {}
            }
        } else {
            delay.await;
        }
    }
}

async fn ready_probe(endpoint: &Endpoint, timeout: Duration) -> ProbeState {
    let mut stream = match tokio::time::timeout(
        timeout,
        tokio::net::TcpStream::connect(&endpoint.connect_addr),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => return ProbeState::Absent(error.to_string()),
        Err(_) => return ProbeState::Absent("connection timed out".into()),
    };

    let request = format!(
        "GET {READY_PATH} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        endpoint.authority
    );
    let response = tokio::time::timeout(timeout, async {
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        let mut chunk = [0u8; 512];
        while response.len() < 4096 && !response.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..n]);
        }
        io::Result::Ok(response)
    })
    .await;
    let response = match response {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => return ProbeState::Occupied(error.to_string()),
        Err(_) => return ProbeState::Occupied("readyz timed out".into()),
    };
    let first_line = String::from_utf8_lossy(&response)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    if first_line.contains(" 200 ") {
        ProbeState::Ready
    } else {
        ProbeState::Occupied(format!("readyz returned {first_line:?}"))
    }
}

async fn deep_verify(url: &str, token_file: Option<&Path>) -> anyhow::Result<()> {
    let token = match token_file {
        Some(path) => {
            let token = tokio::fs::read_to_string(path).await?;
            let token = token.trim().to_string();
            if token.is_empty() {
                anyhow::bail!("codex capability-token file is empty");
            }
            Some(token)
        }
        None => None,
    };

    let authenticated = adapter_codex::CodexAttach::connect(url, token.as_deref()).await?;
    drop(authenticated);
    if token.is_some() && adapter_codex::CodexAttach::connect(url, None).await.is_ok() {
        anyhow::bail!("configured app-server accepts unauthenticated WebSocket clients");
    }
    Ok(())
}

fn spawn_codex(listen: &str, token_file: Option<&Path>) -> io::Result<ManagedChild> {
    let mut command = tokio::process::Command::new("codex");
    command.args(["app-server", "--listen", listen]);
    if let Some(token_file) = token_file {
        command
            .arg("--ws-auth")
            .arg("capability-token")
            .arg("--ws-token-file")
            .arg(token_file);
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false);
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(0x0000_0200); // CREATE_NEW_PROCESS_GROUP
    command.spawn().map(ManagedChild::Real)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    use super::*;

    #[derive(Clone)]
    enum FakeAuth {
        Open,
        Required(String),
    }

    #[derive(Clone)]
    struct FakeConfig {
        ready_status: u16,
        auth: FakeAuth,
        initialize_ok: bool,
    }

    impl Default for FakeConfig {
        fn default() -> Self {
            Self {
                ready_status: 200,
                auth: FakeAuth::Open,
                initialize_ok: true,
            }
        }
    }

    #[derive(Default)]
    struct FakeStats {
        initializes: AtomicUsize,
    }

    struct FakeServer {
        endpoint: Endpoint,
        stats: Arc<FakeStats>,
        task: tokio::task::JoinHandle<()>,
    }

    impl FakeServer {
        async fn start(config: FakeConfig) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let stats = Arc::new(FakeStats::default());
            let task = tokio::spawn(serve_fake(listener, config, stats.clone()));
            Self {
                endpoint: Endpoint::parse(format!("ws://{addr}")).unwrap(),
                stats,
                task,
            }
        }
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[allow(clippy::result_large_err)] // tungstenite handshake callback signature
    async fn serve_fake(listener: TcpListener, config: FakeConfig, stats: Arc<FakeStats>) {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let config = config.clone();
            let stats = stats.clone();
            tokio::spawn(async move {
                let mut peek = [0u8; 1024];
                let Ok(n) = stream.peek(&mut peek).await else {
                    return;
                };
                let request = String::from_utf8_lossy(&peek[..n]);
                if request.starts_with("GET /readyz ") {
                    let mut request_bytes = [0u8; 1024];
                    let _ = stream.read(&mut request_bytes).await;
                    let reason = if config.ready_status == 200 {
                        "OK"
                    } else {
                        "Service Unavailable"
                    };
                    let response = format!(
                        "HTTP/1.1 {} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        config.ready_status
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    return;
                }

                let auth = config.auth.clone();
                let callback = move |
                    request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                    response: tokio_tungstenite::tungstenite::handshake::server::Response,
                | {
                    if let FakeAuth::Required(token) = &auth {
                        let expected = format!("Bearer {token}");
                        let actual = request
                            .headers()
                            .get("authorization")
                            .and_then(|value| value.to_str().ok());
                        if actual != Some(expected.as_str()) {
                            return Err(tokio_tungstenite::tungstenite::http::Response::builder()
                                .status(401)
                                .body(Some("unauthorized".into()))
                                .unwrap());
                        }
                    }
                    Ok(response)
                };
                let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
                    return;
                };
                let (mut sink, mut source) = ws.split();
                while let Some(Ok(WsMessage::Text(text))) = source.next().await {
                    let Ok(request) = serde_json::from_str::<Value>(&text) else {
                        continue;
                    };
                    let Some(id) = request.get("id").cloned() else {
                        continue;
                    };
                    if request.get("method").and_then(Value::as_str) == Some("initialize") {
                        stats.initializes.fetch_add(1, Ordering::SeqCst);
                        let response = if config.initialize_ok {
                            json!({ "jsonrpc": "2.0", "id": id, "result": { "ok": true } })
                        } else {
                            json!({ "jsonrpc": "2.0", "id": id, "error": {
                                "code": -32603, "message": "foreign listener" } })
                        };
                        if sink
                            .send(WsMessage::Text(response.to_string().into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            });
        }
    }

    fn test_timings() -> Timings {
        Timings {
            poll: Duration::from_millis(10),
            probe_timeout: Duration::from_millis(200),
            backoff_initial: Duration::from_millis(5),
            backoff_max: Duration::from_millis(20),
            stable_uptime: Duration::from_millis(30),
            absent_debounce: 2,
        }
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    fn unused_endpoint() -> Endpoint {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        Endpoint::parse(format!("ws://{addr}")).unwrap()
    }

    fn held_fake_child(keepalive: &Arc<Mutex<Vec<oneshot::Sender<String>>>>) -> ManagedChild {
        let (tx, rx) = oneshot::channel();
        keepalive.lock().unwrap().push(tx);
        ManagedChild::fake(rx)
    }

    #[tokio::test]
    async fn adopts_healthy_endpoint_without_spawning() {
        let server = FakeServer::start(FakeConfig::default()).await;
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let supervisor = tokio::spawn(supervise_with(
            server.endpoint.clone(),
            None,
            test_timings(),
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("must not spawn"))
            },
        ));

        wait_until(|| server.stats.initializes.load(Ordering::SeqCst) >= 1).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
        supervisor.abort();
    }

    #[tokio::test]
    async fn absent_endpoint_spawns_once_after_debounce() {
        let endpoint = unused_endpoint();
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let keepalive = Arc::new(Mutex::new(Vec::new()));
        let child_keepalive = keepalive.clone();
        let supervisor = tokio::spawn(supervise_with(
            endpoint,
            None,
            test_timings(),
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(held_fake_child(&child_keepalive))
            },
        ));

        wait_until(|| spawns.load(Ordering::SeqCst) == 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 1);
        supervisor.abort();
    }

    #[tokio::test]
    async fn occupied_unhealthy_endpoint_never_spawns() {
        let server = FakeServer::start(FakeConfig {
            ready_status: 503,
            ..Default::default()
        })
        .await;
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let supervisor = tokio::spawn(supervise_with(
            server.endpoint.clone(),
            None,
            test_timings(),
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("must not spawn"))
            },
        ));

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
        supervisor.abort();
    }

    #[tokio::test]
    async fn foreign_protocol_endpoint_never_spawns() {
        let server = FakeServer::start(FakeConfig {
            initialize_ok: false,
            ..Default::default()
        })
        .await;
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let supervisor = tokio::spawn(supervise_with(
            server.endpoint.clone(),
            None,
            test_timings(),
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("must not spawn"))
            },
        ));

        wait_until(|| server.stats.initializes.load(Ordering::SeqCst) >= 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
        supervisor.abort();
    }

    #[tokio::test]
    async fn configured_auth_rejects_endpoint_that_also_accepts_unauthenticated_clients() {
        let server = FakeServer::start(FakeConfig::default()).await;
        let token_path =
            std::env::temp_dir().join(format!("workplace-supervisor-token-{}", std::process::id()));
        std::fs::write(&token_path, "expected-token\n").unwrap();
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let supervisor = tokio::spawn(supervise_with(
            server.endpoint.clone(),
            Some(token_path.clone()),
            test_timings(),
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("must not spawn"))
            },
        ));

        wait_until(|| server.stats.initializes.load(Ordering::SeqCst) >= 2).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
        supervisor.abort();
        let _ = std::fs::remove_file(token_path);
    }

    #[tokio::test]
    async fn child_exit_is_observed_and_respawned() {
        let endpoint = unused_endpoint();
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let keepalive = Arc::new(Mutex::new(Vec::new()));
        let child_keepalive = keepalive.clone();
        let supervisor = tokio::spawn(supervise_with(
            endpoint,
            None,
            test_timings(),
            move |_, _| {
                let call = observed.fetch_add(1, Ordering::SeqCst) + 1;
                let (tx, rx) = oneshot::channel();
                if call == 1 {
                    drop(tx);
                } else {
                    child_keepalive.lock().unwrap().push(tx);
                }
                Ok(ManagedChild::fake(rx))
            },
        ));

        wait_until(|| spawns.load(Ordering::SeqCst) >= 2).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 2);
        supervisor.abort();
    }

    #[tokio::test]
    async fn cancelling_supervisor_leaves_spawned_endpoint_reachable() {
        let endpoint = unused_endpoint();
        let addr = endpoint.connect_addr.clone();
        let url = endpoint.url.clone();
        let (start_tx, start_rx) = oneshot::channel();
        let mut start_tx = Some(start_tx);
        let stats = Arc::new(FakeStats::default());
        let server_stats = stats.clone();
        let server_task = tokio::spawn(async move {
            start_rx.await.unwrap();
            let listener = TcpListener::bind(addr).await.unwrap();
            serve_fake(listener, FakeConfig::default(), server_stats).await;
        });
        let keepalive = Arc::new(Mutex::new(Vec::new()));
        let child_keepalive = keepalive.clone();
        let supervisor = tokio::spawn(supervise_with(
            endpoint,
            None,
            test_timings(),
            move |_, _| {
                start_tx.take().unwrap().send(()).unwrap();
                Ok(held_fake_child(&child_keepalive))
            },
        ));

        wait_until(|| stats.initializes.load(Ordering::SeqCst) >= 1).await;
        supervisor.abort();
        let _ = supervisor.await;
        let attached = adapter_codex::CodexAttach::connect(&url, None)
            .await
            .expect("spawned endpoint must outlive supervisor cancellation");
        drop(attached);
        server_task.abort();
    }

    #[tokio::test]
    async fn adopts_endpoint_with_required_auth_and_rejects_unauthenticated_probe() {
        let server = FakeServer::start(FakeConfig {
            auth: FakeAuth::Required("expected-token".into()),
            ..Default::default()
        })
        .await;
        let token_path = std::env::temp_dir().join(format!(
            "workplace-supervisor-required-token-{}",
            std::process::id()
        ));
        std::fs::write(&token_path, "expected-token\n").unwrap();
        let spawns = Arc::new(AtomicUsize::new(0));
        let observed = spawns.clone();
        let supervisor = tokio::spawn(supervise_with(
            server.endpoint.clone(),
            Some(token_path.clone()),
            test_timings(),
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("must not spawn"))
            },
        ));

        wait_until(|| server.stats.initializes.load(Ordering::SeqCst) >= 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
        assert_eq!(server.stats.initializes.load(Ordering::SeqCst), 1);
        supervisor.abort();
        let _ = std::fs::remove_file(token_path);
    }
}
