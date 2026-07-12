//! `workplace` — the AgentWorkplace bus.
//!
//! Subcommands: `daemon` (broker), `cli` (interactive TUI), `shim-claude`
//! (Claude Code channel shim, spawned by the harness), `completions`.

mod config;
mod tui;

use std::path::PathBuf;
use std::time::Duration;

use clap::{CommandFactory, Parser, Subcommand};

/// Single authoritative version source: the top-level VERSION file.
pub const VERSION: &str = include_str!("../../../VERSION");

fn version() -> String {
    VERSION.trim().to_string()
}

#[derive(Parser)]
#[command(name = "workplace", about = "AgentWorkplace: a local pub-sub bus for coding agents and their manager", version = VERSION.trim_ascii())]
struct Cli {
    /// Config file path (default: XDG config dir; env: WORKPLACE_CONFIG)
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the broker daemon in the foreground
    Daemon,
    /// Open the interactive TUI (lazy-starts a local daemon if needed)
    Cli {
        /// Broker endpoint host[:port]; overrides [client].broker
        #[arg(long)]
        broker: Option<String>,
        /// Principal name to admin-register as
        #[arg(long, default_value = "@manager")]
        name: String,
    },
    /// Claude Code channel shim (spawned by Claude Code, stdio MCP)
    ShimClaude {
        /// Broker endpoint host[:port]; overrides [client].broker
        #[arg(long)]
        broker: Option<String>,
        /// Codex only: shared `codex app-server --listen` WebSocket endpoint
        /// (e.g. ws://127.0.0.1:9701) attached to registrations carrying a
        /// thread_id, so the broker delivers via the attach engine
        #[arg(long)]
        codex_app_server: Option<String>,
    },
    /// Print shell completions (shell auto-detected from $SHELL)
    Completions {
        /// Override shell detection (bash, zsh, fish, powershell, elvish)
        #[arg(long)]
        shell: Option<clap_complete::Shell>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(cli.config.clone())?;

    match cli.command {
        Command::Daemon => run_daemon(cfg),
        Command::Cli { broker, name } => run_cli(cfg, broker, name),
        Command::ShimClaude {
            broker,
            codex_app_server,
        } => run_shim(cfg, broker, codex_app_server),
        Command::Completions { shell } => run_completions(shell),
    }
}

fn init_logging(level: &str, to_stderr: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if to_stderr {
        builder.with_writer(std::io::stderr).init();
    } else {
        builder.init();
    }
}

fn run_daemon(cfg: config::ConfigFile) -> anyhow::Result<()> {
    init_logging(&cfg.log.level, false);
    let mut listens = Vec::new();
    for l in &cfg.broker.listen {
        listens.push(config::parse_endpoint(l)?);
    }
    let broker_cfg = broker::BrokerConfig {
        listens,
        db_path: Some(
            cfg.storage
                .database
                .clone()
                .unwrap_or_else(|| config::data_dir().join("workplace.db")),
        ),
        message_size_limit: config::parse_size(&cfg.broker.message_size_limit)?,
        grace_window: Duration::from_secs(60),
        version: version(),
        auth_token: cfg.broker.auth_token.clone(),
        codex_token_file: cfg.codex.token_file.clone(),
    };
    let codex_app_server = cfg.codex.app_server.clone();
    let codex_token_file = cfg.codex.token_file.clone();
    tokio::runtime::Runtime::new()?.block_on(async move {
        if let Some(listen) = codex_app_server {
            tokio::spawn(supervise_codex_app_server(listen, codex_token_file));
        }
        // Exit the runtime on SIGTERM/SIGINT so kill_on_drop reaps the
        // managed app-server child (a signal-killed process runs no drops).
        tokio::select! {
            r = broker::run(broker_cfg) => r,
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received");
                Ok(())
            }
        }
    })
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Spawn and supervise the shared `codex app-server --listen <addr>` so
/// `codex --remote <addr>` sessions have an engine to attach to. Restarts
/// with backoff if it dies; the child is killed when the daemon exits.
/// Backoff resets only after the child has stayed up — a spawn that crashes
/// immediately must not restart at full speed forever.
async fn supervise_codex_app_server(listen: String, token_file: Option<std::path::PathBuf>) {
    const STABLE_UPTIME: Duration = Duration::from_secs(30);
    let mut backoff = Duration::from_secs(1);
    loop {
        tracing::info!("starting managed codex app-server on {listen}");
        let mut cmd = tokio::process::Command::new("codex");
        cmd.args(["app-server", "--listen", &listen]);
        if let Some(token_file) = &token_file {
            // Capability-token auth (verified on codex-cli 0.144.1): any
            // local process could otherwise drive the agent.
            cmd.arg("--ws-auth")
                .arg("capability-token")
                .arg("--ws-token-file")
                .arg(token_file);
        }
        let child = cmd
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn();
        match child {
            Ok(mut child) => {
                let started = std::time::Instant::now();
                match child.wait().await {
                    Ok(status) => {
                        tracing::warn!("managed codex app-server exited ({status}); restarting")
                    }
                    Err(e) => tracing::warn!("managed codex app-server wait failed: {e}"),
                }
                if started.elapsed() >= STABLE_UPTIME {
                    backoff = Duration::from_secs(1);
                }
            }
            Err(e) => {
                tracing::warn!("cannot start codex app-server ({e}); retrying in {backoff:?}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn run_shim(
    cfg: config::ConfigFile,
    broker_flag: Option<String>,
    codex_app_server: Option<String>,
) -> anyhow::Result<()> {
    // stdout is MCP protocol: logs must go to stderr only.
    init_logging(&cfg.log.level, true);
    let endpoint = broker_flag.unwrap_or_else(|| cfg.client.broker.clone());
    let addr = config::parse_endpoint(&endpoint)?;
    let shim_cfg = shim_claude::ShimConfig {
        broker_addr: addr.to_string(),
        version: version(),
        codex_app_server,
        auth_token: cfg.client.auth_token.clone(),
    };
    tokio::runtime::Runtime::new()?.block_on(shim_claude::run(shim_cfg))
}

fn run_cli(
    cfg: config::ConfigFile,
    broker_flag: Option<String>,
    name: String,
) -> anyhow::Result<()> {
    let endpoint = broker_flag.unwrap_or_else(|| cfg.client.broker.clone());
    let addr = config::parse_endpoint(&endpoint)?;
    let auth_token = cfg.client.auth_token.clone();
    lazy_start(&addr, auth_token.as_deref())?;
    tokio::runtime::Runtime::new()?.block_on(tui::run(addr, name, version(), auth_token))
}

/// Lazy start (daemon.md): spawn a local daemon when nothing is listening on
/// a local endpoint. A remote endpoint that is down is an error, never a
/// shadow local daemon. A live listener must pass the health check — a
/// foreign process on the port is an error, not a broker.
fn lazy_start(addr: &std::net::SocketAddr, auth_token: Option<&str>) -> anyhow::Result<()> {
    if std::net::TcpStream::connect_timeout(addr, Duration::from_millis(500)).is_ok() {
        return health_check(addr, auth_token);
    }
    if !config::endpoint_is_local(addr) {
        anyhow::bail!(
            "broker at {addr} is not reachable (remote endpoints are never lazy-started)"
        );
    }
    let exe = std::env::current_exe()?;
    std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        if std::net::TcpStream::connect_timeout(addr, Duration::from_millis(200)).is_ok() {
            return health_check(addr, auth_token);
        }
    }
    anyhow::bail!("daemon did not become ready on {addr}")
}

/// One-shot session/hello handshake against a live listener (daemon.md lazy
/// start step 1): proves the listener is a workplace broker that accepts us.
fn health_check(addr: &std::net::SocketAddr, auth_token: Option<&str>) -> anyhow::Result<()> {
    use std::io::{BufRead, Write};
    let stream = std::net::TcpStream::connect_timeout(addr, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let hello = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "session/hello",
        "params": {
            "client_info": {
                "version": version(),
                "pid": std::process::id(),
                "cwd": std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
            },
            "auth_token": auth_token,
        }
    });
    let mut writer = &stream;
    writer.write_all(format!("{hello}\n").as_bytes())?;
    let mut line = String::new();
    std::io::BufReader::new(&stream)
        .read_line(&mut line)
        .map_err(|e| anyhow::anyhow!("no health-check response from {addr}: {e}"))?;
    let v: serde_json::Value = serde_json::from_str(line.trim())
        .map_err(|_| anyhow::anyhow!("listener at {addr} is not a workplace broker"))?;
    if let Some(err) = v.get("error") {
        anyhow::bail!(
            "broker at {addr} refused the health check: {}",
            err.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error")
        );
    }
    if v.pointer("/result/broker_version")
        .and_then(|b| b.as_str())
        .is_none()
    {
        anyhow::bail!("listener at {addr} is not a workplace broker");
    }
    Ok(())
}

fn run_completions(shell: Option<clap_complete::Shell>) -> anyhow::Result<()> {
    let shell = shell.or_else(detect_shell).ok_or_else(|| {
        anyhow::anyhow!("cannot detect shell from $SHELL; pass --shell <bash|zsh|fish|powershell>")
    })?;
    let mut cmd = Cli::command();
    clap_complete::generate(shell, &mut cmd, "workplace", &mut std::io::stdout());
    Ok(())
}

fn detect_shell() -> Option<clap_complete::Shell> {
    let shell_path = std::env::var("SHELL").ok()?;
    let name = std::path::Path::new(&shell_path).file_name()?.to_str()?;
    match name {
        "bash" => Some(clap_complete::Shell::Bash),
        "zsh" => Some(clap_complete::Shell::Zsh),
        "fish" => Some(clap_complete::Shell::Fish),
        "elvish" => Some(clap_complete::Shell::Elvish),
        "pwsh" | "powershell" => Some(clap_complete::Shell::PowerShell),
        _ => None,
    }
}
