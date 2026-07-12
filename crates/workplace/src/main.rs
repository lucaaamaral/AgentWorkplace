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
        Command::ShimClaude { broker } => run_shim(cfg, broker),
        Command::Completions { shell } => run_completions(shell),
    }
}

fn init_logging(level: &str, to_stderr: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level.to_string()));
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
            cfg.storage.database.clone().unwrap_or_else(|| config::data_dir().join("workplace.db")),
        ),
        message_size_limit: config::parse_size(&cfg.broker.message_size_limit)?,
        grace_window: Duration::from_secs(60),
        version: version(),
    };
    tokio::runtime::Runtime::new()?.block_on(broker::run(broker_cfg))
}

fn run_shim(cfg: config::ConfigFile, broker_flag: Option<String>) -> anyhow::Result<()> {
    // stdout is MCP protocol: logs must go to stderr only.
    init_logging(&cfg.log.level, true);
    let endpoint = broker_flag.unwrap_or_else(|| cfg.client.broker.clone());
    let addr = config::parse_endpoint(&endpoint)?;
    let shim_cfg = shim_claude::ShimConfig { broker_addr: addr.to_string(), version: version() };
    tokio::runtime::Runtime::new()?.block_on(shim_claude::run(shim_cfg))
}

fn run_cli(cfg: config::ConfigFile, broker_flag: Option<String>, name: String) -> anyhow::Result<()> {
    let endpoint = broker_flag.unwrap_or_else(|| cfg.client.broker.clone());
    let addr = config::parse_endpoint(&endpoint)?;
    lazy_start(&addr)?;
    tokio::runtime::Runtime::new()?.block_on(tui::run(addr, name, version()))
}

/// Lazy start (daemon.md): spawn a local daemon when nothing is listening on
/// a local endpoint. A remote endpoint that is down is an error, never a
/// shadow local daemon.
fn lazy_start(addr: &std::net::SocketAddr) -> anyhow::Result<()> {
    if std::net::TcpStream::connect_timeout(addr, Duration::from_millis(500)).is_ok() {
        return Ok(());
    }
    if !config::endpoint_is_local(addr) {
        anyhow::bail!("broker at {addr} is not reachable (remote endpoints are never lazy-started)");
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
            return Ok(());
        }
    }
    anyhow::bail!("daemon did not become ready on {addr}")
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
