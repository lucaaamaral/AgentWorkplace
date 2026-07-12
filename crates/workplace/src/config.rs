//! Config file loading and platform paths (docs/architecture/daemon.md).
//!
//! TOML at XDG-style locations (including macOS): ~/.config/workplace/
//! config.toml on POSIX, %APPDATA%\workplace\config.toml on Windows.
//! Precedence: --config flag > WORKPLACE_CONFIG env > platform default.
//! A missing file is not an error — every key has a default.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConfigFile {
    #[serde(default)]
    pub broker: BrokerSection,
    #[serde(default)]
    pub client: ClientSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub log: LogSection,
    #[serde(default)]
    pub codex: CodexSection,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CodexSection {
    /// When set, the daemon spawns and supervises a shared
    /// `codex app-server --listen <this>` so `codex --remote <this>` sessions
    /// have an engine to attach to. Omit to disable Codex push delivery
    /// (plain `codex` sessions then participate outbound-only).
    pub app_server: Option<String>,
    /// Capability-token file for the shared app-server. When set, the daemon
    /// spawns it with `--ws-auth capability-token --ws-token-file <this>` and
    /// the attach client presents the file's contents as a Bearer token on
    /// the WebSocket upgrade. The human's client must use the same token:
    /// `CODEX_REMOTE_AUTH_TOKEN=$(<file) codex --remote <addr>
    /// --remote-auth-token-env CODEX_REMOTE_AUTH_TOKEN`.
    pub token_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerSection {
    #[serde(default = "default_listen")]
    pub listen: Vec<String>,
    #[serde(default = "default_size_limit")]
    pub message_size_limit: String,
    /// Shared-secret token every session must present in session/hello.
    /// Unset = open broker; set one before adding a non-loopback listener.
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for BrokerSection {
    fn default() -> Self {
        BrokerSection {
            listen: default_listen(),
            message_size_limit: default_size_limit(),
            auth_token: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClientSection {
    #[serde(default = "default_endpoint")]
    pub broker: String,
    /// Token presented to the broker in session/hello (matches
    /// [broker].auth_token of the daemon this client dials).
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for ClientSection {
    fn default() -> Self {
        ClientSection {
            broker: default_endpoint(),
            auth_token: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageSection {
    pub database: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogSection {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogSection {
    fn default() -> Self {
        LogSection {
            level: default_log_level(),
        }
    }
}

fn default_listen() -> Vec<String> {
    vec![format!("127.0.0.1:{}", protocol::DEFAULT_PORT)]
}

fn default_size_limit() -> String {
    "8MB".into()
}

fn default_endpoint() -> String {
    format!("127.0.0.1:{}", protocol::DEFAULT_PORT)
}

fn default_log_level() -> String {
    "info".into()
}

/// XDG-style config dir on POSIX (macOS included, by decision); %APPDATA% on
/// Windows.
pub fn config_dir() -> PathBuf {
    #[cfg(windows)]
    {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("workplace")
    }
    #[cfg(not(windows))]
    {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::home_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".config")
            });
        base.join("workplace")
    }
}

pub fn data_dir() -> PathBuf {
    #[cfg(windows)]
    {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("workplace")
    }
    #[cfg(not(windows))]
    {
        let base = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::home_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".local/share")
            });
        base.join("workplace")
    }
}

pub fn load(explicit: Option<PathBuf>) -> anyhow::Result<ConfigFile> {
    let path = explicit
        .or_else(|| std::env::var_os("WORKPLACE_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| config_dir().join("config.toml"));
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            Ok(toml::from_str(&text).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConfigFile::default()),
        Err(e) => Err(anyhow::anyhow!("{}: {e}", path.display())),
    }
}

/// "8MB" / "512KB" / "1048576" → bytes.
pub fn parse_size(s: &str) -> anyhow::Result<usize> {
    let t = s.trim().to_ascii_uppercase();
    let (num, mult) = if let Some(n) = t.strip_suffix("GB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = t.strip_suffix("MB") {
        (n, 1024 * 1024)
    } else if let Some(n) = t.strip_suffix("KB") {
        (n, 1024)
    } else if let Some(n) = t.strip_suffix("B") {
        (n, 1)
    } else {
        (t.as_str(), 1)
    };
    let value: usize = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("bad size: {s}"))?;
    Ok(value * mult)
}

/// "host[:port]" with the default port assumed when unspecified.
pub fn parse_endpoint(s: &str) -> anyhow::Result<SocketAddr> {
    use std::net::ToSocketAddrs;
    let candidate = if s.contains(':') {
        s.to_string()
    } else {
        format!("{s}:{}", protocol::DEFAULT_PORT)
    };
    candidate
        .to_socket_addrs()
        .map_err(|e| anyhow::anyhow!("cannot resolve {s}: {e}"))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("cannot resolve {s}"))
}

/// Loopback or an address of this host — the lazy-start guard.
pub fn endpoint_is_local(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback() || addr.ip().is_unspecified()
}
