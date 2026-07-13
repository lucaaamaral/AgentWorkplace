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
    /// Admin credential file (ADR-0019). Default: <data dir>/admin-token,
    /// auto-generated on first daemon start. A path override, never an
    /// inline secret.
    #[serde(default)]
    pub admin_token_file: Option<PathBuf>,
}

impl Default for BrokerSection {
    fn default() -> Self {
        BrokerSection {
            listen: default_listen(),
            message_size_limit: default_size_limit(),
            auth_token: None,
            admin_token_file: None,
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
    /// Admin credential file the TUI reads for admin/register (ADR-0019).
    /// Default: <data dir>/admin-token — zero-config when the daemon runs
    /// on the same machine. Point it at a copy of the daemon's token for a
    /// remote broker (over a trusted/tunneled link only: TCP is plaintext).
    #[serde(default)]
    pub admin_token_file: Option<PathBuf>,
}

impl Default for ClientSection {
    fn default() -> Self {
        ClientSection {
            broker: default_endpoint(),
            auth_token: None,
            admin_token_file: None,
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

/// Default admin-token location: beside the store, same trust level
/// (ADR-0019).
pub fn default_admin_token_path() -> PathBuf {
    data_dir().join("admin-token")
}

/// Daemon-side admin credential (ADR-0019): read it, or generate it on first
/// start — 256 random bits, hex-encoded, written with exclusive-create
/// semantics and 0600 permissions. Fails closed: any error here aborts
/// daemon startup rather than serving with admin registration open or
/// silently disabled.
pub fn ensure_admin_token(path: &std::path::Path) -> anyhow::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(existing) => {
            check_token_perms(path)?;
            let token = existing.trim().to_string();
            anyhow::ensure!(
                !token.is_empty(),
                "admin token file {} is empty",
                path.display()
            );
            Ok(token)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes)
                .map_err(|e| anyhow::anyhow!("no secure randomness: {e}"))?;
            let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create_new(true); // exclusive: never clobber a concurrent writer
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            use std::io::Write;
            let mut file = opts
                .open(path)
                .map_err(|e| anyhow::anyhow!("cannot create {}: {e}", path.display()))?;
            file.write_all(token.as_bytes())?;
            file.write_all(b"\n")?;
            Ok(token)
        }
        Err(e) => Err(anyhow::anyhow!("cannot read {}: {e}", path.display())),
    }
}

/// Client-side read (the daemon owns creation). Same permission check.
pub fn read_admin_token(path: &std::path::Path) -> anyhow::Result<String> {
    let token = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read admin token {}: {e}", path.display()))?;
    check_token_perms(path)?;
    let token = token.trim().to_string();
    anyhow::ensure!(
        !token.is_empty(),
        "admin token file {} is empty",
        path.display()
    );
    Ok(token)
}

/// The token must not be group/world-accessible (unix; no-op elsewhere).
fn check_token_perms(path: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode();
        anyhow::ensure!(
            mode & 0o077 == 0,
            "{} is group/world-accessible (mode {:o}); run: chmod 600 {}",
            path.display(),
            mode & 0o777,
            path.display()
        );
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_token_lifecycle() {
        let dir = std::env::temp_dir().join(format!("workplace-admin-tok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("admin-token");

        // First start generates: 256 bits hex, private permissions.
        let token = ensure_admin_token(&path).unwrap();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // Second start reads the same credential; the client read agrees.
        assert_eq!(ensure_admin_token(&path).unwrap(), token);
        assert_eq!(read_admin_token(&path).unwrap(), token);

        // Group/world-accessible token is refused, with the fix named.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            let err = read_admin_token(&path).unwrap_err().to_string();
            assert!(err.contains("chmod 600"), "{err}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
