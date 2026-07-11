# Daemon and CLI runtime

**Status: decided.** Process model, configuration, and on-disk layout for `workplace daemon` and `workplace cli`.

## Processes

- **`workplace daemon`** — the broker: owns the store, the channel/principal state, and the delivery adapters. One instance per machine (per configured bind). Transport: [ADR-0016](../decision-records/0016-tcp-broker-transport.md).
- **`workplace cli`** — the human interface: an **interactive TUI** holding one persistent broker connection. A live event-stream pane (messages, system events, ack transitions, DMs included — the `watch/event` stream) plus a command line in the same window: plain text posts to the focused channel; slash commands (`/send`, `/sub`, `/status`, …) cover posting, channel creation, forced subscriptions, ack inspection, and history, with tab completion inside the TUI. One-shot subcommands (`workplace cli send …`) are deferred — added only if they fall out of the same command parser for free. A web interface is deferred.

### Lazy start

`workplace cli` never requires the daemon to be running first:

1. Probe the configured broker endpoint; if a listener answers, call `daemon/status` as a health check.
2. If nothing is listening **and the endpoint is local** (loopback or an address of this host), spawn `workplace daemon` detached and wait for readiness before proceeding. A remote endpoint is never lazy-started: if it is down, report the error.
3. If the daemon is running but errored (health check fails on a live listener), report it instead of silently starting a second instance.

`workplace daemon` can still be run explicitly (foreground, for logs/debugging); service integration (launchd / systemd / Windows service) is an implementation-phase concern per [ADR-0011](../decision-records/0011-multiplatform-support.md).

## Configuration

**Format: TOML** — the Rust-ecosystem convention, comment-friendly, serde-native.

**Location (XDG-style on POSIX, including macOS):**

| Platform | Config file |
| --- | --- |
| macOS / Linux | `$XDG_CONFIG_HOME/workplace/config.toml` (default `~/.config/workplace/config.toml`) |
| Windows | `%APPDATA%\workplace\config.toml` |

Precedence: `--config <path>` flag > `WORKPLACE_CONFIG` env var > platform default. A missing file is not an error — every key has a default.

Initial keys (deliberately minimal; nothing is added until something forces it):

```toml
[broker]
# Bind addresses ([ADR-0016]); default loopback only. Add a network-reachable
# address for agents on other machines — more listeners, same broker.
listen = ["127.0.0.1:9675"]
# Message body size limit; oversized bodies are truncated, not rejected.
message_size_limit = "8MB"

[client]
# Endpoint that workplace cli and the adapters on this machine dial.
# Overridable per invocation: workplace cli --broker <host[:port]>.
broker = "127.0.0.1:9675"

[storage]
# SQLite database path. Default: platform data dir below.
# database = ""

[log]
level = "info"
```

**Broker endpoint selection** for `workplace cli`: the `--broker <host[:port]>` flag overrides `[client].broker` for a single invocation; a bare host assumes the default port (`9675` — "WORK" on a phone keypad). Precedence: `--broker` flag > `[client].broker` > loopback default. The lazy-start local-endpoint guard above applies to whichever endpoint wins.

## On-disk layout

| Purpose | macOS / Linux | Windows |
| --- | --- | --- |
| Config | `~/.config/workplace/` | `%APPDATA%\workplace\` |
| Data (SQLite store) | `$XDG_DATA_HOME/workplace/workplace.db` (default `~/.local/share/workplace/workplace.db`) | `%LOCALAPPDATA%\workplace\workplace.db` |
| Runtime (pid file) | `$XDG_RUNTIME_DIR/workplace/`, falling back to the data directory | `%LOCALAPPDATA%\workplace\` |

## Storage engine

The store ([ADR-0005](../decision-records/0005-broker-owned-append-only-audit-log.md)) is **SQLite embedded in the daemon** via `rusqlite` with the `bundled` feature: the SQLite amalgamation is compiled into the `workplace` binary, so there is no system dependency and behavior is identical on all three platforms. Licensing is unencumbered: SQLite is public domain (no attribution, no copyleft); `rusqlite` is MIT.

## Shell completions

`workplace completions` detects the invoking shell (from `$SHELL` / the parent process) and prints the completion script for it to stdout — one command, no shell argument needed. Completions cover subcommands, flags, and value hints, generated from the CLI definition (`clap_complete`), so tab discovery always matches the implemented surface.

## Version

The binary embeds the version from the top-level [`VERSION`](../../VERSION) file at build time — the single authoritative source.
