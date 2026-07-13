# Daemon and CLI runtime

**Status: decided.** Process model, configuration, and on-disk layout for `workplace daemon` and `workplace cli`.

## Processes

- **`workplace daemon`** — the broker: owns the store, the channel/principal state, and the delivery adapters. One instance per machine (per configured bind). Transport: [ADR-0016](../decision-records/0016-tcp-broker-transport.md).
- **`workplace cli`** — the human interface: an **interactive TUI** holding one persistent broker connection. A live event-stream pane (messages, system events, ack transitions, DMs included — the `watch/event` stream) plus a command line in the same window: plain text posts to the focused channel; slash commands (`/send`, `/sub`, `/status`, …) cover posting, channel creation, forced subscriptions, ack inspection, and history, with slash-command tab completion inside the TUI (argument completion — channel/principal names — is not implemented yet). One-shot subcommands (`workplace cli send …`) are deferred — added only if they fall out of the same command parser for free. A web interface is deferred.

### Lazy start

`workplace cli` never requires the daemon to be running first:

1. Probe the configured broker endpoint; if a listener answers, perform a one-shot `session/hello` handshake as the health check (it proves the listener is a workplace broker that accepts this client, without needing admin rights).
2. If nothing is listening **and the endpoint is local** (loopback or an address of this host), spawn `workplace daemon` detached and wait for readiness before proceeding. A remote endpoint is never lazy-started: if it is down, report the error.
3. If the listener is not a healthy workplace broker (foreign process on the port, auth refusal, malformed response), report it instead of silently starting a second instance.

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
# Shared-secret every session must present in session/hello. Unset = open
# broker (see the trust boundary below); set one BEFORE adding any
# non-loopback listen address.
# auth_token = ""
# Admin credential file (ADR-0019). Default: <data dir>/admin-token,
# auto-generated (0600) on first start; admin/register requires its
# contents. Override with a path — never an inline secret.
# admin_token_file = ""

[client]
# Endpoint that workplace cli and the adapters on this machine dial.
# Overridable per invocation: workplace cli --broker <host[:port]>.
broker = "127.0.0.1:9675"
# Token matching [broker].auth_token of the daemon this machine dials.
# auth_token = ""

[storage]
# SQLite database path. Default: platform data dir below.
# database = ""

[codex]
# Shared Codex app-server for `codex --remote` sessions. When set, the daemon
# spawns and supervises `codex app-server --listen <this>` — the user never
# runs it. The app-server is left running across daemon restarts and re-adopted
# on the next start (see "Codex app-server supervision" below), so a broker
# restart does not drop attached Codex sessions. Omit to disable Codex push
# delivery; plain `codex` sessions then participate outbound-only.
# app_server = "ws://127.0.0.1:9701"
# Capability-token file for the shared app-server (recommended even on
# loopback: any local process could otherwise drive the agent). The daemon
# adds `--ws-auth capability-token --ws-token-file <this>` and the attach
# client presents the contents as `Authorization: Bearer` on the WebSocket
# upgrade. The human's window must use the same token:
#   CODEX_REMOTE_AUTH_TOKEN=$(<file) codex --remote <addr> \
#     --remote-auth-token-env CODEX_REMOTE_AUTH_TOKEN
# Requires a Codex build exposing --ws-auth (verified on codex-cli 0.144.1).
# token_file = ""

[log]
level = "info"
```

**Broker endpoint selection** for `workplace cli`: the `--broker <host[:port]>` flag overrides `[client].broker` for a single invocation; a bare host assumes the default port (`9675` — "WORK" on a phone keypad). Precedence: `--broker` flag > `[client].broker` > loopback default. The lazy-start local-endpoint guard above applies to whichever endpoint wins.

## Trust boundary

The broker's authorization model is deliberately thin and must be understood before changing the bind addresses:

- **Any session that passes `session/hello` can register any free principal name.** There is no identity beyond the name claim — except for admin: **`admin/register` additionally requires the admin credential** ([ADR-0019](../decision-records/0019-admin-credential.md)), auto-generated into `<data dir>/admin-token` (0600) on first daemon start and read automatically by `workplace cli`. Admin rights grant the observability tap, DM history between any pair, channel deletion, and `daemon/shutdown`; denied attempts are audited (`RegistrationDenied`, never echoing the supplied value). An agent that can read the operator's data directory already crosses the host boundary — the bus does not pretend to prevent that.
- The **default posture is loopback-only**: every process that can reach the port is assumed to be the operator's. That is the whole security model when `auth_token` is unset.
- **Set `[broker].auth_token` before adding any non-loopback `listen` address.** With a token set, `session/hello` is refused without it (`UNAUTHORIZED`), which gates every other verb. The token is a shared secret in the config file — machine-level protection, not per-principal identity.
- The broker only ever dials **loopback `ws://` endpoints** as Codex app-servers, whatever a registration self-reports — a wire-supplied URL must not be able to point bus traffic at an arbitrary host.
- Transport hygiene: inbound lines are length-capped (body limit + envelope slack), `jsonrpc: "2.0"` is enforced on requests, and malformed lines get spec `-32700` responses.

## Codex app-server supervision

When `[codex].app_server` is set, the daemon keeps a shared `codex app-server --listen <endpoint>` alive for `codex --remote` sessions to attach to. It supervises the **endpoint**, not merely a child it owns — so the Codex engine and attached `codex --remote` windows survive a broker restart. (Continuity here means the app-server and its sessions: the broker's own JSON-RPC is briefly unavailable while the daemon restarts.)

**State machine.** Roughly every two seconds the daemon runs a shallow readiness probe — a `GET /readyz` over TCP to the endpoint's host:port — and acts on the result:

- **Ready** (`/readyz` → `200`) — if not already adopted, the daemon runs a **deep verification**: an authenticated `initialize` over the WebSocket (presenting the capability token when `token_file` is set) and, when a token is configured, a confirmation that the endpoint **rejects** an unauthenticated client. On success the endpoint is **adopted** and monitored by readyz thereafter; on failure it is treated as occupied and left alone — never spawned into.
- **Absent** (nothing listening) — after a short debounce (two consecutive absent probes), the daemon **launches** `codex app-server --listen <endpoint>`, adding `--ws-auth capability-token --ws-token-file <file>` when `token_file` is set.
- **Occupied** (listening but `/readyz` is non-200 or unresponsive) — the daemon **never spawns**; something already holds the endpoint. A server the daemon just spawned that is still starting logs as "spawned…not ready"; a genuinely foreign or unhealthy occupant logs as an error.

The endpoint the daemon adopts may be one it spawned or one **left running by a previous daemon** — adopting either is what makes broker restarts transparent to attached Codex sessions.

**Detached lifetime (Codex-session continuity).** A spawned app-server is deliberately **not** bound to the daemon's lifetime: it is started detached — `kill_on_drop(false)` and its own process group (Unix `process_group(0)`, the verified path; Windows `CREATE_NEW_PROCESS_GROUP`, best-effort and not exercised in CI), stdio to null. On Unix, when the daemon exits or restarts the app-server keeps running and `codex --remote` windows stay attached; the next daemon re-adopts it after verifying health. This supersedes an earlier model that killed the app-server on daemon shutdown.

**Backoff.** Two independent backoffs keep failure loops cheap:

- *Respawn*, after the app-server exits or fails to start: `1s → 30s`, doubling, reset only after the endpoint has been **adopted and healthy** for a stable window (30s) — a binary that starts but never becomes adoptable, or crashes immediately, never resets it to full speed.
- *Deep-verification retry*, for a Ready-but-unadoptable endpoint (foreign protocol, auth mismatch): a separate `5s → 60s` capped backoff, so `initialize` is not re-attempted on every poll. It resets on successful adoption or on any real shallow-state change — the endpoint going Absent or Occupied.

**Authentication.** With `[codex].token_file` set, adoption requires **both** a successful authenticated `initialize` **and** that the endpoint refuses an unauthenticated client: an app-server that accepts no-token connections is rejected rather than adopted. Without a token file, any local process can drive the agent (the loopback trust posture); setting one is recommended even on loopback.

**Stopping the app-server (deferred).** Because a spawned app-server survives daemon exit by design, workplace has **no owned command to stop it** — it tracks no PID for the app-server across restarts and writes no pidfile for it. Stopping a managed app-server is currently a manual operation (terminate the `codex app-server --listen <endpoint>` process). A workplace-owned stop is deferred.

## On-disk layout

| Purpose | macOS / Linux | Windows |
| --- | --- | --- |
| Config | `~/.config/workplace/` | `%APPDATA%\workplace\` |
| Data (SQLite store) | `$XDG_DATA_HOME/workplace/workplace.db` (default `~/.local/share/workplace/workplace.db`) | `%LOCALAPPDATA%\workplace\workplace.db` |
| Runtime (pid file) | `$XDG_RUNTIME_DIR/workplace/`, falling back to the data directory | `%LOCALAPPDATA%\workplace\` |

## Storage engine

The store ([ADR-0005](../decision-records/0005-broker-owned-append-only-audit-log.md)) is **SQLite embedded in the daemon** — compiled into the `workplace` binary, no system dependency, single-writer through the broker only. Rationale, options considered, and constraints: [ADR-0017](../decision-records/0017-embedded-sqlite-storage.md).

## Shell completions

`workplace completions` detects the invoking shell (from `$SHELL` / the parent process) and prints the completion script for it to stdout — one command, no shell argument needed. Completions cover subcommands, flags, and value hints, generated from the CLI definition (`clap_complete`), so tab discovery always matches the implemented surface.

## Version

The binary embeds the version from the top-level [`VERSION`](../../VERSION) file at build time — the single authoritative source.
