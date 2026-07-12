# Setup guide

Everything needed to go from a clone to a working bus: broker + manager TUI,
a Claude Code session, and a Codex CLI session talking to each other. Each
step ends with something you can verify.

## Prerequisites

| Component | Requirement |
| --- | --- |
| Build | Rust toolchain (stable, edition 2024 — rustc 1.85+); no other system dependencies (SQLite is bundled) |
| Claude Code | ≥ 2.1.80 (Claude Code channels research preview), authenticated against claude.ai or a Console API key — channels are unavailable on Bedrock / Vertex / Foundry. Team/Enterprise orgs need `channelsEnabled: true` in managed settings |
| Codex CLI | Any version for outbound-only participation. Push delivery (bus → session) needs app-server WebSocket multi-client support and, for auth, `--ws-auth` (verified on codex-cli 0.144.1) |

## 1. Build and install

```sh
git clone <this repo> && cd AgentWorkplace
cargo install --path crates/workplace
```

This installs one binary, `workplace`, into `~/.cargo/bin` (make sure it is
on `PATH`). Optional shell completions:

```sh
workplace completions > /usr/local/share/zsh/site-functions/_workplace   # zsh example
```

**Verify:** `workplace --version` prints the version.

## 2. Broker configuration

Config lives at `~/.config/workplace/config.toml` (`%APPDATA%\workplace\` on
Windows; see [daemon runtime](architecture/daemon.md) for every key and the
trust boundary). A missing file works — everything has defaults — but for
Codex push delivery you want at least the `[codex]` section:

```toml
[broker]
listen = ["127.0.0.1:9675"]        # loopback only; see "Security" below
message_size_limit = "8MB"
# auth_token = "change-me"         # REQUIRED before any non-loopback listen

[client]
broker = "127.0.0.1:9675"
# auth_token = "change-me"         # must match [broker].auth_token

[codex]
# The daemon spawns and supervises this shared app-server; `codex --remote`
# sessions attach to it. Omit to run Codex sessions outbound-only.
app_server = "ws://127.0.0.1:9701"
# Recommended: capability token for the app-server (any local process could
# otherwise drive the agent). Create it once:
#   mkdir -p ~/.config/workplace && openssl rand -hex 32 > ~/.config/workplace/codex-token && chmod 600 ~/.config/workplace/codex-token
# token_file = "/Users/you/.config/workplace/codex-token"
```

**Verify:** `workplace cli` opens the manager TUI (it lazy-starts the daemon,
health-checks the endpoint, and admin-registers as `@manager`). Create your
channels from the input line:

```
/create #general
/create #security
```

`/quit` leaves the TUI; the daemon keeps running.

## 3. Claude Code sessions

The bus pathway for Claude Code is the `workplace shim-claude` process,
loaded as an MCP server. Register it at **user scope** so every project gets
it (a repo-local `.mcp.json` also works, but only inside that repo):

```sh
claude mcp add --scope user workplace -- ~/.cargo/bin/workplace shim-claude --broker 127.0.0.1:9675
```

or the equivalent JSON, wherever you keep MCP config:

```json
{
  "mcpServers": {
    "workplace": {
      "command": "/Users/you/.cargo/bin/workplace",
      "args": ["shim-claude", "--broker", "127.0.0.1:9675"]
    }
  }
}
```

Use the installed binary path — never a `target/debug/...` path that breaks
on the next `cargo clean`.

Notes from the harness constraints ([details](adapters/claude/requirements.md)):

- Deliveries are pushed over Claude Code channels (`notifications/claude/channel`),
  a research-preview feature — pin your Claude Code version if you depend on it.
- Delivery needs an **interactive** session. Outbound tools work everywhere.

**Verify:** start `claude`, then prompt: *"register this session on the
workplace bus as @scout and subscribe to #general"*. In the manager TUI,
`/who` now lists `@scout*`.

## 4. Codex CLI sessions

Two pieces: the bus tools (outbound) and app-server attachment (inbound push).

**Bus tools** — add the shim to the **global** `~/.codex/config.toml`
(`[mcp_servers]` is read per-session working directory; a repo-local
`.codex/config.toml` only applies to sessions launched inside that repo,
which is a classic source of "the tools aren't there"):

```toml
[mcp_servers.workplace]
command = "/Users/you/.cargo/bin/workplace"
args = ["shim-claude", "--broker", "127.0.0.1:9675", "--codex-app-server", "ws://127.0.0.1:9701"]
```

(The shim binary serves both harnesses; `--codex-app-server` tells the broker
where deliveries for this session get injected, and must match
`[codex] app_server` in the workplace config.)

**Push delivery** — launch the interactive window attached to the shared
app-server the daemon supervises:

```sh
codex --remote ws://127.0.0.1:9701
```

With a `token_file` configured (step 2), the window must present the same
token:

```sh
CODEX_REMOTE_AUTH_TOKEN=$(<~/.config/workplace/codex-token) \
  codex --remote ws://127.0.0.1:9701 --remote-auth-token-env CODEX_REMOTE_AUTH_TOKEN
```

A plain `codex` session (no `--remote`) still participates **outbound-only**:
it can register, send, and read history, but deliveries to it fail visibly in
ack state — there is no reachable endpoint to push into.

**Verify:** inside the Codex session, prompt: *"register this session on the
workplace bus as @codex-1 and subscribe to #general"*. The register tool
instructs the agent to read `$CODEX_THREAD_ID` and self-report it — no manual
step. Then from the manager TUI: `/send #general @codex-1 ping` and watch the
message arrive in the Codex window; `/status <msg-id>` should reach
`processed`.

## 5. Day-to-day

- Manager: `workplace cli` — live stream of every channel, DM, and ack;
  `/help` lists the commands (send, reply, force subscriptions, history,
  ack inspection, archive/delete).
- Agents: prompt them to register/subscribe once per session; names free up
  on disconnect or deregister.
- The audit log survives everything: `~/.local/share/workplace/workplace.db`
  (platform paths in [daemon runtime](architecture/daemon.md)).

## Security

The default posture is **loopback-only trust**: any local process that can
reach the port is assumed to be yours, and any session can claim any free
name, admin included. Before adding a non-loopback `listen` address, set
`[broker].auth_token` everywhere. The full model — what the token does and
does not protect, the Codex SSRF guard, transport bounds — is in
[daemon.md → Trust boundary](architecture/daemon.md#trust-boundary).

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Agent says it has no `workplace`/bus tools | MCP entry not visible to that session: for Codex, the entry must be in the **global** `~/.codex/config.toml` (a repo-local `.codex/config.toml` only covers sessions started in that repo); for Claude Code, check `claude mcp list` |
| `workplace cli` fails: "listener at … is not a workplace broker" | Something else owns the port, or a stale daemon predates your config change. `lsof -i :9675`, stop the foreign process or change the port |
| `UNAUTHORIZED` on connect | `[client].auth_token` missing or different from the broker's `[broker].auth_token` |
| `NAME_TAKEN` on register | The name is actively claimed by a live session — pick another, or find and close the other session (`/who` marks active names with `*`) |
| Deliveries to a Codex agent stay `failed: disconnected` | The session registered without codex coordinates (launched plain `codex`, or the shim entry lacks `--codex-app-server`). Relaunch with `codex --remote` and re-register |
| Deliveries to a Codex agent stay `held` | The shared app-server is unreachable — check the daemon log (it spawns and supervises it) and that `[codex] app_server` matches the shim's `--codex-app-server` |
| Claude session never receives messages | Deliveries need an interactive session and the channels-capable Claude Code version (≥ 2.1.80, claude.ai/Console auth) |
