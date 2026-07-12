# AgentWorkplace

A local, push-based pub-sub message bus that lets multiple coding agents (Claude Code, OpenAI Codex CLI) communicate with each other and with a human operator, like coworkers with a manager.

Each agent keeps its own long-lived session and specialized context. Agents consult each other proactively by publishing to channels; the human monitors every exchange, participates on any channel, and course-corrects when a conversation drifts.

**Status: implemented, pre-release.** The broker daemon, TUI, Claude Code shim, and Codex adapter are implemented in Rust ([ADR-0013](docs/decision-records/0013-rust-implementation-language.md)), targeting macOS, Linux, and Windows; integration tests cover the RPC surface, delivery acks, Codex routing, and restart semantics. The shipped commands are `workplace daemon` (broker) and `workplace cli` (human interface). Versioning is semantic; the version number lives in [`VERSION`](VERSION) — the single authoritative source, read by anything that needs a version number. Licensed under the [AgentWorkplace License](LICENSE), which requires acknowledgement in any project that incorporates or derives from this work.

## Why this exists

Existing approaches to multi-agent interaction have structural problems this project avoids by design:

- **Agent-as-a-tool nesting** (Codex callable from Claude Code or vice versa) starts with a fresh context, sometimes contaminated from the current agent and is hard to audit — rejected.
- **Polling buses** burn a model turn on every poll timeout and block the agent from doing its own work while waiting — rejected.
- **Merged contexts** (one agent with everything loaded) defeats deliberate context curation — rejected.

Instead, AgentWorkplace uses each harness's native push mechanism to deliver messages into an already-running, context-bearing session:

- **Codex CLI**: the `codex app-server` protocol (`turn/start`, `turn/steer`, `thread/inject_items`) over WebSocket, with the interactive TUI attached to the same thread for native monitoring.
- **Claude Code**: *Claude Code channels* (`notifications/claude/channel`), the official mechanism for pushing events into a live session — a harness feature unrelated to AgentWorkplace channels, and confined to the Claude adapter.

All traffic transits a single local broker with an append-only store, so the full inter-agent conversation is auditable independently of any agent's context window.

See [docs/setup.md](docs/setup.md) to get running, [docs/architecture/overview.md](docs/architecture/overview.md) for the design, and [docs/decision-records/](docs/decision-records/README.md) for the numbered ADRs recording the rationale behind each choice.

## Quick start

Full walkthrough with per-harness wiring, auth, and troubleshooting: **[docs/setup.md](docs/setup.md)**. The short version:

1. **Install** (Rust stable required): `cargo install --path crates/workplace` — one binary, `workplace`.
2. **Start as manager**: `workplace cli` — lazy-starts the broker daemon, opens the TUI. Create channels: `/create #general`, `/create #security`, …
3. **Wire Claude Code** (once per machine): `claude mcp add --scope user workplace -- ~/.cargo/bin/workplace shim-claude --broker 127.0.0.1:9675`
4. **Wire Codex** (once per machine): add the same shim to the global `~/.codex/config.toml` `[mcp_servers]` (with `--codex-app-server ws://127.0.0.1:9701`), set `[codex] app_server = "ws://127.0.0.1:9701"` in `~/.config/workplace/config.toml`, and launch sessions with `codex --remote ws://127.0.0.1:9701` for push delivery.
5. **Register each session from inside it** — prompt the agent: *"register this session as @sec-reviewer and subscribe to #security and #general"*. Verify with `/who` in the TUI.

Every message, subscription change, and delivery ack streams live into the TUI and persists in the append-only store.

## Use cases

- **Specialist consultation.** The performance agent hits an authentication hot path and asks on `#security`: "is caching these tokens acceptable?" The security agent answers from its accumulated review context. One compact message crosses over instead of merging two contexts.
- **Parallel work with coordination.** Agents work independently on their own desktops and only synchronize at contract boundaries (interfaces, schemas, migration order) via the bus.
- **Manager oversight and course correction.** Every message is visible in the TUI and persisted. When two agents converge on a directionally wrong plan, the human posts a correction to the channel; both receive it as a pushed event.
- **Context-controlled staffing.** Subscriptions are the context boundary: agents subscribe to channels themselves as their work requires, and the human can force or cancel any subscription — final authority over what each agent hears stays with the manager, and every change is logged.
- **Audit after the fact.** The broker's store is the source of truth for who asked what and when — it survives session compaction, restarts, and window closures.
