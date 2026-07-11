# AgentWorkplace

A local, push-based pub-sub message bus that lets multiple coding agents (Claude Code, OpenAI Codex CLI) communicate with each other and with a human operator, like coworkers with a manager.

Each agent keeps its own long-lived session and specialized context. Agents consult each other proactively by publishing to channels; the human monitors every exchange, participates on any channel, and course-corrects when a conversation drifts.

**Status: design phase.** The architecture and adapter requirements are documented; no implementation exists yet. The implementation language is Rust ([ADR-0013](docs/decision-records/0013-rust-implementation-language.md)), targeting macOS, Linux, and Windows. The shipped commands are `workplace daemon` (broker) and `workplace cli` (human interface). Versioning is semantic; the version number lives in [`VERSION`](VERSION) — the single authoritative source, read by anything that needs a version number. Licensed under the [AgentWorkplace License](LICENSE), which requires acknowledgement in any project that incorporates or derives from this work; the repository is private for now.

## Why this exists

Existing approaches to multi-agent interaction have structural problems this project avoids by design:

- **Agent-as-a-tool nesting** (Codex callable from Claude Code or vice versa) starts with a fresh context, sometimes contaminated from the current agent and is hard to audit — rejected.
- **Polling buses** burn a model turn on every poll timeout and block the agent from doing its own work while waiting — rejected.
- **Merged contexts** (one agent with everything loaded) defeats deliberate context curation — rejected.

Instead, AgentWorkplace uses each harness's native push mechanism to deliver messages into an already-running, context-bearing session:

- **Codex CLI**: the `codex app-server` protocol (`turn/start`, `turn/steer`, `thread/inject_items`) over WebSocket, with the interactive TUI attached to the same thread for native monitoring.
- **Claude Code**: *Claude Code channels* (`notifications/claude/channel`), the official mechanism for pushing events into a live session — a harness feature unrelated to AgentWorkplace channels, and confined to the Claude adapter.

All traffic transits a single local broker with an append-only store, so the full inter-agent conversation is auditable independently of any agent's context window.

See [docs/architecture/overview.md](docs/architecture/overview.md) for the design and [docs/decision-records/](docs/decision-records/README.md) for the numbered ADRs recording the rationale behind each choice.

## Quick setup (aspirational)

> Placeholder — this is the intended flow once implemented; nothing exists yet.

1. Start the broker: `workplace daemon`.
2. Create a channel per domain: `#security`, `#business`, `#performance`, `#general`.
3. One-time per machine, enable the bus pathway in each harness: the shim for Claude Code (loaded through Claude Code's own channels mechanism at launch) and, for Codex, app-server attachment plus the bus MCP entry in its config.
4. Launch your environments normally, one per desktop/window: `claude` for the security and business work, `codex` for the performance work.
5. From inside each session, register it on the bus — prompt the agent: *"register this session as sec-reviewer and subscribe to #security and #general"*. The agent calls the bus registration/subscription tools; the human can force or cancel subscriptions later.
6. Join as manager: `workplace cli` — live view of every channel, post to any channel or principal. (Web interface deferred.)

## Use cases

- **Specialist consultation.** The performance agent hits an authentication hot path and asks on `#security`: "is caching these tokens acceptable?" The security agent answers from its accumulated review context. One compact message crosses over instead of merging two contexts.
- **Parallel work with coordination.** Agents work independently on their own desktops and only synchronize at contract boundaries (interfaces, schemas, migration order) via the bus.
- **Manager oversight and course correction.** Every message is visible in the TUI and persisted. When two agents converge on a directionally wrong plan, the human posts a correction to the channel; both receive it as a pushed event.
- **Context-controlled staffing.** Subscriptions are the context boundary: agents subscribe to channels themselves as their work requires, and the human can force or cancel any subscription — final authority over what each agent hears stays with the manager, and every change is logged.
- **Audit after the fact.** The broker's store is the source of truth for who asked what and when — it survives session compaction, restarts, and window closures.

## Documentation

| Document | Content |
| --- | --- |
| [docs/architecture/overview.md](docs/architecture/overview.md) | Components, delivery flows, scope |
| [docs/decision-records/](docs/decision-records/README.md) | Numbered ADRs: design decisions and rejected alternatives |
| [docs/adapters/claude/requirements.md](docs/adapters/claude/requirements.md) | Claude Code adapter requirements (channels) |
| [docs/adapters/codex/requirements.md](docs/adapters/codex/requirements.md) | Codex CLI adapter requirements (app-server) |
