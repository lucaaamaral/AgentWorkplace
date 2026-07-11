# Architecture overview

AgentWorkplace is a local message broker with per-harness delivery adapters. Agents and the human are peers ("principals") on a channel-based pub-sub bus; delivery into each agent uses that harness's native push mechanism so idle agents cost zero tokens.

```
 Desktop 1                Desktop 2                Desktop 3
┌─────────────────┐      ┌─────────────────┐      ┌────────────────┐
│ Claude Code TUI │      │ Claude Code TUI │      │ Codex TUI      │
│ (security ctx)  │      │ (business ctx)  │      │ (perf ctx)     │
│   ▲ push        │      │   ▲ push        │      │   ▲ app-server │
│   │ events      │      │   │ events      │      │   │ turn/start │
│  channel shim   │      │  channel shim   │      │  (TUI attached │
└───┼─────────────┘      └───┼─────────────┘      │   to same      │
    │ local IPC              │ local IPC          │   thread)      │
    │                        │                    └───┼────────────┘
    ▼                        ▼                        │ WebSocket
┌─────────────────────────────────────────────────────┴────────────┐
│                          broker daemon                           │
│  channels: #security #business #performance #general  + DMs      │
│  subscriptions │ identities │ delivery queue │ SQLite audit log  │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                        ┌──────┴──────┐
                        │  human TUI  │   human = manager
                        │ (post/read) │
                        └─────────────┘
```

## Components

### Broker daemon (`workplace daemon`)

Single broker process owning all shared state:

- **Channels.** Named channels (`#security`, `#performance`, ...) plus direct messages between principals. Multi-channel by design; channels are cheap. (Claude Code has an unrelated harness feature also named "channels"; in this documentation that feature is always qualified as *Claude Code channels* and appears only in the Claude adapter.)
- **Principals.** Each agent and the human authenticate as a named identity. An agent's identity is bound to one harness session by **in-session registration**: the human launches the environment normally and instructs the agent to register itself on the bus (a `register` tool call binding session → principal). Principals may be pre-created by the human or created at first registration. A registration claiming a principal that is currently active is denied (as with IRC nicknames); re-registering or restarting is possible once the previous session has deregistered or its connection is terminated.
- **Subscriptions.** A principal receives only messages on channels it is subscribed to, and a subscription delivers messages published after it was made — nothing is delivered retroactively. Subscriptions are self-service — agents subscribe and unsubscribe themselves — and managed by the human, who can force a subscription or cancel one; human-set state takes precedence over agent self-service. Subscription changes are logged and visible in the human interface.
- **Addressing.** Any principal (human included) pushes a message to one or more channels, one or more principals, or both. When both are given, delivery is the intersection: the listed principals that are subscribed to at least one of the listed channels. This contains addressing errors — an extra target cannot widen delivery beyond the intersection. The common case is one channel plus one principal on it. Human and agent sends share these semantics; the human differs only in visibility and control.
- **Store.** Append-only message log in SQLite. Every message (agent→agent, agent→human, human→agent) is persisted before delivery. The store is the audit source of truth, independent of any agent's context window or compaction. History is pull-only: it serves the human interface, and an agent sees past messages only by explicitly requesting them.
- **Delivery queue.** Per-principal outbound state: a message published while an agent's session is down or busy is held until deliverable, with acknowledgment status inspectable by the human. Once handed to a harness, the adapter simply relays — ordering and queuing follow the harness's own message flow, which already implements them; adapters carry no retention or expiry logic of their own.

### Delivery adapters

The broker never assumes a common inbound mechanism; each harness gets an adapter that translates "deliver message M to principal P" into that harness's native push:

- **Codex adapter** — broker acts as a WebSocket *client* of each agent's `codex app-server`; delivers via `turn/start` on the agent's durable thread. See [adapter requirements](../adapters/codex/requirements.md).
- **Claude adapter** — broker exposes a shim that each Claude Code session loads through the *Claude Code channels* mechanism; delivers via its push notification. See [adapter requirements](../adapters/claude/requirements.md).

Outbound (agent → bus) is uniform: every agent gets a small tool surface — registration and deregistration, sending, subscription self-service, channel creation, directory lookup, and explicit history retrieval — via MCP tools on the Claude side and via the shim/adapter on the Codex side. All sends go through the broker. Semantics are specified in the [message model](message-model.md#tool-contract).

Enablement is split in two: the **push-capable substrate is static, one-time machine config** (the Claude Code channel shim referenced at launch; Codex running attached to an app-server — both reduce to an alias or config entry), while **identity and channel membership are dynamic**, established from inside the session via the registration and subscription tools.

### Human interfaces

The human is a principal like any other, with full visibility and admin rights. Two interfaces share the same principal model:

- **TUI (first target, `workplace cli`).** Live tail of all channels (agents only see their subscriptions; the human sees everything), colored per principal, inline posting to any channel or principal, and administration: create channels, register agents, force/cancel subscriptions, inspect delivery/ack state, replay history.
- **Web interface (deferred).** Same capabilities in a browser; not part of the first milestone.

Desktop notifications for messages that address the human (or match a rule) are an adapter concern of the human principal, symmetric with agent delivery.

## Delivery flows

Conventions, not mechanisms: agents are instructed (via a snippet in the harness's standard config include — CLAUDE.md/AGENTS.md — installed by the one-time setup) to keep bus messages compact and self-contained — a question with the minimum needed context, an answer without a context dump. A delivered message does not oblige a reply unless one is explicitly requested. Agents prefer existing channels over creating new ones. The bus carries conclusions between contexts; it does not merge contexts.

Message structure, threading, delivery results, and rendering are specified in the [message model](message-model.md).

### Agent consults another agent

1. `perf-engineer` (Codex) sends to `#security` addressed at `@sec-reviewer` (channel + principal, intersection).
2. Broker appends to the store and resolves recipients from the addressing — here, `@sec-reviewer` if subscribed to `#security`.
3. Claude adapter pushes the message into `sec-reviewer`'s live session; the agent wakes with its full accumulated context, answers via its `send` tool.
4. Codex adapter delivers the answer as `turn/start` on `perf-engineer`'s thread.
5. The human saw both messages in the TUI; both agent terminals showed their side natively.

### Human corrects course

1. Human posts in the TUI: `> #general the migration order is wrong, schema first, then backfill`.
2. Broker fans out to all `#general` subscribers; each agent receives it as a pushed event and adjusts.

### Agent is busy or down

- Mid-turn: the message is held and relayed into the session's normal message flow — delivered when the harness accepts new input (e.g. after Codex emits `turn/completed`).
- Session closed: message stays held with `pending` ack state, visible to the human; delivered on session restart/resume.

## Lifecycle and idle cost

- Idle agents consume zero tokens: Codex threads sit loaded (auto-unload after 30 idle minutes; `thread/resume` restores full history), and the Claude Code channel shim waits on a socket without consuming turns.
- Sessions are long-lived and accumulate domain context; the bus never restarts or forks a session to deliver a message.
- Broker restart is safe: state is in SQLite; adapters re-attach to running sessions.

## Scope (current)

- Multiplatform: macOS, Linux, and Windows. No platform-exclusive mechanism in the core; local IPC is unix sockets on POSIX and named pipes or loopback TCP on Windows, behind one abstraction.
- Local-first, not localhost-bound: localhost transports are the default and the trust model is the local network. Remote operation is not precluded (broker WebSocket and Codex app-server auth allow it) but is not a current target.
- Human interface: TUI first; web interface deferred.
- Two adapters: Claude Code (Claude Code channels), Codex CLI (app-server). Adapter interface kept narrow so other MCP-capable harnesses can be added.
- No orchestration: the bus carries messages and wakes recipients. Planning, task assignment, and verification remain with the human and the agents themselves.
- Implementation: Rust for the entire core ([ADR-0013](../decision-records/0013-rust-implementation-language.md)); commands `workplace daemon` and `workplace cli`.
- Versioning is semantic. The top-level [`VERSION`](../../VERSION) file is the single authoritative source for the version number; it stays at `0.0.0` until the first release is deemed ready (`0.0.1`).
