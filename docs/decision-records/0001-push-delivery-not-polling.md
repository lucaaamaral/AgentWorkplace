# ADR-0001 — Push delivery, no polling

**Status**: Accepted

## Context

The bus must deliver messages to agents that are idle or busy with their own work. Existing multi-agent buses (e.g. murmur) use long-polling MCP tools: the agent calls `poll(timeout)` and blocks until a message arrives or the timeout fires.

While blocked, a poll costs nothing — but every timeout ends the tool call and the agent spends a model turn deciding to poll again, a steady-state token burn. Worse, a polling agent cannot do its own work while parked on the poll, which conflicts with agents working in parallel and being interrupted only when a coworker needs them.

MCP alone cannot solve this: the protocol is client-initiated, and server-to-client notifications only surface mid-request. An MCP-only design is structurally forced into polling.

## Decision

Messages are delivered by starting or steering a turn in the recipient's live session:

- Codex: `turn/start` / `turn/steer` via the app-server protocol.
- Claude Code: channel events (`notifications/claude/channel`).

The bus has no poll loop for agents. MCP is used only for the outbound tool surface.

## Consequences

- Idle agents consume zero tokens and remain free to work between messages.
- Delivery is necessarily harness-specific, which motivates the adapter layer ([ADR-0003](0003-per-harness-native-push-adapters.md)).
- The broker needs per-recipient delivery queues and acknowledgment state, since push targets can be busy or down.
