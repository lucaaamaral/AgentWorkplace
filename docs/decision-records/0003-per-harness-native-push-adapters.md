# ADR-0003 — Per-harness native push adapters

**Status**: Accepted

## Context

Given push delivery ([ADR-0001](0001-push-delivery-not-polling.md)), the bus needs a way to start a turn in each harness. Options considered:

- **Vendor-native mechanisms**: `codex app-server` protocol for Codex; channels (`notifications/claude/channel`) for Claude Code. Official, acknowledged, with delivery semantics.
- **Terminal injection** (tmux `send-keys`): genuine push and harness-agnostic, but fragile — messages can interleave with user typing, formatting is mangled by the TUI, there is no delivery acknowledgment, and it depends on a specific terminal multiplexer.

## Decision

Use each vendor's official mechanism, wrapped in a per-harness delivery adapter with a narrow interface (`deliver(principal, message) → ack`). Terminal injection is kept only as a documented fallback (see [ADR-0007](0007-accept-claude-channels-research-preview.md)).

## Consequences

- Adapter designs are asymmetric: the broker is a WebSocket *client* of each Codex app-server, but the *spawned channel-server* of each Claude Code session. The adapter interface hides this from the broker core.
- Adding another harness (any agent CLI with a push or session-drive mechanism) means writing one adapter, no broker changes.
- The bus depends on vendor protocol stability; version pinning and thin adapters contain the exposure.
