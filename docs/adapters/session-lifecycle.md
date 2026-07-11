# Session lifecycle — common contract

How a harness session's presence on the bus is established, maintained, and lost. This document defines only what is common to every adapter; the mechanics that drive the transitions are harness-specific and live in each adapter's own `session-lifecycle.md`:

- [Claude Code](claude/session-lifecycle.md)
- [Codex CLI](codex/session-lifecycle.md)

Every future adapter must ship its own session-lifecycle document answering the questions in [Adapter obligations](#adapter-obligations).

## States

`connected` (anonymous) → `registered` → `deregistered` | `disconnected`

- A session connects anonymous; `register` binds it to a principal ([tool contract](../architecture/message-model.md#tool-contract)).
- `deregister` is the explicit, logged unbind; connection termination is the implicit one. Either releases the principal name for re-registration.
- Registration, denial, deregistration, and disconnection are `system` events in the log.

## Relation to the message lifecycle

Presence determines the fate of a delivery but never advances it: a disconnected recipient's deliveries **fail** (no store-and-forward across sessions), a present-but-busy recipient's deliveries are **held** until the harness accepts input. Relay and processing come from delivery attempts and harness protocol signals — see the [acknowledgment lifecycle](../architecture/message-model.md#acknowledgment-lifecycle).

## Adapter obligations

Each adapter's session-lifecycle document must answer, for its harness:

1. **Presence signal** — what observable event tells the broker the session exists, and what tells it the session is gone.
2. **Session ↔ connection mapping** — whether the bus-facing connection's lifetime actually tracks the session's, and every known case where they diverge (suspends, reconnects, idle unloads) with the required adapter behavior.
3. **Busy signal** — whether "connected but not currently able to accept a delivery" is observable, and how.
4. **Identity carrier** — which component holds the session→principal binding after registration.
