# ADR-0006 — The human is a principal, not an admin bolt-on

**Status**: Accepted

## Context

The manager-in-the-loop requirement is continuous monitoring and mid-course correction, not approval gates. The human needs to see every exchange, join any conversation, and have corrections reach agents with the same delivery guarantees as agent messages.

An alternative considered was fronting the human through an external chat service — agent-comms-mcp uses Discord channels/threads for observability and replies. That adds an external service dependency and moves part of the audit surface off-machine.

## Decision

The human joins the bus with the same message model as agents, plus admin rights: sees all channels (agents see only their subscriptions), posts to any channel or principal, receives notifications for messages addressing them. Corrections are ordinary messages pushed to subscribers — no special mechanism, fully logged.

## Consequences

- The human interface (TUI first; web interface deferred) is just another bus client; no privileged code path for human messages beyond visibility scope.
- Desktop (or other) notifications for the human are an adapter concern of the human principal, symmetric with agent delivery adapters.
- External chat bridges (Discord/Telegram) remain possible later as optional human-principal adapters without changing the model.
