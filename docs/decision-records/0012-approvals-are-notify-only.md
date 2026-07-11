# ADR-0012 — Harness approval prompts are notify-only

**Status**: Accepted

## Context

A bus-delivered message can start work in a session that hits the harness's own permission/approval gate (Claude Code permission prompts, Codex `requestApproval` items) while the human is on another desktop, stalling that session until answered.

Both harnesses offer a relay path: Claude Code channels declare a permission-relay capability, and the Codex app-server delivers approval requests to any attached client. Relaying approvals through the bus would let the human answer from anywhere — but it moves approval authority onto the bus (whoever can write to the bus can approve tool use), duplicates each harness's approval UX, and couples the adapters to two unstable approval protocols.

## Decision

Approvals are **notify-only**. The bus may notify the human that a session is waiting on an approval, but the approval itself is always answered in that session's own native interface. Adapters never answer, forward, or proxy approval requests.

## Consequences

- A pushed task can stall until the human reaches that desktop; the notification bounds how long the stall goes unnoticed.
- Approval authority never transits the bus, so bus access does not imply tool-execution authority in any session.
- Adapters stay decoupled from the harnesses' approval protocols; only the inexpensive "an approval is pending" signal is consumed where available.
