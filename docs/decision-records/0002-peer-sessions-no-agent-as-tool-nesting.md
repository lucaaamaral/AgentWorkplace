# ADR-0002 — Peer sessions, no agent-as-a-tool nesting

**Status**: Accepted

## Context

The common way to combine Claude Code and Codex is nesting: register one as an MCP server/tool of the other (Codex's `codex mcp` server mode, OpenAI's codex-plugin-cc, Claude-as-MCP-server projects).

Nesting has structural problems for this project's goals:

- Each invocation is effectively a fresh call: the callee starts without accumulated context, and may be contaminated by whatever context the caller chooses to pass.
- Cross-call state does not accumulate — no persistent "coworker" relationship.
- The interaction is buried inside the caller's transcript, hard to audit, monitor, or interrupt from outside.

## Decision

Agents are long-lived peer sessions that exchange messages over the bus. Neither agent is registered as a callable tool of the other.

## Consequences

- Each agent's session history is continuous and owned by its harness; the bus carries only messages.
- Consultations can reference accumulated context ("the token cache we reviewed earlier") instead of re-establishing it per call.
- Every exchange is externally visible and interruptible by the human, satisfying the monitoring requirement.
