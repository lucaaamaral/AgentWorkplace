# ADR-0005 — Broker-owned append-only audit log

**Status**: Accepted

## Context

Auditability is a hard requirement: who asked whom, what was answered, and what the human corrected must be reconstructable after the fact. Agent transcripts cannot serve this role — context windows compact, sessions restart, terminals close, and each harness stores history in its own opaque format.

## Decision

Every message is persisted by the broker to an append-only SQLite log *before* delivery. The log, not any agent transcript, is the audit source of truth.

## Consequences

- The human interface's live view, history replay, and delivery-state inspection are all reads of the same log.
- All agent replies must transit the broker (`send` tool); adapters must not offer side channels that bypass the log, otherwise the audit trail has gaps.
- The log survives broker restarts and is independent of any session's lifetime.
