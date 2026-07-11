# ADR-0017 — Embedded SQLite as the broker store

**Status**: Accepted

## Context

The broker owns an append-only audit log ([ADR-0005](0005-broker-owned-append-only-audit-log.md)) that is the source of truth for messages, system events, and acknowledgment state. The store must:

- Survive broker restarts (held/ack state and full history are recovered from it).
- Serve cursor-based history queries and per-recipient ack lookups, not just appends.
- Ship inside a single binary on macOS, Linux, and Windows ([ADR-0011](0011-multiplatform-support.md)) with no service to install or administer.

Options considered:

- **Embedded SQLite** — a linked library, no server; SQL for cursors and ack queries; the most widely deployed and tested storage engine in existence.
- **Append-only JSONL file** — matches "append-only" literally, but every query (history cursors, ack state, `who`) needs hand-built indexing, and crash-consistency is homemade.
- **Rust-native embedded KV (sled, redb)** — embeds equally well, but young engines with no SQL; cursor and ack queries become manual index maintenance.
- **External database server** — rejected outright: a service dependency contradicts the single-daemon, zero-administration design.

## Decision

The store is **SQLite, embedded in `workplace daemon`** via `rusqlite` with the `bundled` feature: the SQLite amalgamation is compiled into the binary, so there is no system dependency and identical behavior on all three platforms (the only build-time requirement is a C compiler).

- **Single writer**: the broker process is the only thing that ever opens the database; the TUI and agents read through the broker's RPC surface, never the file. WAL mode for durability with concurrent reads inside the process.
- **Append-only is a broker-enforced discipline, not a physical property**: message and system records are never updated or deleted by the broker; acknowledgment state lives in separate mutable rows keyed to immutable records.
- The schema is versioned; the broker migrates forward on startup.
- Database location: the platform data directory ([daemon runtime](../architecture/daemon.md)), overridable in config.

Licensing is unencumbered: SQLite is public domain (no attribution, no copyleft, commercial use unrestricted); `rusqlite` is MIT. Neither interacts with the AgentWorkplace license.

## Consequences

- Zero-administration persistence: one file, backed up by copying, inspectable with standard SQLite tooling when debugging.
- Retention/compaction policy is deliberately unaddressed: append forever until a real size problem exists; revisit with data.
- An export command (e.g. JSONL dump) can be added later without touching the storage decision.
- The single-writer rule is load-bearing: any future component wanting the data must go through the broker's RPC surface, which preserves the audit path and visibility rules.
