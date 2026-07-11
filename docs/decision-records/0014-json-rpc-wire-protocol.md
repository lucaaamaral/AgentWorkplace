# ADR-0014 — Wire protocol is JSON-RPC 2.0

**Status**: Accepted

## Context

Every component boundary the project controls — shim ↔ broker, human interface ↔ broker, future adapters ↔ broker — needs one framing for requests, responses, errors, and server-initiated push. The boundaries the project does not control already have one: MCP is JSON-RPC 2.0, and the Codex app-server protocol is JSON-RPC 2.0.

The alternative, plain newline-delimited JSON, would require inventing request/response correlation, an error shape, and push framing ad hoc — reinventing exactly the parts of JSON-RPC that matter, without the specification.

JSON-based framing is also the friendliest shape for LLM harnesses to expose and relay as tools to the underlying models.

## Decision

JSON-RPC 2.0 on every broker connection, over all supported transports (local IPC — unix socket / named pipe / loopback TCP — and WebSocket). Broker-to-client push (deliveries to the shim, live tail to the human interface) uses JSON-RPC notifications.

## Consequences

- One grammar at every boundary in the system, internal and external.
- Requests are correlated and errors are structured by specification; nothing bespoke to document or debug.
- Rust implementation is plain serde over the chosen transports; no protocol library lock-in required.
