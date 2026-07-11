# ADR-0016 — Single TCP transport for broker connections

**Status**: Accepted

## Context

Early drafts assumed platform-specific local IPC for broker connections (unix domain sockets on POSIX, named pipes or loopback TCP on Windows) without a decision record. Revisiting under the actual requirements:

- Agents on other machines are a wanted deployment ([ADR-0010](0010-local-first-defaults-not-localhost-bound.md)): a transport that cannot cross hosts disqualifies itself.
- Multiplatform support ([ADR-0011](0011-multiplatform-support.md)): platform-specific IPC forces a three-way transport abstraction (socket / pipe / TCP) to build, test, and document.
- There is no authentication layer for now — the trust model is the operator's machine or a protected network. Unix sockets' one real advantage, filesystem-permission access control, protects against a threat (other users on a shared machine) this deployment does not have.

## Decision

All broker connections use a single transport: **TCP**, carrying newline-delimited JSON-RPC 2.0 ([ADR-0014](0014-json-rpc-wire-protocol.md)).

- The broker binds a **configurable list of addresses**; the default is loopback only. Agents on another machine are reached by adding a network-reachable bind — more listeners, not more protocol stacks.
- Clients (CLI, adapters, shims) dial a configured `host:port`.
- No unix sockets, no named pipes, on any platform.
- **All other documents stay transport-agnostic**: they say "the broker connection" and nothing more. The concrete transport is recorded here; concrete endpoint configuration lives in the [daemon runtime](../architecture/daemon.md) document only.

## Consequences

- The per-platform IPC abstraction disappears: one transport code path, identical config and docs on macOS, Linux, and Windows. This refines the local-IPC point of ADR-0011 — the "no platform-exclusive mechanism" decision is now satisfied trivially.
- Any process that can reach a bind can connect. Accepted under the current trust model; a security layer remains deliberately deferred until there are users beyond the operator.
- `workplace cli` lazy-start applies only when the configured endpoint is local; a remote broker that is down is an error, never a shadow local daemon.
- A WebSocket listener can be added later for the web interface as an additional bind type without revisiting this decision.
- The Codex app-server's own WebSocket transport is a harness fact on the adapter's far side, unaffected by this ADR.
