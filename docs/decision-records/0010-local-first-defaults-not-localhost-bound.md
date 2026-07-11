# ADR-0010 — Local-first defaults, not localhost-bound

**Status**: Accepted. Supersedes [ADR-0008](superseded/0008-v1-local-first-single-machine.md).

## Context

ADR-0008 scoped the design to a single machine with localhost-only transports. That overconstrained it: the workflow is local-first (multiple desktops on one workstation), but the architecture must not preclude agents or the human interface on other hosts.

## Decision

- **Localhost is the default**, not a boundary: broker transports bind to loopback out of the box, and all documented flows work on one machine.
- **Trust model is the local network**: per-principal identity, plus transport-level tokens where the underlying protocol supports them (Codex app-server bearer/capability token is set even locally, since any local process could otherwise drive the agent).
- Remote operation over the local network is supported by configuration (bind address), not by a separate mode. Anything beyond local-network trust (TLS, cryptographic principal auth, internet exposure) is out of scope until a concrete need exists.

The workflow remains local-first: localhost is the default and remote hosts are not a current target, merely not precluded.

## Consequences

- Transport choices must work across hosts from day one (broker transport decided in [ADR-0016](0016-tcp-broker-transport.md)).
- No redesign needed for a networked setup: the Codex app-server already supports WebSocket with bearer/JWT auth, and the broker reaches other hosts by configuration alone.
- Security posture is explicit: within the local network, principals are trusted to be who they claim beyond token checks; hostile-network operation is a future ADR.
