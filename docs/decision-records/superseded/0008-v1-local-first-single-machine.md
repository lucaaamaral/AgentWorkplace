# ADR-0008 — v1 is local-first, single machine

**Status**: Superseded

**Superseded by**: [ADR-0010](../0010-local-first-defaults-not-localhost-bound.md)

## Context

The concrete workflow is multiple desktops/workspaces on one workstation, one agent per desktop. Networked multi-host operation would force authentication, TLS, and message-security decisions that add nothing to the core problem of push delivery, context isolation, and audit.

## Decision

v1 is localhost only: unix sockets and loopback WebSocket, local trust model, per-principal identity but no cryptographic auth between local processes. Exception: the Codex app-server bearer/capability token is set even locally, since any local process could otherwise drive the agent.

## Consequences

- No remote monitoring in v1; the human interface runs on the same machine.
- The Codex app-server already supports bearer/JWT auth and remote transports, so a later multi-host version has a path without redesign; the broker's socket layer would need auth added.
- Simpler failure model: broker restart and adapter re-attach are the only distributed-systems concerns.
