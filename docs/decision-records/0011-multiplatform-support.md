# ADR-0011 — Multiplatform support: macOS, Linux, Windows

**Status**: Accepted

## Context

Agent workstations run on all three major desktop platforms. Some mechanisms discussed in the design are platform-specific: unix domain sockets, for example, do not exist on Windows in all runtimes.

## Decision

macOS, Linux, and Windows are all supported targets. The core (broker, adapters, TUI) must not depend on a platform-exclusive mechanism without an equivalent on the other platforms:

- **Broker transport** is a single mechanism portable across all three platforms ([ADR-0016](0016-tcp-broker-transport.md)).
- **Delivery paths are inherently portable**: Codex app-server WebSocket and Claude Code channels (stdio subprocess) work on all three platforms.
- Implementation language/runtime must have first-class support on all three platforms (constraint on the future implementation choice).

## Consequences

- Path handling, process spawning, and service/daemon lifecycle need per-platform care (launchd/systemd/Windows service, or foreground-only initially).
- CI must eventually cover all three platforms; until then, platform gaps are tracked as known issues rather than accepted design.
