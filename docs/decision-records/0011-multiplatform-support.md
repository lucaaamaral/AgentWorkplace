# ADR-0011 — Multiplatform support: macOS, Linux, Windows

**Status**: Accepted

## Context

Agent workstations run on all three major desktop platforms. Several mechanisms discussed in the design are platform-specific: unix domain sockets do not exist on Windows in all runtimes, and the tmux terminal-injection fallback ([ADR-0003](0003-per-harness-native-push-adapters.md)) is POSIX-only.

## Decision

macOS, Linux, and Windows are all supported targets. The core (broker, adapters, TUI) must not depend on a platform-exclusive mechanism without an equivalent on the other platforms:

- **Local IPC** is an abstraction: unix domain sockets on POSIX, named pipes or loopback TCP on Windows.
- **Primary delivery paths are inherently portable**: Codex app-server WebSocket and Claude Code channels (stdio subprocess) work on all three platforms.
- **POSIX-only fallbacks** (tmux injection) may exist but must be documented as such and never be the only path on any platform.
- Implementation language/runtime must have first-class support on all three platforms (constraint on the future implementation choice).

## Consequences

- Windows delivery fallback for Claude Code, if channels are unavailable, is headless session-resume driving (ADR-0007 fallback 1), not terminal injection.
- Path handling, process spawning, and service/daemon lifecycle need per-platform care (launchd/systemd/Windows service, or foreground-only initially).
- CI must eventually cover all three platforms; until then, platform gaps are tracked as known issues rather than accepted design.
