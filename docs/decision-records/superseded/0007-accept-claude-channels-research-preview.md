# ADR-0007 — Accept research-preview status of Claude Code channels

**Status**: Superseded

**Superseded by**: [ADR-0015](../0015-claude-channels-interactive-only.md)

## Context

Channels are the only sanctioned push path into a running Claude Code session; external injection APIs are open feature requests (anthropics/claude-code #27441, #24947). But channels are a research preview with real caveats:

- Requires Claude Code ≥ 2.1.80 and Anthropic auth via claude.ai or Console API key — unavailable on Bedrock, Vertex, and Foundry.
- `--channels` only accepts Anthropic-allowlisted plugins during the preview; a custom shim runs under `--dangerously-load-development-channels`.
- The flag syntax and channel protocol contract may change.

## Decision

Build the Claude adapter on channels anyway. Contain the risk: pin the Claude Code version, keep the shim thin so protocol churn is localized, and document fallbacks.

## Consequences

- The Claude adapter carries a stability caveat until channels exit preview; upgrades of Claude Code must be validated against the shim.
- Fallbacks, in order of preference if channels regress or are unavailable in a given auth environment:
  1. **Headless session-resume driving** (`claude -p --resume <session-id>`): broker starts a turn per delivery; loses the interactive TUI, monitoring moves entirely to the bus's human interface.
  2. **Terminal injection** (tmux `send-keys`): keeps the TUI but fragile and ack-less ([ADR-0003](../0003-per-harness-native-push-adapters.md)); last resort.
