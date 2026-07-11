# ADR-0015 — Claude adapter on Claude Code channels, interactive-only

**Status**: Accepted. Supersedes [ADR-0007](superseded/0007-accept-claude-channels-research-preview.md).

## Context

The Claude adapter delivers bus messages into a running Claude Code session. Claude Code channels (`notifications/claude/channel`) are the only sanctioned push path into a live session; external injection APIs remain open feature requests (anthropics/claude-code #27441, #24947).

Channels are a research preview with real constraints:

- Requires Claude Code ≥ 2.1.80 and Anthropic auth via claude.ai or Console API key — unavailable on Bedrock, Vertex, and Foundry.
- `--channels` accepts only Anthropic-allowlisted plugins during the preview; a custom shim runs under `--dangerously-load-development-channels`.
- Flag syntax and protocol contract may change.

The spike ([findings](../adapters/claude/findings.md)) confirmed the delivery path: a pushed event wakes an idle interactive session and the model acts within seconds. Channel events are delivered only to interactive sessions — which is the project's sole operating mode: interactive sessions the human watches and steers.

## Decision

Build the Claude adapter on Claude Code channels. The adapter targets interactive sessions — the only mode this project supports. Contain the preview risk: pin the Claude Code version, keep the shim thin so protocol churn is localized.

There is no secondary delivery path. If channels are unavailable in an environment (unsupported auth backend, or a preview regression), the Claude adapter is unavailable in that environment until channels are.

## Consequences

- The Claude adapter carries a stability caveat until channels exit preview; Claude Code upgrades must be validated against the shim.
- Auth environments without channels (Bedrock, Vertex, Foundry) cannot participate through the Claude adapter.
- Monitoring is native: the human watches the interactive session directly, consistent with the project's operating model.
