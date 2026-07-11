# Claude Code — session lifecycle

Harness-specific mechanics behind the [common session lifecycle contract](../session-lifecycle.md).

## Presence signal

The channel shim is spawned by the Claude Code session itself (through the Claude Code channels mechanism, `--channels` at launch) and connects to the broker. Shim connected and handshaken = session present. The broker observes session loss as the shim's connection dropping.

## Session ↔ connection mapping

The shim is a stdio subprocess of the session: session exit closes its stdio, the shim terminates (or detects EOF and closes its broker connection), and the broker sees the drop. Lifetimes track each other to within process-teardown latency.

Known divergences:

- Shim crash without session exit: the broker sees a disconnect while the session still runs. The session-side effect is that Claude Code loses the channel connection; whether the harness restarts the plugin or the session must be relaunched with `--channels` is a spike/implementation finding to be recorded here.
- No suspend/resume concept is known for an interactive session; nothing to map.

## Busy signal

None at the connection level: an MCP channel server is not told whether the session is mid-turn, and notifications carry no receipt ([findings](findings.md)). Per the Claude Code channels reference, events queue into the session and are grouped on the next turn when it is busy — so the harness absorbs the busy case and the adapter does not need a busy signal to avoid loss. Requires an interactive session ([ADR-0015](../../decision-records/0015-claude-channels-interactive-only.md)).

## Identity carrier

The shim holds the session→principal binding after `register` and stamps it on all subsequent traffic. One shim instance per session; the binding dies with the connection (implicit unbind) or with `deregister`.
