# Codex CLI — session lifecycle

Harness-specific mechanics behind the [common session lifecycle contract](../session-lifecycle.md).

## Presence signal

Two independent signals, used together:

- The bus MCP server entry (`[mcp_servers.*]` in `~/.codex/config.toml`) is spawned by the session over stdio — its broker connection existing means the session is up; its drop means the session is gone.
- The delivery path is separate: the adapter is a WebSocket client of the session's app-server, attached to the agent's thread. App-server reachability and thread state qualify *deliverability*, not existence.

## Session ↔ connection mapping

The MCP-entry connection tracks the session's process lifetime, as on the Claude side.

Known divergences on the delivery path:

- `thread/closed` (idle unload after ~30 minutes with no activity/subscribers) is **not** absence: the session/thread is restorable with full history via `thread/resume`. The adapter must resume transparently on next delivery and never report unload as disconnection.
- App-server WebSocket drop while the MCP-entry connection lives: delivery is impaired but the session is present; messages `held`, adapter reconnects/resumes.
- Broker restart: the adapter re-attaches from persisted `(principal, endpoint, thread id)` state and reconciles missed events via `thread/read`.

## Busy signal

Observable in-band: the app-server event stream shows a turn in progress (`item/*` deltas until `turn/completed`). Whether `turn/start` during an active turn is rejected or queued by the protocol is a spike question; the finding lands here and sets the broker-vs-protocol holding split.

## Identity carrier

The adapter records `(principal, app-server endpoint, thread id)` at `register`; the thread id is the session identity on the delivery path. Unbind by `deregister` or by the MCP-entry connection dropping.
