# Codex CLI adapter — requirements

Integration between the broker and a Codex CLI agent. Bus semantics (channels, addressing, registration model, tool surface, audit rules) are defined in the [architecture overview](../../architecture/overview.md); this document covers only what is specific to the Codex harness.

## Mechanism

Codex exposes its agent over the **`codex app-server`** protocol (JSON-RPC; stdio, WebSocket, unix-socket transports). Work is organized as durable **Threads** (survive restarts) containing **Turns** containing **Items**. The app-server supports multiple simultaneous clients on one thread, so the interactive TUI and the adapter attach together: the human watches natively while the adapter injects. The broker acts as an ordinary client.

```
Codex TUI ──┐
            ├── codex app-server (ws://127.0.0.1:<port>) ── thread <id>
broker ─────┘
  (adapter = WebSocket client; delivers via turn/start)
```

Protocol operations the adapter relies on:

| Operation | Use |
| --- | --- |
| `thread/start`, `thread/resume` | Create/reattach the agent's persistent thread |
| `turn/start` | Deliver a bus message as a new user turn (primary delivery path) |
| `turn/steer` | Inject guidance into a turn already in progress, without interrupting |
| `turn/interrupt` | Cancel the current turn (human emergency stop) |
| `thread/inject_items` | Append context items to model-visible history without starting a turn |
| `thread/read`, `thread/list` | Recover history/thread ids after restarts |
| `item/*` deltas, `turn/completed` | Event stream for liveness, busy-state, and ack tracking |

## Adapter requirements

### Inbound (broker → session)

- CX-1. Deliver a message via `turn/start` on the agent's thread, formatted so the event identifies the bus channel, the sender, the body, and how to reply through the bus tools.
- CX-2. Delivery must serialize on the thread: `turn/start` while a turn is in progress is accepted but the turn never runs ([findings](findings.md)). The adapter waits for the thread to be idle (`turn/completed` / `thread/status: idle`) before `turn/start`; it never fires while busy. All holding is the broker's — the protocol does not queue.
- CX-3. Ack mapping: `turn/start` accepted → `delivered`; `turn/completed` for that turn → `processed`.
- CX-4. `thread/inject_items` may be used for context drops that should not trigger a reaction. Use sparingly — injected items bypass the agent's explicit attention.
- CX-5. If the app-server is unreachable while the session is still present (MCP-entry connection alive), recipients are `held`; the adapter reconnects, resumes the thread (`thread/resume`), and drains. If the session itself is disconnected, deliveries fail per the message model — no store-and-forward.

### Outbound (session → broker)

- CX-6. The bus tool surface (defined in the overview) is exposed to the session as a bus MCP server entry in `~/.codex/config.toml` (`[mcp_servers.*]`), installed by one-time setup. All outbound traffic goes through the broker.

### Session binding and thread lifecycle

States and presence semantics: [common contract](../session-lifecycle.md) · [Codex specifics](session-lifecycle.md).

- CX-7. The human starts the Codex environment normally, attached to an app-server (one-time setup provides the config/alias); no wrapper. On in-session registration, the call must carry enough to identify the session (thread id, or metadata resolved via `thread/list`); the adapter records `(principal, app-server endpoint, thread id)` and attaches as a client for delivery.
- CX-8. Threads unload after ~30 idle minutes with no subscribers (`thread/closed` notification). This is normal, not an error; the adapter must `thread/resume` transparently on next delivery — history is preserved.
- CX-9. On broker restart, the adapter re-attaches to running app-servers from persisted state; `thread/read` reconciles missed events.
- CX-10. Thread id is the session identity. The adapter forwards liveness (loaded/unloaded, turn in progress, token usage from `turn/completed`) for display in the human interface.
- CX-11. Approvals are notify-only ([ADR-0012](../../decision-records/0012-approvals-are-notify-only.md)): approval requests (`item/commandExecution/requestApproval`, `item/fileChange/requestApproval`) are answered by the human's attached TUI. The adapter must never answer or forward them; it may surface "approval pending" as a notification to the human.

## Harness facts and constraints

| Constraint | Detail |
| --- | --- |
| Codex CLI version | Needs app-server WebSocket transport, multi-client mode, and TUI attach (March 2026 releases onward); pin and verify |
| Transport | WebSocket (or unix socket on POSIX) required for multi-client (TUI + adapter); stdio is single-client. On Windows, loopback WebSocket is the multi-client transport |
| Auth | Bearer/capability token supported and should be set even for loopback (any local process could otherwise drive the agent) |
| Turn semantics | `turn/start` during an active turn: confirm protocol behavior (rejected vs queued) — drives CX-2 |
| Thread unload | 30-minute idle unload is server policy (drives CX-8) |

See also: [spike findings](findings.md).

## Risks / open questions

- **TUI + injected turns UX.** The TUI renders turns the adapter started. Verify the human can distinguish bus-initiated turns from their own prompts (delivery formatting should make the source explicit). Not yet tested — the spike used single-client stdio, not a WebSocket TUI sharing the thread.
- **`turn/steer` usefulness.** Confirmed to work (requires `expectedTurnId`; appends guidance to the active turn rather than interrupting — [findings](findings.md)). Deliveries still wait for idle (CX-2); evaluate later whether any delivery class justifies steering into a running turn.
- **Port/endpoint management.** One app-server per agent vs one shared app-server with multiple threads — decide at implementation; per-agent isolates failures, shared simplifies discovery.

## References

- App-server official docs: https://developers.openai.com/codex/app-server
- App-server README: https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md
- Protocol guide (threads/turns/items, multi-client): https://codex.danielvaughan.com/2026/04/15/codex-app-server-complete-guide/
- WebSocket transport & remote access: https://codex.danielvaughan.com/2026/03/31/codex-cli-app-server-remote-websocket/
- Config reference (`[mcp_servers]`): https://github.com/openai/codex/blob/main/docs/config.md
