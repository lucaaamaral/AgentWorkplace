# Codex CLI adapter — requirements

Integration between the broker and a Codex CLI agent. Bus semantics (channels, addressing, registration model, tool surface, audit rules) are defined in the [architecture overview](../../architecture/overview.md); this document covers only what is specific to the Codex harness.

## Mechanism

Codex exposes its agent over the **`codex app-server`** protocol (JSON-RPC; stdio, WebSocket, unix-socket transports). Work is organized as durable **Threads** (survive restarts) containing **Turns** containing **Items**. A plain `codex` session runs its engine in-process with **no reachable endpoint** (verified: no listener, no socket, no child server) — push into it is impossible, exactly as a plain `claude` session is closed without the channels flag. The one interactive attach point is the **`codex --remote <addr>`** flag, which runs the interactive TUI against a shared `codex app-server --listen <addr>`; multiple clients share that server, so the human watches natively while the adapter injects. The broker acts as an ordinary client.

The shared app-server is **spawned and supervised by `workplace daemon`** (config `[codex] app_server`, see [daemon runtime](../../architecture/daemon.md)) — the human never runs it. Per-session opt-in is one launch flag, symmetric with Claude:

```
codex --remote ws://127.0.0.1:9701          # the human's interactive window
                 │
                 ├── codex app-server --listen ws://127.0.0.1:9701
                 │        (managed child of workplace daemon)
broker ──────────┘
  (adapter = second WebSocket client; delivers via turn/start on the
   human-owned thread; processed observed by polling thread/read)
```

Without the flag, a plain `codex` session still participates **outbound-only** through the bus MCP entry (register, send, subscribe, history on request); deliveries to it fail visibly in ack state.

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

- CX-7. The human starts the Codex environment with one flag (`codex --remote <addr>`, alias-able; one-time setup provides config and alias); no wrapper. On in-session registration the **agent self-reports its thread id** by reading `$CODEX_THREAD_ID` from its own shell environment (the register tool instructs this; `thread/list` is connection-scoped and cannot discover it — [findings](findings.md)); the bus MCP entry contributes the app-server endpoint from its configuration. The adapter records `(principal, app-server endpoint, thread id)` and attaches as a client for delivery.
- CX-8. Threads unload after ~30 idle minutes with no subscribers (`thread/closed` notification). This is normal, not an error; the adapter must `thread/resume` transparently on next delivery — history is preserved.
- CX-9. On broker restart, the adapter re-attaches to running app-servers from persisted state; `thread/read` reconciles missed events.
- CX-10. Thread id is the session identity. The adapter forwards liveness (loaded/unloaded, turn in progress, token usage from `turn/completed`) for display in the human interface. *Status: not yet implemented — the attach client is a non-owner and does not receive owner-routed events; liveness display needs a polling design first.*
- CX-11. Approvals are notify-only ([ADR-0012](../../decision-records/0012-approvals-are-notify-only.md)): approval requests (`item/commandExecution/requestApproval`, `item/fileChange/requestApproval`) are answered by the human's attached TUI. The adapter must never answer or forward them; it may surface "approval pending" as a notification to the human.

## Harness facts and constraints

| Constraint | Detail |
| --- | --- |
| Codex CLI version | Needs app-server WebSocket transport, multi-client mode, and TUI attach (March 2026 releases onward); pin and verify |
| Transport | WebSocket (or unix socket on POSIX) required for multi-client (TUI + adapter); stdio is single-client. On Windows, loopback WebSocket is the multi-client transport |
| Auth | Capability token via `codex app-server --ws-auth capability-token --ws-token-file <path>` (verified on 0.144.1); clients present `Authorization: Bearer <token>` on the WebSocket upgrade, the interactive window via `codex --remote <addr> --remote-auth-token-env <VAR>`. Should be set even for loopback (any local process could otherwise drive the agent) — `[codex] token_file` in the daemon config wires all three sides |
| Turn semantics | `turn/start` during an active turn: confirm protocol behavior (rejected vs queued) — drives CX-2 |
| Thread unload | 30-minute idle unload is server policy (drives CX-8) |

See also: [spike findings](findings.md).

## Risks / open questions

- **TUI + injected turns UX.** The TUI renders turns the adapter started. Verify the human can distinguish bus-initiated turns from their own prompts (delivery formatting should make the source explicit). Not yet tested — the spike used single-client stdio, not a WebSocket TUI sharing the thread.
- **`turn/steer` not adopted for delivery (settled).** Edge-case testing ([findings](findings.md)) showed steering an unrelated message into a running turn blends it into the agent's current work (no clean thread/attribution), and steering races turn completion. Delivery therefore stays `turn/start` serialized on idle (CX-2); `turn/steer` is reserved for a possible future override/interrupt class where derailing the current turn is the intent.
- **Delivery-path discovery at registration (settled).** The initiative is the harness's: registration arrives through the session's bus MCP entry, and must carry the delivery path the adapter records — app-server endpoint and thread id (CX-7). The MCP entry contributes the endpoint from the config it is launched with; the thread id is self-reported by the agent from `$CODEX_THREAD_ID` ([findings](findings.md) — `thread/list` is connection-scoped and cannot discover it). One app-server per agent vs one shared app-server with many threads is a deployment choice the adapter is agnostic to — it dials whatever registration reports, **constrained to loopback `ws://` endpoints** (the value arrives over the wire; a non-loopback URL is rejected at registration).

## References

- App-server official docs: https://developers.openai.com/codex/app-server
- App-server README: https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md
- Protocol guide (threads/turns/items, multi-client): https://codex.danielvaughan.com/2026/04/15/codex-app-server-complete-guide/
- WebSocket transport & remote access: https://codex.danielvaughan.com/2026/03/31/codex-cli-app-server-remote-websocket/
- Config reference (`[mcp_servers]`): https://github.com/openai/codex/blob/main/docs/config.md
