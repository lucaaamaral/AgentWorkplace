# Claude Code adapter — requirements

Integration between the broker and a running Claude Code interactive session. Bus semantics (channels, addressing, registration model, tool surface, audit rules) are defined in the [architecture overview](../../architecture/overview.md); this document covers only what is specific to the Claude Code harness.

## Mechanism

Claude Code's only sanctioned push path into a running session is its **Claude Code channels** feature (`notifications/claude/channel`) — a harness mechanism unrelated to AgentWorkplace channels, and referred to with the full qualifier throughout. Control is inverted relative to the Codex adapter: the broker does not connect to Claude Code. Claude Code spawns a **channel plugin** (an MCP server declaring the `claude/channel` capability) as a stdio subprocess when launched with `--channels`. The plugin pushes events into the live session; each event surfaces to the model as a `<channel source="...">` message.

The adapter is therefore a **shim**: a minimal channel-plugin process bridging Claude Code's stdio protocol to the broker.

```
Claude Code TUI ── stdio ── channel shim ── local IPC ── broker
                            (unix socket on POSIX, named pipe/loopback TCP on Windows)
```

## Adapter requirements

### Inbound (broker → session)

- CL-1. Translate broker delivery into a `notifications/claude/channel` notification on the session's stdio connection, formatted so the event identifies the bus channel, the sender, the body, and how to reply through the bus tools.
- CL-2. Report acknowledgment to the broker: at minimum "notification emitted into session"; "turn started" if the protocol surfaces it.
- CL-3. Events can only reach an open session. When the shim is not connected the recipient is disconnected: the broker marks the delivery `failed` per the message model — the shim never queues, and there is no drain-on-reconnect.
- CL-4. Delivery requires an **interactive** session ([findings](findings.md), [ADR-0015](../../decision-records/0015-claude-channels-interactive-only.md)). Idle delivery into an interactive session is confirmed. Mid-turn events queue and are grouped on the next turn.

### Outbound (session → broker)

- CL-5. The shim exposes the bus tool surface (defined in the overview) to the session as MCP tools; all outbound traffic goes over the shim's broker connection — the shim must not provide any path that bypasses the broker.

### Session binding

States and presence semantics: [common contract](../session-lifecycle.md) · [Claude Code specifics](session-lifecycle.md).

- CL-6. One shim instance per session. The session connects anonymous; after in-session registration the shim carries the session→principal binding for all subsequent traffic.
- CL-7. On connect and on registration, the shim reports session metadata (cwd, harness version, pid) for liveness display.

### Launch integration

- CL-8. The human starts `claude` normally; no wrapper. One-time machine setup must therefore ensure: the shim is installed where `--channels` can reference it, and the `--channels` flag is applied per session (alias or shell default) — per-session opt-in is a hard requirement of the Claude Code channels feature, not a choice.
- CL-9. Approvals are notify-only ([ADR-0012](../../decision-records/0012-approvals-are-notify-only.md)): the shim never implements the permission-relay capability. If a pending permission prompt is detectable, it may be surfaced to the human as a notification; the approval is answered in the session's own terminal.

## Harness facts and constraints

| Constraint | Detail |
| --- | --- |
| Claude Code version | ≥ 2.1.80 (Claude Code channels research preview) |
| Auth environment | claude.ai or Console API key only — Claude Code channels unavailable on Bedrock / Vertex / Foundry |
| Plugin allowlist | Research preview restricts `--channels` to Anthropic-allowlisted plugins; a custom shim requires `--dangerously-load-development-channels` |
| Org policy | Team/Enterprise orgs need `channelsEnabled: true` in managed settings |
| Session scope | Events arrive only while the session is open (drives CL-3) |
| Protocol stability | Research preview: flag syntax and protocol contract may change — pin the Claude Code version, isolate protocol handling in the shim |

## Risks / open questions

- **Preview churn ([ADR-0015](../../decision-records/0015-claude-channels-interactive-only.md)).** The Claude Code channels contract is explicitly unstable. Mitigation: version-pin, keep the shim thin. There is no secondary delivery path: if channels are unavailable in an environment, the Claude adapter is unavailable there.
- **Reply visibility.** With chat-bridge usage of Claude Code channels, the terminal shows the inbound event and the reply tool call but not the reply text; verify how bus replies render in the session's terminal.
- **Notification vs turn semantics.** Confirm whether a pushed event always initiates a model turn on an idle session, and how events queue mid-turn. Acceptance test required before ack semantics (CL-2) are finalized.

See also: [spike findings](findings.md).

## References

- Claude Code channels: https://code.claude.com/docs/en/channels
- Claude Code channels protocol reference (build your own): https://code.claude.com/docs/en/channels-reference
- Injection-API feature requests (context for why channels is the only sanctioned path): anthropics/claude-code #27441, #24947
