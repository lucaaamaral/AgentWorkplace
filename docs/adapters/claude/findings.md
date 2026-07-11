# Claude Code — spike findings

Empirical results from driving a custom development channel against Claude Code. These confirm or correct the assumptions in [requirements](requirements.md) and [session lifecycle](session-lifecycle.md).

- **Environment**: Claude Code 2.1.207, macOS. Custom channel server (Node + `@modelcontextprotocol/sdk`) declaring `experimental: { 'claude/channel': {} }` plus a `reply` tool, launched with `--dangerously-load-development-channels server:<name>`. Model `haiku`.
- **Method**: the channel server logged every notification it emitted and every `reply` tool call it received; a control HTTP endpoint pushed events on demand. The session was driven interactively through a PTY.

## Confirmed

- **Interactive idle delivery works.** With an interactive session sitting idle, a pushed `notifications/claude/channel` event **woke the session and the model acted on it within ~6s**, calling the `reply` tool with the `event_id` passed in the event's `meta`. This validates the primary Claude delivery path and confirms `meta` reaches the model.
- **No inbound acknowledgment.** MCP notifications are one-way: the channel server's `notification()` resolves when the message is written to the transport, with no receipt and no processing signal. The ack lifecycle's `relayed` is a **transport fact only** on Claude, and there is no protocol `processed` signal — Claude recipients top out at `relayed`, exactly as the message model states.
- **Event rendering.** Events reach the model wrapped as `<channel source="<server-name>" <meta-key>="<val>" …>body</channel>`; `meta` keys must be identifier-safe (letters/digits/underscore) or they are dropped. This is the structured, delimited block the delivery-rendering section calls for — provided by the harness, not the adapter.
- **Delivery requires an interactive session.** A non-interactive control run (`claude -p --output-format stream-json`) received none of the pushed events despite zero transport errors (known limitation, anthropics/claude-code #55896). Interactive sessions are this project's only operating mode ([ADR-0015](../../decision-records/0015-claude-channels-interactive-only.md)), so this grounds CL-4 without constraining the design.

## Corrected assumptions

- **Launch is gated by interactive dialogs.** A fresh interactive session blocks on a folder-trust prompt and (first use of a project MCP server) a consent prompt before the channel server is spawned. The one-time setup and launch integration (CL-8) must account for these being answered once per project, not per session.

## Not yet tested (docs are authoritative here)

- **Mid-turn queueing/grouping.** Not exercised empirically. The [channels reference](https://code.claude.com/docs/en/channels-reference) states events queue into the session and, if several arrive while Claude is busy, are delivered together on the next turn and handled as a group. Treated as authoritative pending a direct test.
- **Permission relay** (notify-only per [ADR-0012](../../decision-records/0012-approvals-are-notify-only.md), so not integrated) — the `claude/channel/permission` capability exists but was not exercised.
