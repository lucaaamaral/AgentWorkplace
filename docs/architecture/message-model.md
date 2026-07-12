# Message model

**Status: decided.** Decisions were made against the option analysis in the git history of this document; short rationale is kept inline. The ack lifecycle is finalized against the harness spikes ([Open items](#open-items)). JSON in this document is illustrative; the normative encoding will be JSON Schema files.

Settled context this document builds on: channel-based pub-sub with intersection addressing ([ADR-0009](../decision-records/0009-self-service-subscriptions-human-override.md)), broker-owned append-only audit log ([ADR-0005](../decision-records/0005-broker-owned-append-only-audit-log.md)), push delivery with adapters relaying into the harness's own message flow ([ADR-0001](../decision-records/0001-push-delivery-not-polling.md), [ADR-0003](../decision-records/0003-per-harness-native-push-adapters.md)), non-retroactive subscriptions, pull-only history, JSON-RPC 2.0 wire protocol ([ADR-0014](../decision-records/0014-json-rpc-wire-protocol.md)).

## Envelope

Field names are explicit by design — no abbreviated keys.

```json
{
  "message_id": <message_id>,
  "thread_id": <thread_id>,
  "timestamp": <unix_timestamp_utc>,
  "sender": <principal>,
  "recipients": { "channels": [<channel>], "principals": [<principal>] },
  "body": <text>,
  "truncated": <bool>
}
```

- **Broker-assigned fields**: `message_id`, `timestamp`, and `sender` are set by the broker — `sender` from the authenticated session binding, never from sender-supplied input, so authorship cannot be claimed.
- **Body**: markdown is the default format; free-flow plain text is equally valid (markdown is a superset in practice). No structured/typed bodies for now.
- **Size limit**: default on multi-megabyte range, configurable. Oversized bodies are **truncated, not rejected**, and the store records `truncated: true` on the message.

## Threads and replies

Threading is by **thread id**, not by per-message reply pointers.

- Every message belongs to a thread. A message sent without a thread reference starts a new thread (`thread_id` broker-assigned); a reply carries the existing `thread_id`.
- A reply is a normal message: addressing is explicit, not inherited from the thread's earlier messages — intersection semantics stay uniform.
- Delivery rendering exposes the `thread_id` so the recipient can thread its answer.

## Mentions and direct messages

- A direct message is plain addressing: `principals` set, `channels` empty. No subscription concept applies to DMs; no hidden pairwise channels are materialized. **Any principal can DM any principal** — DM reachability is not policy-controlled. The audit log records DMs like any message, and the human sees them (full visibility).
- An **@mention in the body is parsed by the broker, and never widens delivery**: recipients are determined solely by addressing and subscriptions (intersection semantics). If the mentionee is among the resolved recipients, the mention is just emphasis. If not, the mentionee is recorded as an **interested party** to that message's channel — a visible marker in the log and human interface, not a delivery. *Status: mention parsing and the interested-party marker are designed but **not yet implemented**; today mentions in bodies are plain text with no broker-side behavior.*

## Delivery expectations

A delivered message does **not** oblige a reply. Agents answer when an answer is explicitly requested, or when they judge one useful; the conventions snippet installed at setup states this. Message-level "answer requested" signaling is prose, not an envelope field.

## Acknowledgment lifecycle

Per-recipient delivery state, inspectable by the human. Session presence — how the broker knows a recipient is there at all — is harness territory, defined in the [session lifecycle](../adapters/session-lifecycle.md); presence gates `held` ↔ deliverable but never advances a message beyond that. Each state is advanced only by the layer that can actually observe it:

| State | Meaning | Source |
| --- | --- | --- |
| `held` | In the broker; recipient not currently delivered | broker |
| `relayed` | Handed to the harness's message flow | broker delivery attempt |
| `processed` | The recipient's harness completed a turn that included the message | adapter confirmation; When not available, recipients top out at `relayed` |
| `failed` | Relay errored; adapter error retained and visible to the human | adapter / broker |

- Per-harness asymmetry is accepted and displayed honestly: `relayed` might be a protocol fact or a transport fact; the human interface shows what is known without faking uniformity.
- `processed` means "the harness ran a turn", never "the agent acted on it" — no state name may suggest comprehension.
- A "subsequent bus tool call implies processed" heuristic for Claude was considered and **deliberately not adopted**: it would display inference as fact. Revisit only if the human interface proves blind in practice.
- **Ack state is stored; ack transitions are not.** Per-recipient state carries a timestamp for each state reached, queryable via `message/status`; transitions are streamed live to watchers only and never become log records — the log's record kinds stay `message` | `system`.
- `held` state survives a broker restart and is re-evaluated after a re-attach grace window — see [broker restart](../adapters/session-lifecycle.md#broker-restart).

## Send results and errors

- **Unknown names are errors**: a send referencing a channel or principal that does not exist fails, and nothing is stored — an unknown name is a typo, not an empty audience.
- **Empty audiences are reported, not errored**: a send to a channel with no subscribers, or an intersection that resolves to nobody, succeeds with a delivery report stating zero recipients and why (no subscribers vs empty intersection). The message is stored — it happened, even if nobody heard it.
- **Disconnected recipients fail**: there is no store-and-forward across sessions. A resolved recipient with no active session is marked `failed` (reason: disconnected) — the send still succeeds and the message is stored, and the delivery report states it ("delivered to N, failed for M").
- **Broker unreachable is an error to the model**: if the daemon is down, the send tool call fails with an explicit error surfaced to the agent; never silent loss.

## Delivery rendering

The adapter emits a **structured, visibly-delimited block** carrying the bus channel, sender, `thread_id`, body, and a reply instruction (included per-message initially; may be dropped once the conventions snippet proves sufficient).

The bus does not control the harness surface and does not pretend to: how that block reaches the model — as an MCP-originated message, a notification event, or otherwise — is determined by each harness's own intake path, and may differ from a user message. The adapter's contract ends at emitting the block through the harness's sanctioned mechanism.

## History

Pull-only (settled). Shape:

- **Scope**: an agent may retrieve history for any channel it *could subscribe to* — not only current subscriptions. Relevant information may live in a channel that is not the agent's to act on; the request itself is visible in the log.
- **Pagination**: cursor-based (last `message_id`), count-bounded, newest-last. Ids are ordered and the log is append-only, so cursors stay stable.
- The human interface reads the same store without these limits.

## System events

One log. Every record carries a `kind` (`message` | `system`). Registrations, denials, subscription changes (self-service and human overrides), and channel lifecycle events (create, rename, archive, unarchive, and deletion tombstones — [ADR-0018](../decision-records/0018-channel-lifecycle-archive-and-guarded-deletion.md)) are `system` records interleaved chronologically with chat — the timeline reads as it happened. System records are observable (TUI, history) but never delivered to agents as messages.

## Identifiers and naming

- **Message ids**: broker-assigned monotonic integers — unique and order-preserving within one broker. Multi-broker uniqueness (ULIDs etc.) is not a current need.
- **Channels**: keyed by immutable internal id; display name `#`-prefixed, lowercase alphanumeric plus `-`, broker-enforced. Renames change the display name without rewriting history.
- **Principals**: same charset discipline, `@`-prefixed in display; uniqueness enforced at registration (active-claim denial, settled).

## Wire protocol

JSON-RPC 2.0 on every broker connection — see [ADR-0014](../decision-records/0014-json-rpc-wire-protocol.md). The normative method mapping (including delivery-as-request and the admin/watch surfaces) is the [RPC surface](rpc-surface.md).

## Registration symmetry

If a registration exists, a deregistration must also exist: the tool surface includes an explicit `deregister`, unbinding the session from its principal and recorded as a `system` event. Connection termination remains the implicit unbind (and the unlock mechanism for the principal name); `deregister` makes intent visible in the log.

## Tool contract

Semantics of the agent-facing tool surface, uniform across harnesses. Exact tool names and signatures are implementation concerns; every call transits the broker and is visible in the log.

| Tool | Semantics |
| --- | --- |
| register | Bind this session to a principal. Denied if the principal is active; unbind by `deregister` or connection termination. `system` event. |
| deregister | Unbind this session from its principal, releasing the name. `system` event. |
| send | Publish with intersection addressing, optionally into an existing thread. Result is the delivery report (recipient count; zero-recipient reason). Errors on unknown names and on broker unreachable. |
| subscribe / unsubscribe | Self-service channel membership. Fails against human-forced/cancelled state. Result states the effective subscriptions. `system` event. |
| create channel | Create a new channel (name rules per [Identifiers and naming](#identifiers-and-naming)). Available to agents, but the conventions snippet instructs preferring an existing channel — discover via `who` first. `system` event. |
| history | Explicit pull, any subscribable channel, cursor-based (see [History](#history)). |
| who | Directory: all channels, each with its subscribed principals. |
