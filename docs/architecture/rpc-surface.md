# Broker RPC surface

**Status: decided.** Normative method surface of the broker daemon. Envelope, acknowledgment lifecycle, and send-result semantics are defined in the [message model](message-model.md); this document maps them onto the wire. JSON here is illustrative; the normative encoding will be JSON Schema files.

## Wire and trust

- JSON-RPC 2.0 ([ADR-0014](../decision-records/0014-json-rpc-wire-protocol.md)), newline-delimited, on every broker connection. This document is transport-agnostic; the transport is decided in [ADR-0016](../decision-records/0016-tcp-broker-transport.md) and endpoint configuration lives in the [daemon runtime](daemon.md).
- **No authentication.** The trust model is the local machine or a protected local network ([ADR-0010](../decision-records/0010-local-first-defaults-not-localhost-bound.md)). A security model is deliberately deferred until there are users beyond the operator.
- Method naming is `namespace/verb`, matching the harness protocols this project integrates with.

## Roles

There is no role field and no privileged connection class. A session becomes what it registers as:

- `principal/register` → an **agent participant**: the tool-contract surface below.
- `admin/register` → the **manager**: a participant plus full visibility and the admin verbs.
- No registration → **observer**: `watch/*` and `session/hello` only.

Admin verbs are honored only on admin-registered sessions. This is role hygiene and security: it makes accidental misuse by an agent structurally impossible (the adapter shims never expose admin tools to models in the first place), while any local process remains free to admin-register.

## Handshake

| Method | Params | Result |
| --- | --- | --- |
| `session/hello` | `{client_info: {harness?, version, pid, cwd}}` | `{broker_version, session_id}` |

First call on every connection. The session is anonymous until it registers; `client_info` feeds liveness display (CL-7 / CX analog).

## Agent surface

The wire mapping of the [tool contract](message-model.md#tool-contract) — same operations, same semantics, one method each.

| Method | Params | Result | Errors |
| --- | --- | --- | --- |
| `principal/register` | `{name}` | `{principal}` | `NAME_TAKEN`, `INVALID_NAME` |
| `principal/deregister` | `{}` | ok | `NOT_REGISTERED` |
| `message/send` | `{channels: [], principals: [], body, thread_id?}` | `{message_id, thread_id, delivery}` | `UNKNOWN_NAME`, `NOT_REGISTERED` |
| `channel/subscribe` | `{channel}` | ok | `UNKNOWN_NAME`, `NOT_REGISTERED` |
| `channel/unsubscribe` | `{channel}` | ok | `UNKNOWN_NAME`, `NOT_REGISTERED` |
| `channel/create` | `{name}` | `{channel}` | `NAME_TAKEN`, `INVALID_NAME`, `NOT_REGISTERED` |
| `history/get` | `{scope, before_message_id?, limit}` | `{records: [], next_cursor?}` | `UNKNOWN_NAME`, `SCOPE_DENIED` |
| `directory/who` | `{}` | `{channels: [{channel, subscribers: []}], principals: [{principal, active}]}` | — |

- `message/send` result `delivery`: `{delivered: [principal], failed: [{principal, reason}], empty_audience?: "no_subscribers" | "empty_intersection"}` — the delivery report the message model requires. Unknown names error and store nothing; empty audiences succeed and are reported.
- `history/get` `scope` is an explicit object: `{channel: <name>}` or `{dm_with: <principal>}`. An agent may read any subscribable channel and its own DM threads; anything else is `SCOPE_DENIED`. Cursor is the last `message_id`, records newest-last.
- Registration, deregistration, subscription changes, and channel creation produce `system` records in the log.

## Delivery: request, not notification

Broker-to-recipient delivery is a JSON-RPC **request whose response is the acknowledgment**, so the ack lifecycle is a protocol fact rather than a side channel:

| Direction | Method | Params | Response |
| --- | --- | --- | --- |
| broker → adapter | `message/deliver` | `{recipient, envelope}` | `{status: "relayed"}` — or a JSON-RPC error with `data.reason`, which the broker records as `failed` |
| adapter → broker | `message/processed` (notification) | `{message_id, recipient}` | — |

- `message/processed` is emitted only where the harness makes it observable (Codex `turn/completed`); recipients without it top out at `relayed`, per the message model.
- This contract is the adapter interface. It crosses the wire only where the adapter is a separate process (the Claude channel shim); in-process adapters (Codex, inside the daemon) implement the same contract natively.
- `relayed` remains a transport fact or a protocol fact depending on the harness; the broker records what the adapter can honestly claim.

## Admin surface

`admin/register {name}` claims a principal like any registration and additionally grants:

- **Full visibility**: this session receives `message/deliver` for every message on every channel and every DM. These monitor copies are an **observability tap, not audience membership** — they never appear in delivery reports or per-recipient ack state unless the admin principal was an actual resolved recipient.
- The admin verbs:

| Method | Params | Purpose |
| --- | --- | --- |
| `admin/subscribe` | `{principal, channel}` | Force a subscription ([ADR-0009](../decision-records/0009-self-service-subscriptions-human-override.md)); `system` record attributed to the admin principal |
| `admin/unsubscribe` | `{principal, channel}` | Cancel a subscription; same recording |
| `channel/rename` | `{channel, new_name}` | Display-name change; internal id immutable, history untouched. Works on archived channels (frees a name — [ADR-0018](../decision-records/0018-channel-lifecycle-archive-and-guarded-deletion.md)) |
| `channel/archive` | `{channel}` | Hide from directory, refuse new subscriptions, force-cancel active ones (`system` records). Name stays reserved — one namespace across live and archived |
| `channel/unarchive` | `{channel}` | Restore an archived channel. Unconditional: name reservation makes collision impossible |
| `channel/delete` | `{channel}` | Phase one of guarded permanent deletion: returns `{confirmation_token}` — single-use, short-lived, bound to this session and channel |
| `channel/delete_confirm` | `{channel, confirmation_token}` | Phase two: physically removes the channel and its records, writes a tombstone `system` record, frees the name ([ADR-0018](../decision-records/0018-channel-lifecycle-archive-and-guarded-deletion.md)) |
| `message/status` | `{message_id}` | Per-recipient acknowledgment states with per-state timestamps and retained failure reasons |
| `daemon/status` | `{}` | Version, uptime, connected sessions (`client_info`, bound principal), channel/principal counts. Also the CLI's lazy-start health check |
| `daemon/shutdown` | `{}` | Graceful stop |

- Deletion always crosses at least two deliberate steps: the two-phase token handshake at the protocol, plus the interface's own confirmation (the TUI requires typing the channel name verbatim) — see [ADR-0018](../decision-records/0018-channel-lifecycle-archive-and-guarded-deletion.md).
- An admin session's `history/get` accepts any channel and any DM pair: `{dm_between: [a, b]}`.
- The manager sends through the same `message/send` — no privileged send path ([ADR-0006](../decision-records/0006-human-as-first-class-principal.md)).
- Admin verbs on a non-admin session: `NOT_ADMIN`.

## Watch surface

Registration-free observation — a bare connection may `session/hello` and watch without claiming a principal. This feeds the TUI's stream pane and the development loop (observe events while driving a single harness, no second agent needed).

| Method | Params | Result |
| --- | --- | --- |
| `watch/start` | `{channels?: []}` (omitted = everything, DMs included) | ok; broker then streams `watch/event` notifications |
| `watch/stop` | `{}` | ok |

`watch/event` streams log records (`message` | `system`) as they are appended, plus **live acknowledgment transitions** (`kind: "ack"`). Ack transitions are wire-only telemetry: never stored, never in `history/get` — current ack state, with per-state timestamps, is queryable via `message/status` ([message model](message-model.md#acknowledgment-lifecycle)). Watching is read-only and leaves no trace in the log.

## Errors

Application error codes, stable symbolic names in `error.data.code`:

| Code | Name | Meaning |
| --- | --- | --- |
| -32000 | `UNKNOWN_NAME` | Channel or principal does not exist; nothing stored |
| -32001 | `NAME_TAKEN` | Registration or channel name actively claimed |
| -32002 | `NOT_REGISTERED` | Method requires a registered principal |
| -32003 | `ALREADY_REGISTERED` | Session already bound to a principal |
| -32004 | `INVALID_NAME` | Name violates the charset rules ([identifiers](message-model.md#identifiers-and-naming)) |
| -32005 | `NOT_ADMIN` | Admin verb on a non-admin session |
| -32006 | `SHUTTING_DOWN` | Broker is stopping; retry against the next daemon |
| -32007 | `SCOPE_DENIED` | History scope outside the caller's visibility (e.g. another pair's DMs) |
| -32008 | `BAD_CONFIRMATION` | Deletion token missing, expired, already used, or bound to another session/channel |

Broker-unreachable is not an error code: it is the adapter failing the agent's tool call locally, per the message model.
