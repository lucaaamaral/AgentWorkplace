# Codex CLI — spike findings

Empirical results from driving `codex app-server` directly. These confirm or correct the assumptions in [requirements](requirements.md) and [session lifecycle](session-lifecycle.md).

- **Environment**: codex-cli 0.144.1, macOS, stdio transport, existing ChatGPT (plus) auth. Model `gpt-5.6-sol`.
- **Method**: newline-delimited JSON-RPC 2.0 over the process's stdio; scripted `initialize` → `thread/start` → `turn/*`, observing the notification stream.

## Confirmed

- **Transport & handshake.** `codex app-server` speaks newline-delimited JSON-RPC 2.0 over stdio. `initialize` (with `clientInfo`) returns environment info; `thread/start` returns a durable thread id; local stdio needs no token. A machine-readable protocol schema is available via `codex app-server generate-json-schema --out <dir>` — usable directly to generate the adapter's types.
- **Idle delivery.** `turn/start` on an idle thread is accepted immediately (response carries the created turn, `status: inProgress`), followed by a `turn/started` notification, streaming `item/*` deltas, and a terminating `turn/completed` carrying `durationMs`. `thread/status/changed → idle` and `thread/tokenUsage/updated` also fire.
- **`processed` is observable.** `turn/completed` for the delivered turn is a reliable "the harness ran a turn with this input" signal — the ack lifecycle's `processed` state is real on Codex.
- **`turn/steer` works, with a required field.** It **requires `expectedTurnId`** (the active turn's id) — omitting it errors with `-32600 missing field expectedTurnId`. With it, the steer input is injected into the running turn and the model incorporates it (observed: the model completed its current output, then appended the steer-requested token). Semantics are "add guidance to the active turn", not "interrupt/replace it".

## Corrected assumptions

- **`turn/start` while busy is NOT a queue — it is dropped.** Issuing `turn/start` during an in-progress turn returns a turn object with `status: inProgress` and no error, but that turn **never runs** — it did not start or complete even after the prior turn finished and 120s elapsed. **Consequence for the adapter: it must serialize on the thread — wait for the thread to be idle (`turn/completed` / `thread/status: idle`) before `turn/start`.** The broker holds; the protocol does not queue. Fire-and-forget delivery while busy loses the message.
  - This makes the broker-vs-protocol holding split concrete: **all holding is the broker's**; the app-server offers no mid-turn delivery except `turn/steer` (which needs the active turn id and appends rather than enqueues).

## Not yet tested

- `turn/start` acceptance-vs-reject nuance across codex versions (only "accepted-but-dropped" observed on 0.144.1).
- Multi-client WebSocket transport with a TUI attached to the same thread (spike used stdio, single client).
- `thread/resume` across a *restarted* app-server process (only same-process resume was smoke-tested).
