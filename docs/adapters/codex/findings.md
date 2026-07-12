# Codex CLI — spike findings

Empirical results from driving `codex app-server` directly. These confirm or correct the assumptions in [requirements](requirements.md) and [session lifecycle](session-lifecycle.md).

- **Environment**: codex-cli 0.144.1, macOS, stdio transport, existing ChatGPT (plus) auth. Model `gpt-5.6-sol`.
- **Method**: newline-delimited JSON-RPC 2.0 over the process's stdio; scripted `initialize` → `thread/start` → `turn/*`, observing the notification stream.

## Confirmed

- **Transport & handshake.** `codex app-server` speaks newline-delimited JSON-RPC 2.0 over stdio. `initialize` (with `clientInfo`) returns environment info; `thread/start` returns a durable thread id; local stdio needs no token. A machine-readable protocol schema is available via `codex app-server generate-json-schema --out <dir>` — usable directly to generate the adapter's types.
- **Idle delivery.** `turn/start` on an idle thread is accepted immediately (response carries the created turn, `status: inProgress`), followed by a `turn/started` notification, streaming `item/*` deltas, and a terminating `turn/completed` carrying `durationMs`. `thread/status/changed → idle` and `thread/tokenUsage/updated` also fire.
- **`processed` is observable.** `turn/completed` for the delivered turn is a reliable "the harness ran a turn with this input" signal — the ack lifecycle's `processed` state is real on Codex.
- **`turn/steer` works, with a required field.** It **requires `expectedTurnId`** (the active turn's id) and `threadId`. With them, the steer input is injected into the running turn and the model incorporates it by turn end (observed: the "PINEAPPLE" marker requested via steer appeared in the completed output). Semantics are "add guidance to the active turn", not "interrupt/replace it".

## `turn/steer` vs `thread/resume` for live injection

A dedicated edge-case pass (`spike4.mjs`) on 0.144.1 evaluated both as delivery mechanisms. Neither is the delivery primitive — `turn/start` is (idle → the agent reacts). Findings:

**`turn/steer`** (inject into an in-progress turn):

- **Idle → error.** Steering with no active turn returns `-32600 no active turn to steer`. Bus messages usually arrive while the agent is idle, so steer cannot serve the common case.
- **Completion race.** Steering a just-completed turn's id returns the same "no active turn" error. Because the live turn id comes from the `turn/started` event and the turn can finish before a steer lands, any steer-based path needs "on race, fall back to `turn/start`" and must track the active turn id from the event stream.
- **Semantic conflation (decisive).** Injecting an unrelated message mid-turn (a security question steered into a haiku task) made the agent address **both** in one turn — the reply blends into the current work with no clean thread boundary or attribution. Wrong for a bus where each message deserves its own attributable, threadable reply.
- **When it lands, it works** — reliable incorporation on a live turn — which suits a deliberate *interrupt/override* ("stop, wrong direction"), where derailing the current turn is the intent.

**`thread/resume`** (reattach a thread — not content injection):

- **Not a no-op on a live thread.** Resuming a loaded, idle thread was accepted but emitted `thread/goal/cleared` (plus a token-usage update) — it resets goal state. Resume only when the thread is actually unloaded.
- **Clean "gone" signal.** A missing thread returns `-32600 no rollout found for thread id …`, which maps to session-disconnected (fail delivery, no store-and-forward).
- **Heavy config surface.** The params mirror `thread/start` (`model`, `sandbox`, `approvalPolicy`, `cwd`, …). Passing `threadId` alone restores from the persisted rollout; partial overrides would silently change the agent's model/sandbox — so resume with `threadId` only (or the recorded config), never partial.
- **`turn/start` after resume works** — resume→`turn/start` is the reattach-then-deliver sequence for unloaded threads (CX-8).

**Decision:** delivery is `turn/start` only, serialized on thread-idle; `thread/resume` (threadId-only) precedes it when the thread has unloaded. `turn/steer` is **not adopted for delivery** (conflation + completion race) — reserved for a possible future override/interrupt class. This closes the "turn/steer usefulness" open question in the [requirements](requirements.md).

## Corrected assumptions

- **`turn/start` while busy is NOT a queue — it is dropped.** Issuing `turn/start` during an in-progress turn returns a turn object with `status: inProgress` and no error, but that turn **never runs** — it did not start or complete even after the prior turn finished and 120s elapsed. **Consequence for the adapter: it must serialize on the thread — wait for the thread to be idle (`turn/completed` / `thread/status: idle`) before `turn/start`.** The broker holds; the protocol does not queue. Fire-and-forget delivery while busy loses the message.
  - This makes the broker-vs-protocol holding split concrete: **all holding is the broker's**; the app-server offers no mid-turn delivery except `turn/steer` (which needs the active turn id and appends rather than enqueues).

## Multi-client attach transport

The Codex adapter must attach to a session the human is also watching (the attach model). Established on 0.144.1 (`spike7.mjs`):

- **The transport is `codex app-server --listen`**, not the app-server *daemon*. `--listen unix://PATH` or `ws://IP:PORT` starts a shared app-server multiple clients connect to. The `app-server daemon` + `app-server proxy --sock` control socket is a **dead end** for our use: raw JSON-RPC over the control socket is dropped, and `proxy` returned nothing to `initialize` even with `enable-remote-control`.
- **`--listen` speaks WebSocket, not line-delimited JSON.** The listener exposes `/readyz` and `/healthz` HTTP endpoints; clients must do a WebSocket handshake (raw NDJSON over the socket hangs). Unix-socket form needs a short path (`SUN_LEN` ~104 chars) in a real directory (macOS `/tmp` is a symlink and is rejected; the process's home dir works).
- **Cross-client injection works.** A second client can `turn/start` on a thread another client created, addressing it by thread id — accepted and the turn runs.
- **Events route to the thread *owner*, not the injector.** The client that created the thread receives `turn/started` / `item/*` / `turn/completed`; the injecting client does **not** see completion of the turn it started. Consequence: whoever owns the thread sees the full stream (good for the human's monitoring), but an attach-only adapter cannot read `processed` from its own connection.
- **`thread/list` is connection-scoped.** A client does not enumerate another client's threads, so the adapter must be **told** the thread id (matches CX-7: registration carries it) rather than discover it by listing.
- **No explicit `thread/subscribe`.** Subscription is implicit in `thread/start` / `thread/resume` (`thread/unsubscribe` opts out); `thread/read {includeTurns}` is a pull. So an injector can observe completion only by resuming-to-subscribe (side-effecting — resume clears the goal, see above) or by polling `thread/read` / status.

**Decision: the human owns the thread.** The human's Codex client creates the thread and receives the full event stream (native monitoring — the point of the attach model). The adapter attaches as a non-owner: it injects deliveries with `turn/start` by thread id and, since turn events route to the owner, observes `processed` by **polling `thread/read {includeTurns}`** for its turn's status.

**Verified end-to-end with the interactive TUI (`pty_remote.py`):** `codex --remote ws://…` runs the normal interactive window against the shared app-server; the thread is created lazily on the first user message (rollout appears then, not at boot); an adapter client's injected `turn/start` is accepted, and **both the injected bus message and the agent's reply render natively in the human's window**. A plain `codex` (no flag) was separately verified to expose no reachable endpoint at all — no listener, no socket, no server child — so `--remote` is the only interactive attach point, and plain sessions participate outbound-only.

**Thread-id discovery: the agent self-reports at registration.** Codex injects **`CODEX_THREAD_ID`** into every shell environment the agent executes in (`codex-rs/core/src/exec_env.rs`; verified empirically on 0.144.1 — `echo $CODEX_THREAD_ID` from inside a session prints the real thread id). So the register flow is fully agent-initiated, symmetric with Claude: the human says "register as @codex-1", the agent reads its own `$CODEX_THREAD_ID` and passes it as a register-tool argument (the tool description instructs this). The bus MCP entry supplies the app-server endpoint from its own configuration. Self-reported coordinates are consistent with the trust model — a wrong thread id just fails deliveries, visibly in ack state.

## Not yet tested

- `turn/start` acceptance-vs-reject nuance across codex versions (only "accepted-but-dropped" observed on 0.144.1).
- `thread/resume` across a *restarted* app-server process (only same-process resume was smoke-tested; the rollout persists to `~/.codex/sessions/.../rollout-*.jsonl`, so cross-restart resume is plausible but unverified).
