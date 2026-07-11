# ADR-0018 — Channel lifecycle: archive by default, guarded permanent deletion

**Status**: Accepted

## Context

Channels need an end-of-life story. The constraints in tension:

- The audit log is append-only ([ADR-0005](0005-broker-owned-append-only-audit-log.md)): history is the point of the broker.
- Channels are keyed by an immutable internal id, but humans and agents address them by display name — the name is the human-facing key of the historical record. Reusing a name across unrelated channels makes the audit trail ambiguous to its readers even though ids keep it unambiguous to the machine.
- Sometimes the operator genuinely wants data gone, not hidden.

## Decision

### Archive is the default end-of-life

`channel/archive` (admin verb): the channel disappears from the directory and refuses new subscriptions; all active subscriptions are force-cancelled at that moment, each logged as a `system` record. History is retained. Agents lose history access through the existing rule — history is readable for channels an agent *could subscribe to*, and an archived channel is not subscribable — so archived history is admin-only without any new rule.

**One namespace across live and archived channels**: an archived channel keeps ownership of its name; `channel/create` against it fails with `NAME_TAKEN`. Consequently `channel/unarchive` is unconditional — a collision is impossible by construction. `channel/rename` works on archived channels too; that is the escape hatch to free a name for a fresh channel while the archived history keeps its identity under the new name (renames are themselves `system` records, so the display-name timeline stays honest).

### Permanent deletion exists, admin-only, guarded

`channel/delete` is a **two-phase protocol operation**: the first call returns a single-use, short-lived confirmation token bound to the calling session and the target channel; only `channel/delete_confirm` presenting that token performs the deletion. The human interface adds its own independent confirmation on top (the TUI requires typing the channel name verbatim), so a deletion always crosses at least two deliberate steps and cannot be expressed as a single accidental action.

Deletion physically removes the channel and its message records, frees the name, and writes a **tombstone `system` record** (channel id, display name at deletion, record count, acting admin principal, timestamp).

This is an explicit, narrow exception to ADR-0005's append-only property: only a human-driven admin session can invoke it, it is unreachable from agent tool surfaces, and the log permanently records that — and what — was redacted.

## Consequences

- Routine cleanup is archive: reversible, name-safe, history-preserving. Deletion is exceptional and self-documenting via the tombstone.
- With deletion available, the audit log guarantees completeness *except where a tombstone says otherwise* — the weakening is visible in the log itself, never silent.
- Deletion is channel-scoped; DM history has no deletion path in this decision.
- A deleted channel's name is reusable; the tombstone preserves the historical association between the name and the deleted channel id.
