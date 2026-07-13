# ADR-0019 — Admin registration requires a local admin credential

**Status**: Accepted

## Context

`admin/register` was an open verb: any session passing `session/hello` could
claim admin rights — the observability tap, DM history between any pair,
channel deletion, `daemon/shutdown`. On a loopback bus with no identity this
meant agents could self-grant the manager's powers, which contradicts the
human-as-manager model (ADR-0006) and was demonstrated in practice by an
agent admin-registering to read ack state.

Constraint: the tool must stay simple, and `workplace cli` must remain
zero-config on a single machine.

## Decision

`admin/register` requires an **admin credential**, separate from the session
`auth_token` (which shims must hold and therefore cannot double as an admin
secret):

- On first start the daemon generates 256 random bits, hex-encoded, into
  `<data dir>/admin-token` — exclusive-create, `0600`, private to the
  operator at exactly the trust level of the store beside it. Any failure to
  resolve the credential aborts daemon startup (fail closed, never open
  admin).
- `admin/register` without the exact token fails `UNAUTHORIZED` and appends a
  `RegistrationDenied` audit record carrying only the name and "invalid
  admin credential" — never the supplied value. The token never appears in
  argv, URLs, logs, errors, or records.
- `workplace cli` reads the file automatically: same-machine UX stays
  zero-config. `[broker].admin_token_file` / `[client].admin_token_file`
  override the path (a file path, never an inline TOML secret) for
  split-machine setups — over a trusted or tunneled link only, since the TCP
  transport is plaintext.
- The shim never reads or exposes the credential, closing the agent-facing
  path.

## Consequences

- Admin rights now require the operator's private files, not merely a TCP
  connection. An agent that can read the operator's data directory already
  crosses the declared host boundary (daemon.md trust boundary); no bus
  machinery pretends to prevent that.
- Rejected alternatives: reusing `auth_token` (hands agents the admin
  credential), a unix-socket admin surface (reopens ADR-0016, heavier for
  the same practical boundary), pinned/first-claim admin names (racy — an
  agent can claim first).
