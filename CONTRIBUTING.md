# Contributing to AgentWorkplace

For human and agent contributors alike. (Agents: this guide is referenced from
[`AGENTS.md`](AGENTS.md), which adds the rules specific to working in a session.)

## Orientation

The docs are the source of truth for the design — read the relevant ones before
changing behaviour, and keep code and docs consistent. Start here:

| Path | What lives there |
| --- | --- |
| `README.md` | What the project is, design posture, quick start |
| `docs/architecture/overview.md` | System design — links the deeper architecture docs |
| `docs/decision-records/README.md` | ADR conventions, and the numbered decisions beside it |
| `crates/` | The Rust workspace (protocol, broker, adapter-codex, shim-claude, workplace) |

## Building and testing

- Rust toolchain via rustup (user-local). Prefer the standard library; justify
  every new dependency — it is compile time and audit surface.
- Cargo workspace under `crates/`. Shared dependency and package settings live
  in the root `[workspace.*]`; members inherit with `.workspace = true`.
- The top-level `VERSION` file is the single authoritative version source; a
  build script fails the build if `Cargo.toml` drifts from it.
- Build: `cargo build`. Test: `cargo test`. Tests that spawn a real Claude Code
  or Codex process are `#[ignore]` by default — run with `cargo test -- --ignored`.
- Keep the default `cargo test` green before opening a PR.

## Documentation style

- Prefer precise technical language. Avoid metaphors that do not reduce
  cognitive load.

## Commits

- **One logical change per commit** — keep unrelated changes in separate commits.
- Messages follow **[Conventional Commits](https://www.conventionalcommits.org/)** —
  `type(scope): summary`. Example:

  ```
  git commit -s -m "fix(shim-claude): exit on stdin close so the principal name frees" \
                -m "A closed session left the shim alive holding its broker connection, so the broker kept the name claimed."
  ```

- Body (optional): explains the what and why; use `-` bullets for multiple
  distinct changes; omit for trivial changes.
- Footer: holds references and metadata (e.g. `Refs: #43`, `Closes #43`);
  reference issues/PRs there or in the subject when applicable.
- Sign off with `git commit -s`.
- **Never include AI attribution** — no "co-authored-by"
  Claude/Anthropic/ChatGPT/OpenAI/agents, no tool attribution.
- **Do not commit harness-specific configuration** — `.codex/`, `.claude/`,
  `.grok/`, `.mcp.json`, and the like. These are machine-local (personal
  settings, absolute paths) and are gitignored by default. To share one, it
  must be explicitly whitelisted in `.gitignore` (a `!` un-ignore); by default
  nothing here is whitelisted. Prefer keeping templates in the `docs/` tree.

## Pull requests

- Work on a branch, not `main`/`master`; keep each PR to one focused change.
- Never force-push or rewrite already-pushed history without coordinating first.
