# Decision records

Architecture Decision Records (ADRs) for AgentWorkplace. One decision per file, numbered sequentially, never renumbered.

## Conventions

- **Filename**: `NNNN-short-slug.md` (four-digit, zero-padded, kebab-case).
- **Statuses**: `Accepted`, `Proposed`, `Superseded`.
- **Immutability**: an accepted ADR is never edited to change its meaning. If a decision changes, a new ADR supersedes it.
- **Superseding ADRs are self-contained**: a superseding ADR carries the full necessary context and the complete decision — never a delta against the ADR it replaces. The newest ADR in any chain is the whole record; a reader must never need to assemble the current decision from partially valid predecessors.
- **`Superseded by` field (mandatory)**: a superseded ADR carries, immediately after its Status line, a `Superseded by` field linking the successor ADR. Its file moves to [`superseded/`](superseded/).
- **Format**: Status / Context / Decision / Consequences, with rejected alternatives recorded under Context or Decision as appropriate.
