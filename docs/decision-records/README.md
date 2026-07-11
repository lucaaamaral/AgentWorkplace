# Decision records

Architecture Decision Records (ADRs) for AgentWorkplace. One decision per file, numbered sequentially, never renumbered.

## Conventions

- **Filename**: `NNNN-short-slug.md` (four-digit, zero-padded, kebab-case).
- **Statuses**: `Accepted`, `Proposed`, `Superseded by ADR-NNNN`.
- **Immutability**: an accepted ADR is not edited to change its meaning. If a decision changes, write a new ADR that supersedes it, update the old ADR's status line, and move the old file to [`superseded/`](superseded/).
- **Format**: Status / Context / Decision / Consequences, with rejected alternatives recorded under Context or Decision as appropriate.
