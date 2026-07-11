# ADR-0013 — Rust as the implementation language

**Status**: Accepted

## Context

Candidates were Go, Rust, C, and C++ (TypeScript was excluded by the maintainer). Constraints from prior decisions: first-class support on macOS, Linux, and Windows ([ADR-0011](0011-multiplatform-support.md)); a long-running daemon parsing JSON from multiple local sockets — a security-relevant input surface; single-binary distribution desirable; a TUI as the first human interface.

## Decision

Rust, for the entire core: broker, adapters, shim, and TUI.

Rationale:

- Memory safety without a garbage collector on the untrusted-input parsing surface.
- First-class cross-platform toolchain and cross-compilation for all three target platforms; single static binaries.
- The ecosystem covers every component directly: async runtime (tokio), JSON/JSON-RPC (serde), SQLite (rusqlite), WebSocket (tokio-tungstenite), and TUI (ratatui) crates.
- Maintainer goal: gaining Rust exposure. Recorded as a real input to the decision, not an afterthought.

Rejected:

- **Go** — operationally equally viable (single binary, trivial cross-compilation, mature ecosystem); not chosen primarily on the maintainer-exposure criterion.
- **C / C++** — manual memory safety on a security-relevant surface, no WebSocket/HTTP/JSON in the standard library, and a build matrix across MSVC/clang/gcc; highest maintenance for no offsetting benefit here.
- **TypeScript** — excluded by the maintainer; would also add a runtime dependency to every install.

## Consequences

- Cargo workspace layout; contributors need the Rust toolchain; compile times are the accepted cost.
- The Claude shim is also Rust and must implement the Claude Code channels stdio protocol directly (the official example plugins are Bun scripts and serve as protocol references only).
- Binary naming: the product is **AgentWorkplace**; the shipped commands are `workplace daemon` (broker) and `workplace cli` (human interface).
