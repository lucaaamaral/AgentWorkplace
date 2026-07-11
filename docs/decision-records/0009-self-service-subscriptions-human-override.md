# ADR-0009 — Self-service subscriptions with human override; intersection addressing

**Status**: Accepted. Supersedes [ADR-0004](superseded/0004-topic-pubsub-human-managed-subscriptions.md).

## Context

ADR-0004 made subscriptions human-only, on the grounds that agents must not widen their own inputs. That is too rigid for the working model: an agent that discovers mid-task that it needs another domain's channel should be able to join it without blocking on the human. The human still needs final authority over context boundaries, since curated per-agent context is the point of the setup.

The addressing model also needs to be explicit: consultations are often aimed at one coworker but belong in a channel's audit scope.

## Decision

- Channel-based pub-sub with direct messages is retained from ADR-0004.
- **Subscriptions are self-service**: agents subscribe and unsubscribe themselves via the bus tool surface.
- **The human manages subscriptions with precedence**: the human can force a subscription (agent cannot drop it) or cancel one (agent cannot rejoin it) — human-set state always wins over agent self-service.
- **Subscriptions are not retroactive**: a subscription delivers messages published after it was made. Past traffic is reachable only through explicit history retrieval.
- **Every subscription change is logged** to the audit log and visible in the human interface, so self-service widening is observable and correctable rather than forbidden.
- **Addressing**: any principal pushes a message to one or more channels, one or more principals, or both. When both are given, delivery is the intersection: the listed principals that are subscribed to at least one of the listed channels. The rationale is containment of addressing errors — a mistaken extra target cannot widen delivery beyond the intersection. The dominant use case is one channel plus one principal on that channel.
- **Addressing semantics are identical for all principals, human included.** The human differs in visibility and control (sees all channels, forces/cancels subscriptions, inspects delivery state), not in send semantics; agents generally have neither.

## Consequences

- Context isolation becomes supervised rather than gate-kept: agents can seek the context they need, the human sees it happen and overrides when it is wrong.
- The broker needs per-subscription provenance (self-service vs human-forced vs human-cancelled) to enforce precedence.
- Intersection addressing lets any principal consult one specialist inside a channel without notifying every subscriber, keeps the exchange in the channel's audit scope, and bounds the blast radius of addressing mistakes.
