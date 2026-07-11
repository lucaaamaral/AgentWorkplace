# ADR-0004 — Topic-based pub-sub with human-managed subscriptions

**Status**: Superseded by [ADR-0009](../0009-self-service-subscriptions-human-override.md)

## Context

The operating model is one agent per desktop with deliberately curated context (security, business, performance, ...). The human controls what each agent knows in order to get the best result from each; the bus must not erode that control.

A single shared room (murmur's v1 model) works for two agents; with specialized agents it either broadcasts noise into every context or relies on @-mention discipline the bus cannot enforce.

## Decision

Multi-channel topics plus direct messages. Subscriptions are administered by the human, not self-service by agents: an agent cannot widen its own inputs by joining topics.

## Consequences

- Subscriptions are the context-isolation mechanism. Deciding who hears what is the same act as deciding what context each agent holds.
- Nothing enters an agent's context except messages on topics the human subscribed it to — cross-agent consultation arrives as one compact message, not a merged context.
- Topic administration is part of the human interface (`awp topic`, `awp agent add --topics ...`); the broker enforces subscription checks on fan-out.
