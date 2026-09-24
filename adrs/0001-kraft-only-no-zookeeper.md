# ADR 0001: KRaft-only architecture, no ZooKeeper as a primary path

## Status

Accepted (established WP-01/WP-02; documented retrospectively in WP-20)

## Context

Every Kafka deployment before Kafka 4.0's KRaft graduation relied on
ZooKeeper for cluster metadata and controller election. Apache Kafka
4.x removes ZooKeeper support entirely for new clusters. This
repository had to pick which architecture to teach as the primary path
from its very first lab.

## Problem

Should this curriculum teach ZooKeeper-based Kafka (still the deployed
reality in many existing production clusters) or KRaft (the current and
future architecture), and how much weight should each get?

## Decision drivers

- The pinned Kafka version (`apache/kafka:4.3.1`) does not support
  ZooKeeper mode for new clusters at all.
- Teaching a deprecated architecture as the primary path would mean
  every lab's infrastructure goes stale the moment ZooKeeper support is
  fully removed upstream.
- Real engineers joining this curriculum are more likely to encounter
  KRaft in any new deployment going forward.

## Options considered

1. **ZooKeeper as primary, KRaft as a footnote.** Matches the majority
   of currently-deployed production clusters at the time this
   repository started. Rejected — teaches an architecture already being
   phased out as the default mental model.
2. **KRaft as primary, ZooKeeper as historical context only.** Matches
   where Kafka is going and what the pinned version actually supports
   as a first-class path.
3. **Cover both equally.** Rejected — doubles every controller/metadata
   lab's infrastructure for a comparison this curriculum doesn't need;
   splits learner attention without a clear payoff.

## Decision

KRaft is the sole architecture for every hands-on lab. ZooKeeper is
covered only as historical/legacy context, explicitly marked as such,
never as the primary path (roadmap Non-goals, first bullet).

## Consequences

- Every lab's cluster setup (`platform/kafka-cluster/`,
  `platform/kraft-quorum/`, and every Testcontainers-based lab
  environment including `lab-19`'s `TwoClusterEnvironment`) is
  KRaft-native from WP-02 onward — no ZooKeeper dependency anywhere in
  this repository.
- A learner using this repository to understand a real, existing
  ZooKeeper-based production cluster needs to separately map KRaft
  concepts (controller quorum, `KAFKA_CONTROLLER_QUORUM_VOTERS`) back
  onto ZooKeeper's older equivalents — this repository does not do that
  mapping for them.

## Risks

None material — this tracks upstream Kafka's own direction, not a
speculative bet against it.

## Operational implications

Any lab or platform component added to this repository going forward
must be KRaft-compatible by default; introducing a ZooKeeper dependency
anywhere would be a regression against this decision.

## Alternatives

See Options considered above.

## Validation

Every cluster this repository has stood up since WP-02 — `lab-01`
through `lab-19` — runs KRaft successfully, including multi-voter
quorum failure scenarios (WP-08) and the two-cluster MM2 environment
(WP-20) — real, repeated evidence this decision holds across every
topology this curriculum has needed so far.
