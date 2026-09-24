# System Design 2: Real-Time Fraud Detection Platform

## Purpose

A second worked system-design exercise applying the Principal Engineer
decision lens end to end, this time forced through a very different
workload shape than Exercise 1 — high-volume, latency-critical,
loss-tolerant-in-a-specific-sense — to show the same lens producing a
genuinely different architecture when the requirements actually differ.

## The scenario

A payments company scores every card transaction for fraud in real
time, within a strict latency budget, before the transaction is
approved or declined. Transaction volume is an order of magnitude
higher than Exercise 1's order-processing workload, and a delayed score
is operationally worse than a slightly-stale one — the transaction has
to be approved or declined *now*, not after Kafka's own guarantees
finish settling.

## Walking the decision lens

**What problem are we solving?** Score a transaction for fraud risk
within a hard latency budget (single-digit milliseconds of Kafka-path
budget, out of a larger end-to-end SLA), at an order of magnitude more
volume than Exercise 1.

**Workload characteristics.** Extremely high event rate, small message
size, latency-critical — the opposite profile from Exercise 1's
moderate-volume, high-value orders. This changes multiple decisions
below, not just a scale number.

**SLOs.** A scoring decision must be available before the transaction's
overall approval deadline, or the system must have an explicit
fallback (e.g., default-approve or default-decline under timeout) —
the fraud model's absence must never silently block a transaction
indefinitely.

**What data loss / duplicate processing is acceptable?** A transaction
event must never be silently dropped before scoring (that's a
compliance and financial-risk issue, not just a data-quality one) — but
the fraud *score* itself, if computed twice due to redelivery, is
idempotent by construction (same transaction, same features, same
score) as long as the scoring function itself is pure, so duplicate
scoring is a wasted-compute problem, not a correctness one.

**What ordering is required?** None across transactions. Even
per-card-id ordering is not strictly required for scoring an individual
transaction (each transaction is scored on its own features, not as a
sequence) — though a *downstream* velocity-check feature (e.g., "3
transactions on this card in 60 seconds") does need to observe
same-card events close to their real arrival order, which is a
narrower requirement than this exercise's headline ordering question
appears to suggest at first.

## Architecture

```text
card-network --> transactions topic (partitioned by cardId)
                        |
                        v
              fraud-scoring consumer group (Kafka Streams)
                        |
              scored-transactions topic --> approval service
```

**Why partition by `cardId`, not `transactionId`?** Because the one
real ordering requirement that exists (the velocity-check feature)
needs same-card events to land on the same partition, processed by the
same consumer instance, in close-to-arrival order — an
otherwise-unnecessary ordering constraint this design accepts
specifically because one downstream feature depends on it, not
speculatively.

**Why Kafka Streams, not a hand-rolled consumer?** The velocity-check
feature is exactly a windowed, stateful aggregation over a keyed stream
— `docs/kafka-streams/` (WP-14)'s tumbling-window and state-store
primitives map directly onto "count transactions per card in the last
60 seconds," and Streams' task-migration/changelog-restoration model
(also WP-14) gives this a tested failure-recovery story a hand-rolled
consumer would have to reimplement.

**Why not the same multi-region hub topology as Exercise 1?** Exercise
1's hub pattern optimizes for a globally consistent *aggregate* view
fed by two regional write paths. This workload's dominant constraint is
raw per-transaction latency, not cross-region consistency — an
additional MM2 hop before scoring would add latency this SLO cannot
afford. If this platform needed multi-region *disaster recovery* (not
live cross-region aggregation), that would be a separate,
active/passive DR topology per `KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md`
Decision 1 — layered underneath this scoring architecture, not replacing
it.

## Delivery semantics

Producer (card network ingest): `acks=all`, idempotent producer — a
transaction event must not be lost, and a retried send must not
duplicate it at the broker level.

Consumer (fraud scoring): at-least-once is acceptable *because* scoring
is idempotent by construction (stated above) — this is the one place
this design deliberately does NOT add an idempotent-consumer
deduplication table (WP-13's pattern), because the cost of that extra
lookup on the hot path is not justified when duplicate scoring produces
an identical, harmless result rather than an incorrect one. This is a
explicit trade-off, not an oversight — contrast directly with Exercise
1's order-processing consumer, where a duplicate order-state
transition would NOT be harmless and idempotency was required.

## Partition count and scaling

Partition count here is dominated by throughput, not the ordering
requirement (`docs/partitioning/`'s and this WP's own partition
lifecycle discussion, `KAFKA_MULTI_CLUSTER_AND_DR.md` §4): enough
partitions to keep per-partition throughput under WP-17's measured
per-partition ceiling at this workload's actual volume, with headroom
for the stated "what happens at 10x" question — planned for, not
retrofitted after the fact, since Kafka cannot decrease partition count
later (§4).

## What would change this design

If a regulator later required an audit trail proving every transaction
was scored exactly once (not "idempotently equivalent to once"), that
changes the delivery-semantics trade-off above — it would force
introducing the idempotent-consumer pattern this design explicitly
skipped, at the cost this design deliberately avoided. That is exactly
the kind of requirement-driven reversal the Principal Engineer decision
lens's last question ("what would make us change this decision?") is
built to surface before it becomes an incident.
