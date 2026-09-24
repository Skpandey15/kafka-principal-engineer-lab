# System Design 1: Multi-Region Order Processing Platform

## Purpose

A worked system-design exercise applying the Principal Engineer decision
lens (`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md#principal-engineer-decision-lens`)
end to end, forced through partition key, replication, delivery
semantics, schema, retention, retry/DLQ, ordering, scaling, DR, security,
and cost decisions together — grounded throughout in this repository's
own real labs, never an invented capability no lab actually
demonstrated.

## The scenario

An e-commerce company processes orders across two regions (US and EU),
serving customers in both. Orders placed in a region must be processed
by that region's services for latency reasons, but a full regional
outage must not lose accepted orders, and a global inventory service
needs a consistent, ordered view of every order regardless of origin
region.

## Walking the decision lens

**What problem are we solving?** Regional latency for order placement,
combined with cross-region durability and a globally consistent
inventory view — three requirements that pull in different directions
if solved naively (e.g., a single global cluster would violate the
latency requirement; two fully independent clusters would violate the
consistent-inventory requirement).

**Workload characteristics.** Orders are moderate-volume, high-value
events (unlike, say, clickstream data) — favoring stronger durability
guarantees over maximum throughput.

**SLOs.** Order acceptance must complete within regional latency
budgets (tens of milliseconds, not cross-region round-trips). Inventory
must reflect every accepted order within a bounded, stated staleness
window (not "eventually," a number).

**What can fail? What data loss is acceptable?** A regional Kafka
cluster loss must not lose already-accepted orders — RPO for orders is
effectively zero (a placed, acknowledged order must survive). Inventory
staleness during a region's mirroring lag is acceptable up to the
stated SLO window; it is a latency risk, not a durability one, as long
as the underlying event isn't lost.

**What ordering is required?** Per-order-id ordering is required
(state transitions for one order must be processed in sequence);
cross-order ordering is not.

## Architecture

```text
US region: us-orders-cluster (writes accepted here for US customers)
EU region: eu-orders-cluster (writes accepted here for EU customers)

us-orders-cluster --[MM2: us -> global]--> global-inventory-cluster
eu-orders-cluster --[MM2: eu -> global]--> global-inventory-cluster

Inventory service consumes ONLY from global-inventory-cluster,
reading both us.orders.* and eu.orders.* mirrored topics.
```

**Why not a single global cluster?** Violates the regional-latency SLO
— every order write from the EU would cross the Atlantic before being
acknowledged.

**Why not fully independent regional clusters with no bridge?**
Violates the consistent-global-inventory requirement — inventory would
need to separately consume from two clusters with no unified offset
model, and a regional outage would make that region's orders invisible
to inventory indefinitely rather than just delayed.

**Why active/passive-per-region into a third hub cluster, not
active/active between US and EU directly?** This is architecturally an
active/passive fan-in (each regional cluster is "active" for its own
writes, "passive"/source-only toward the hub) — it avoids the
same-topic bidirectional-mirroring hazard ADR 0005 and
`KAFKA_MULTI_CLUSTER_AND_DR.md` §2 describe, since US and EU never mirror
directly to each other, only outward to the hub.

## Partition key

Partition key: `orderId`. Guarantees per-order ordering (the actual
requirement) without forcing global ordering across unrelated orders,
which would cap throughput at a single partition's ceiling for no
benefit (`docs/partitioning/`, WP-04's core lesson).

## Delivery semantics

Producer: idempotent producer (`enable.idempotence=true`) plus
`acks=all` on each regional cluster — a lost ACK on retry must not
double-place an order. Transactions (WP-09) are not required here since
this is a single-topic produce path per order event, not a
multi-partition atomic write.

Consumer (inventory service): at-least-once with an idempotency key
(`orderId` + event type) against a durable processed-events store — the
same pattern WP-13's production idempotent-consumer treatment
establishes, applied here because MM2 replication plus a consumer
restart can both produce redelivery.

## Schema

Avro with backward-compatible evolution only, via a shared Schema
Registry the inventory service and both regional order services all
validate against before producing — a schema break in one region must
not silently corrupt the hub's consumer (`docs/schema-evolution/`,
WP-10).

## Retention and DR

Regional clusters: retention long enough to survive the DR runbook's
stated RTO for a hub-cluster rebuild (if the hub is ever lost, it can be
recreated and backfilled from both regional clusters' retained history).
Hub cluster: standard operational retention, since it is fed
continuously, not relied upon as the durability source of truth for
already-mirrored data.

## Cost

Per `KAFKA_MULTI_CLUSTER_AND_DR.md` §7's formula, this design pays
cross-region transfer cost twice (US→hub, EU→hub) but avoids
active/active's doubled steady-state storage, since orders are written
once, in their origin region, and mirrored once outward — not
symmetrically replicated back and forth.

## What would change this design

If the business later required each region to also see the *other*
region's live order state (not just aggregated inventory), that is a
new requirement this design does not serve — it would force a genuine
active/active evaluation (`KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md`,
Decision 1) with its associated cost and correctness trade-offs, not a
configuration change to this architecture.
