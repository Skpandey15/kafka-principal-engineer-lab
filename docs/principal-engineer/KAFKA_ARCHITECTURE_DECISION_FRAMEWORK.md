# Kafka Architecture Decision Framework (WP-20)

## Purpose

The roadmap's Principal Engineer decision lens (`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md#principal-engineer-decision-lens`)
is deliberately generic — a 19-question checklist meant to be walked
through out loud against a *specific* decision, not answered in the
abstract. This document is that walkthrough, applied explicitly to five
named decision points this repository's own work packages actually
faced. It is written after the evidence existed (`CONTRIBUTING.md`'s
ADR standard: never before), and each answer below traces back to a
real lab, doc, or finding rather than an opinion.

The lens, for reference:

```text
What problem are we solving?            What happens during deployment?
What are the workload characteristics?  What happens during broker failure?
What are the SLOs?                      What happens during consumer failure?
What can fail?                          How do we observe failure?
What data loss is acceptable?           How do we recover?
What duplicate processing acceptable?   How do we test recovery?
What ordering is required?              What does it cost?
What recovery time is required?         What security boundary exists?
What scale do we expect?                What alternatives exist?
What happens at 10x?                    What would make us change this decision?
```

Not every question is equally load-bearing for every decision — a
security-model decision leans hard on "what security boundary exists";
a DR architecture decision leans hard on RPO/RTO and cost. Each section
below answers the questions that actually mattered for that decision
and says so explicitly, rather than padding every section to the same
length.

## Decision 1 — Multi-cluster DR architecture: active/passive with MM2, not active/active

**Problem.** A single Kafka cluster is a single-region failure domain
(failure matrix, "Multi-cluster / region loss" row) — Kafka itself has
no built-in cross-cluster failover.

**Workload / SLOs.** `lab-19`'s own workload is intentionally small (a
handful of records) to isolate replication and offset-translation
mechanics; a real deployment's actual event rate and required RPO/RTO
must be stated before this decision is portable to it — this is a
worked *mechanism* decision, not a sized production recommendation.

**What can fail / data loss / ordering.** MM2 replication and
checkpoint translation are both asynchronous (`KAFKA_MULTI_CLUSTER_AND_DR.md`,
§3.2) — some in-flight, not-yet-replicated data is always at risk during
an actual region loss. Active/passive keeps this risk in one direction
only; active/active doubles the surface (both clusters can each be
mid-replication toward the other) for a use case (both regions serving
live writes) most DR requirements do not actually need.

**Alternatives considered.** Active/active — rejected as the default
because it introduces a correctness hazard active/passive never has
(a record mirrored back to its origin cluster under a renamed topic,
unless explicitly excluded, per §2 of the conceptual doc) for a
capability (dual-region writes) that is a genuine business requirement
in some cases but not implied by "we want DR" alone.

**Cost.** Active/active roughly doubles steady-state storage and
cross-region transfer relative to active/passive with a shorter
secondary retention (conceptual doc §7) — a real, ongoing cost, not a
one-time migration cost.

**What would change this decision.** A genuine requirement for
simultaneous multi-region writes (not just multi-region *survivability*)
— at that point active/active's added complexity is justified by an
actual requirement rather than adopted for its own sake.

## Decision 2 — `offset.lag.max` tuned down from its default for this lab

**Problem.** MM2's default checkpoint-translation precision
(`offset.lag.max = 100`) produced a translated DR-failover offset off by
9 records against a total workload of 20 (conceptual doc, §3.2, Finding
3) — confirmed via the raw checkpoint topic, not assumed.

**Workload characteristics.** This is the one place workload scale
*directly* determines a correctness-adjacent config, not just a
performance one: `offset.lag.max` is a record-count threshold, so its
appropriate value scales with a topic's actual throughput, not with
wall-clock time the way the lab's other MM2 intervals do.

**Options considered.** (a) Leave the default and accept imprecise
translation at lab scale — rejected, since it would misrepresent the
DR mechanism as broken when it is actually a tuning gap. (b) Lower
`offset.lag.max` — chosen, and explicitly documented as a lab-speed
value, not a production recommendation (conceptual doc §3.2 says so
directly).

**What would make us change this decision.** Any production topic's
real measured throughput — `offset.lag.max` should be set from that
number, following the same "no universal formula" principle the
roadmap's Non-goals section already states for partition count and
retention.

## Decision 3 — `StandardAuthorizer` (KRaft-native), not the legacy `AclAuthorizer`, for WP-18's security lab

**Problem.** WP-18 needed real ACL enforcement (deny, grant, revoke) on
a KRaft-only cluster (this repository's stated non-goal against
ZooKeeper as a primary path, roadmap Non-goals).

**Alternatives considered.** `AclAuthorizer` is the legacy,
ZooKeeper-era authorizer; it is not the forward-compatible choice for a
KRaft-first curriculum and would have required either a hybrid
ZK-metadata mode this repository explicitly avoids, or documenting a
component this repository would then need to explain as deprecated
context rather than the primary path.

**Security boundary.** `StandardAuthorizer` stores ACLs in the KRaft
metadata log itself — the same quorum already securing topic/partition
metadata, rather than a separate ZooKeeper ACL store with its own
consistency model.

**What would change this decision.** Nothing version-current — this is
the only forward-compatible authorizer for a KRaft cluster as of the
pinned `apache/kafka:4.3.1`. A future Kafka release changing this
picture would be the only thing to revisit it against.

## Decision 4 — SASL/SCRAM-SHA-512 + SASL_SSL, not mutual TLS, for WP-18's authentication

**Problem.** WP-18 needed real client authentication alongside
encryption in transit, on a single-node lab cluster.

**Alternatives considered.** mTLS (client certificate authentication)
is a legitimate production alternative — it was not chosen for this lab
because SASL/SCRAM's credential model (username/password-equivalent,
server-side salted-hash verification) is more directly comparable to
how most real organizations manage human/service credentials
(a centralized identity system issuing SCRAM credentials) than
provisioning and rotating per-client certificates, and it keeps the
lab's demonstrated mechanism (grant/deny/revoke via `StandardAuthorizer`,
Decision 3) orthogonal to the transport-security mechanism.

**Security boundary / production implications.** The lab's self-signed
CA and checked-in credentials are explicitly marked local-development-only
per `CONTRIBUTING.md` #16, with the production alternative (a real CA,
a real secrets manager) named directly in the lab's own README.

**What would change this decision.** An organization's existing PKI
infrastructure already issuing per-service certificates would make mTLS
the lower-friction production choice — this is a "which credential
model do you already operate" decision, not a claim that SCRAM is
universally superior to mTLS.

## Decision 5 — Real Testcontainers integration tests, not mocked brokers, from WP-03 onward

**Problem.** Verifying Kafka client code's *correctness* (not just that
it calls the client API without throwing) requires observing real
broker behavior — rebalances, replication, leader election, MM2
checkpoint translation — none of which a mocked `KafkaProducer`/`KafkaConsumer`
can produce.

**Evidence.** Every real finding documented across this repository's 20
WPs (three independent ones in this WP alone, §3.2 of the conceptual
doc) was discovered *because* the test ran against a real broker and
produced a real, sometimes surprising result — a mock would have
returned whatever the test author assumed, hiding exactly the gap
between assumption and actual behavior that this curriculum exists to
close.

**Alternatives considered.** Mocked/in-memory Kafka test doubles —
rejected as the primary testing strategy (`REFERENCE_REPOSITORIES.md`'s
testing-strategy section) specifically because they can confirm API
usage but not Kafka's actual behavior; used, if at all, only for
narrow, non-Kafka-behavior unit tests outside this repository's own
scope.

**Cost.** Slower test runs (real container startup, real KRaft
formation) — accepted deliberately, since the roadmap's own non-goal
against fake production claims makes this trade-off non-negotiable, not
a convenience trade-off to reconsider under time pressure.

**What would change this decision.** Nothing — this is a foundational
methodology choice for the entire curriculum, not a per-WP tuning
knob.
