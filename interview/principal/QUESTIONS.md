# Interview Questions — Principal Engineer (M3–M5)

Grounded in this repository's own labs, docs, ADRs, and system designs.
These questions expect trade-off reasoning, not a memorized "best
practice" — the Principal Engineer decision lens
(`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md#principal-engineer-decision-lens`)
is the expected reasoning shape for most of these.

## Operations & failure

1. Two failures overlap: a broker dies while a consumer group is
   mid-rebalance. What guarantees from each individual failure
   scenario still need to hold, and how would you prove it in a test,
   not just an argument? (`lab-18`)
2. Consumer lag is growing with no producer backpressure. What's the
   actual failure mode if nothing changes, and what's the real,
   irreversible consequence once it crosses a threshold? (`lab-15`,
   failure matrix)
3. Walk through why `offset.lag.max`'s default can make MirrorMaker 2's
   consumer-group offset translation wildly inaccurate at low
   throughput, and how you'd size it correctly for a specific topic.
   (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §3.2, Finding 3)

## Multi-cluster & DR

4. A stakeholder asks for "active/active for resilience." What
   follow-up questions determine whether that's the right choice, and
   what does active/active cost that active/passive doesn't? (ADR
   0005, `KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md` Decision 1)
5. Why doesn't replicating data between two clusters with MirrorMaker
   2 alone make a DR failover cheap? What additional mechanism is
   required, and what does it depend on being accurate?
   (`lab-19`, `RemoteClusterUtils.translateOffsets`)
6. What is RPO/RTO, and why must they be defined before a DR
   architecture is chosen rather than measured after the fact from
   whichever tool was picked? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §2)

## Platform & scale

7. Why doesn't adding a broker to a cluster automatically rebalance
   partition placement? What actually closes that gap, and why isn't
   it a broker feature? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §5)
8. When does a shared Kafka platform need client quotas, and what's
   the actual enforcement mechanism (throttling, not rejection) once a
   client exceeds one? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §6)
9. A partition count decision made at topic creation turns out to be
   too low a year later. What are your actual options, and what do you
   lose by exercising each one? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §4)
10. Should this organization run Kafka on Kubernetes? What's the actual
    question behind that question, and what does an operator like
    Strimzi manage that a bare StatefulSet can't reason about safely
    on its own? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §8)

## Cost & trade-offs

11. Write the cost formula for a cross-region DR setup with every term's
    assumption stated. What changes under active/active versus
    active/passive? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §7)
12. Defend a specific partition-key choice for a workload with one real
    ordering requirement and one hot-key risk — what did you trade
    away, and why was it the right trade for the stated requirement?
    (`system-design/01-multi-region-order-platform.md`,
    `system-design/02-real-time-fraud-detection-platform.md`)

## Security

13. Why choose `StandardAuthorizer` over the legacy `AclAuthorizer` for
    a KRaft-only cluster, and what would have to be true for that
    decision to be wrong? (ADR 0001, ADR 0002)
14. When would you choose mutual TLS over SASL/SCRAM for client
    authentication, and what does that decision actually depend on
    (hint: not "which is more secure in the abstract")? (ADR 0003)

## Testing & methodology

15. Why does this curriculum insist on real Testcontainers-backed
    integration tests over mocked brokers, and what specific, real bugs
    would a mock have hidden? (ADR 0004, `KAFKA_MULTI_CLUSTER_AND_DR.md`
    §3.2's three findings)

## Source-level reasoning

16. Trace `enable.auto.commit`'s final commit-on-close to the actual
    client-internals code path, and explain a real bug it caused in
    this repository's own test suite. (`KAFKA_SOURCE_CODE_READING_GUIDE.md`,
    Exercise 3)
17. Given any surprising behavior you've personally observed in a
    Kafka client or broker, describe how you would locate its actual
    implementation in `apache/kafka`'s source rather than guessing from
    documentation alone. (`KAFKA_SOURCE_CODE_READING_GUIDE.md`, general
    method)
