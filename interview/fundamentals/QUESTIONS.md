# Interview Questions — Fundamentals (M0–M2)

Grounded in this repository's own labs. Each question names which lab
or doc actually taught the answer — use that as your source of truth,
not general Kafka folklore.

## Architecture & core concepts

1. What is a partition, and why can't a topic's ordering guarantee
   extend beyond a single partition? (`docs/partitioning/`, `lab-03`)
2. What does the controller (KRaft) actually do that a broker doesn't?
   (`docs/kraft/`, `lab-07`)
3. Why is Kafka described as a distributed log rather than a message
   queue — what operational consequence follows from that distinction?
   (`docs/fundamentals/`, `KAFKA_MENTAL_MODEL.md`)
4. What is the difference between a record's offset, a consumer's
   current position, and its committed offset? Why does this
   distinction matter for reasoning about duplicate processing?
   (`KAFKA_MENTAL_MODEL.md`, `lab-05`)

## Producer

5. Explain `acks=0`, `acks=1`, and `acks=all` in terms of what the
   broker does before responding — not just "reliability level." (`lab-02`, `lab-06`)
6. What does `enable.idempotence=true` actually prevent, mechanically?
   (`lab-08`)
7. A producer's `send()` future fails. Name three genuinely different
   root causes and how you'd distinguish them. (failure matrix,
   producer-side section)

## Consumer & consumer groups

8. Walk through what happens when a consumer in a group crashes without
   committing its last processed offset. (`lab-04`, `lab-05`)
9. What's the actual difference between `commitSync()` and
   `commitAsync()`, and when would an application prefer one over the
   other? (`lab-05`)
10. Why can a single `poll()` call return more records than you
    intended to process before your next commit — and why does that
    matter for correctness, not just performance? (`lab-16`'s consumer-lag
    test finding; `lab-19`'s DR offset-translation test hit the identical
    class of bug from the commit side — see `KAFKA_MULTI_CLUSTER_AND_DR.md` §3.2, Finding 2)
11. What does `enable.auto.commit=true`'s default actually commit, and
    when — and what real bug can this cause if you also commit
    explicitly? (`KAFKA_MULTI_CLUSTER_AND_DR.md` §3.2, Finding 2)

## Delivery semantics

12. What does Kafka's own "exactly-once semantics" actually guarantee,
    precisely — and what does it not guarantee about a downstream
    database write? (`lab-08`, `docs/delivery-semantics/`)
13. What is the dual-write problem, and what pattern addresses it
    without distributed transactions across two different systems?
    (`lab-11`, the transactional outbox)
14. Give a concrete scenario where at-least-once delivery produces a
    duplicate, and the specific mechanism that prevents that duplicate
    from becoming a duplicate business side effect. (`lab-12`,
    idempotent consumer)

## Replication & failure

15. With `acks=all` and `min.insync.replicas=2` on a 3-replica topic,
    a broker dies mid-write. Was the last acknowledged write
    necessarily durable? (`lab-06`)
16. What happens to the ISR when a follower falls too far behind, and
    why does that protect `acks=all`'s guarantee rather than
    undermine it? (`lab-06`, failure matrix)

## Schema evolution

17. What makes a schema change "backward compatible" in the strict
    sense a compatibility-mode check enforces — give an example of a
    change that looks safe but isn't. (`lab-09`)
