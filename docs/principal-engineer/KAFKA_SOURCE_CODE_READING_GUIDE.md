# Kafka Source-Code Reading Guide (WP-20)

## Purpose

The roadmap names a source-code reading track (curriculum-map row 33)
that runs alongside the rest of the curriculum: given a behavior
already *observed* in a lab, trace it to where it actually lives in
`apache/kafka`'s own source. This document is the worked version of
that track's prescribed shape, applied to real behaviors this
repository's own labs already produced — not a Kafka internals
tutorial written from memory.

Every exercise follows the same shape (roadmap, "Source-code reading
track"):

```text
Question
   ↓
Relevant subsystem
   ↓
Locate the implementation (verified against the current source tree)
   ↓
Trace the call path
   ↓
Relate it to behavior you actually observed in a lab
   ↓
Write down what you found
```

**A note on verification.** Kafka's internal package layout changes
between releases (see `REFERENCE_REPOSITORIES.md`'s warning about
naming drift in external example repositories). This repository is
pinned to `apache/kafka:4.3.1`; before treating any class or method
name below as current, check it against the `4.3.1` tag of
`apache/kafka` yourself — this document names subsystems by role first,
specific classes second, and the specific classes were confirmed the
same way this lab's own `RemoteClusterUtils` signature was confirmed in
WP-20 (§3 below): by extracting the real artifact and reading it,
never by assuming a remembered signature is still current.

## Exercise 1 — Where does MM2 decide a remote topic's name?

**Question.** `lab-19` observed `orders-abc123` on `primary` become
`primary.orders-abc123` on `secondary`. Where does that naming decision
actually happen?

**Relevant subsystem.** MirrorMaker 2's replication policy, inside
`connect-mirror-client` — the same artifact this lab already depends on
for `RemoteClusterUtils`.

**Locate the implementation.** `org.apache.kafka.connect.mirror.DefaultReplicationPolicy`
(and its interface, `ReplicationPolicy`) — confirmed present in the
`connect-mirror-client-4.3.1.jar` this lab's `build.gradle` already
pulls in.

**Trace the call path.** `MirrorSourceConnector`/`MirrorSourceTask`
consult the configured `replication.policy.class` (defaulting to
`DefaultReplicationPolicy`) to compute each destination topic name
before creating it on the target cluster and starting to mirror
records into it.

**Relate it to lab behavior.** This is exactly why `lab-19`'s tests
compute `mirroredTopic = "primary." + topic` rather than hard-coding an
assumed convention — the naming is a real, swappable policy, not an
implementation detail this lab invented.

**What this confirms.** A custom `ReplicationPolicy` is the real
extension point an active/active deployment would use to avoid
re-mirroring a topic back to its origin cluster under a doubly-prefixed
name (conceptual doc, §2) — not something requiring a fork of MM2
itself.

## Exercise 2 — Where does checkpoint offset translation actually compute a number?

**Question.** `lab-19`'s Finding 3 (`KAFKA_MULTI_CLUSTER_AND_DR.md`,
§3.2) traced an imprecise translated offset to `offset.lag.max`. Where,
concretely, does that threshold get applied?

**Relevant subsystem.** MM2's checkpoint connector and its
offset-sync bookkeeping, inside `connect-mirror-client`.

**Locate the implementation.** `org.apache.kafka.connect.mirror.Checkpoint`
(confirmed by this lab's own diagnostic code, which used
`Checkpoint.deserializeRecord` directly to read raw checkpoint records
off `primary.checkpoints.internal` and print their real
`upstreamOffset`/`downstreamOffset` pair) and the offset-sync store the
`MirrorSourceTask`/`MirrorCheckpointTask` pair maintains, gated by the
`offset.lag.max` configuration this lab tuned down.

**Trace the call path.** `MirrorSourceTask` decides, per record, whether
replication drift since the last recorded offset-sync pair has crossed
`offset.lag.max`; if so it emits a new sync pair. `MirrorCheckpointTask`
later reads a consumer group's real committed offset from the source
cluster and interpolates a downstream offset against the *nearest*
known sync pair — which is only as accurate as how recently one was
recorded.

**Relate it to lab behavior.** This is the exact mechanism `lab-19`'s
Finding 3 diagnosed empirically before this exercise ever named a
class: the lab found the *symptom* (a stuck, wrong offset) and the
*ground truth* (the raw checkpoint record) before this document
supplies the *why* (the sync-pair interpolation and its record-count
gate). That ordering — lab evidence before source explanation — is the
whole point of this track.

**What this confirms.** `offset.lag.max` is not a knob this repository
should have needed the source to discover — but confirming *why*
lowering it fixed the symptom, rather than treating the fix as
trial-and-error, is exactly what separates "I changed a number until
the test passed" from "I understand the mechanism I'm tuning."

## Exercise 3 — Where does `enable.auto.commit`'s final commit-on-close actually fire?

**Question.** `lab-19`'s Finding 2 observed `consumer.close()` silently
overwrite an explicit `commitSync(10)` back to offset 20. Where does
that final auto-commit happen?

**Relevant subsystem.** The consumer's group-coordination client logic
(`kafka-clients`), specifically the coordinator component responsible
for offset commits.

**Locate the implementation.** `org.apache.kafka.clients.consumer.internals.ConsumerCoordinator`
— its close path performs a final synchronous auto-commit attempt when
`enable.auto.commit=true`, using the consumer's actual current fetch
position at close time, not whatever position an application last
explicitly committed.

**Trace the call path.** `KafkaConsumer.close()` → coordinator shutdown
→ (if auto-commit is enabled) one last `commitOffsetsSync`-equivalent
call against the consumer's current position, which by the time
`close()` runs had already advanced past the intended commit point
because the earlier `poll()` had buffered every available record at
once.

**Relate it to lab behavior.** This confirms `lab-19`'s fix
(`ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false`) addressed the actual
mechanism, not a symptom-level workaround — with auto-commit disabled,
this close-time commit path never fires, and the only commit that ever
happens is the test's own deliberate one.

**What this confirms.** This is the *same* subsystem (auto-commit) that
made WP-16's own consumer-lag test require careful poll-loop control —
seen here from the commit side rather than the consumption side. One
subsystem, two labs, two different failure angles.

## Exercise 4 — Subsystems worth reading next (not yet exercised by a WP-20 lab)

Per the roadmap's own subsystem list (`KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`,
"Source-code reading track"), these remain valid future exercises,
each already anchored to an existing lab's observed behavior:

- **The producer's batching and send path** — anchor: WP-17's real,
  measured batching/compression throughput effect (`lab-16`).
- **The broker's group-coordinator implementation** — anchor: WP-05's
  rebalance behavior (`lab-04`) and this WP's own consumer-group
  offset-commit findings (Exercise 3 above).
- **The controller / KRaft implementation** — anchor: WP-08's quorum
  failure/election behavior (`lab-07`).
- **Replica management (ISR tracking)** — anchor: WP-07's broker-failure
  and ISR-shrink behavior (`lab-06`).
- **Transaction coordination** — anchor: WP-09's idempotent
  producer/transaction behavior (`lab-08`).

Each should follow this document's same shape: start from the lab's
already-observed behavior, then locate and trace the source — never the
reverse.
