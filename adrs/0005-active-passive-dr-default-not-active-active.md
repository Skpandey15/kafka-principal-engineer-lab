# ADR 0005: Active/passive as the default DR pattern demonstrated, not active/active

## Status

Accepted (WP-20)

## Context

`lab-19` needed to pick a MirrorMaker 2 replication topology to build
and test. MM2 supports both active/passive (one-directional mirroring)
and active/active (bidirectional mirroring between two live clusters).

## Problem

Which topology should this repository's one real multi-cluster lab
demonstrate as its primary, tested pattern?

## Decision drivers

- Active/active introduces a correctness hazard active/passive never
  has: without topic-level exclusion or a custom `ReplicationPolicy`, a
  record mirrored `primary -> secondary` and then `secondary ->
  primary` on the same topic can loop or double-count
  (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §2).
- This repository's rule against unnecessary complexity
  (`CONTRIBUTING.md` #4) argues for demonstrating the simpler,
  more broadly applicable pattern first and describing the more complex
  one's hazard conceptually, rather than building both.
- Most DR requirements (survive a region loss) do not actually require
  simultaneous dual-region writes — that is a different, additional
  requirement active/active specifically serves.

## Options considered

1. **Active/active as the primary demonstrated pattern.** Rejected as
   the default — it would require either a custom `ReplicationPolicy`
   or explicit per-topic exclusion rules this repository's other labs
   don't otherwise need, adding infrastructure to demonstrate a
   correctness hazard rather than the underlying DR mechanism.
2. **Active/passive as the primary demonstrated pattern, active/active
   described conceptually.** Chosen — isolates the two real,
   independent things this WP needed to teach (replication mechanics,
   offset-translation mechanics) from a correctness hazard that is a
   separate decision entirely.

## Decision

`lab-19` mirrors `primary -> secondary` only
(`secondary->primary.enabled = false` in `TwoClusterEnvironment`'s
`mm2.properties`). Active/active's trade-offs and hazards are documented
conceptually in `KAFKA_MULTI_CLUSTER_AND_DR.md`, §2, not built.

## Consequences

- The lab's two tests isolate exactly the mechanics this WP needed to
  prove (real replication, real offset translation) without an
  additional bidirectional-mirroring correctness experiment competing
  for the same test suite's clarity.
- A reader evaluating an active/active requirement for their own system
  gets the conceptual trade-off here, but not a working reference
  implementation from this repository — they would need to build and
  test the exclusion/policy logic themselves.

## Risks

A learner might read "this repository demonstrates active/passive" as
"active/passive is always the right choice" — mitigated by
`KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md` Decision 1 stating explicitly
that the choice depends on whether dual-region writes are a genuine
business requirement, not a default.

## Operational implications

A future lab extending this one to active/active would need to add
explicit topic exclusion or a custom `ReplicationPolicy` before
enabling bidirectional mirroring — that is new work, not a
configuration flip.

## Alternatives

See Options considered above.

## Validation

`lab-19`'s two tests pass against the one-directional topology as
built; the active/active hazard this ADR avoids building is described,
not tested, and is named as such rather than implied to be covered.
