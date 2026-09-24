# ADR 0006: Consolidate the curriculum plan to exactly 20 work packages

## Status

Accepted (documented in the roadmap prior to WP-07; retrospectively
recorded as an ADR in WP-20)

## Context

The roadmap was originally planned as 31+ separate work packages (plus a
lettered insertion, WP-02A) before WP-07 onward were built. Several of
those planned WPs (e.g., original WP-22 through WP-31: system design,
ADRs, the PE decision framework, source-code reading, interview banks,
partition lifecycle, cluster rebalancing/Cruise Control, multi-cluster/DR,
platform governance, cost engineering, Kubernetes/Strimzi) were each
individually thin — real content, but not enough to justify a fully
separate reviewable unit of work per `CONTRIBUTING.md` #5's "no giant
commits, but also no unnecessarily fragmented ones" spirit.

## Problem

Should the curriculum keep growing its work-package count indefinitely
as new topics are scoped, or consolidate related thin topics into fewer,
more substantial work packages?

## Decision drivers

- `CONTRIBUTING.md` #5 requires each WP to be "a coherent piece of the
  curriculum" — a WP that only produces a short conceptual doc with no
  accompanying lab is a weaker reviewable unit than one that pairs
  conceptual depth with real infrastructure.
- Level 5 (fleet/platform engineering: partition lifecycle, cluster
  rebalancing, multi-cluster/DR) and Level 6 (Principal Engineer:
  governance, cost, system design, ADRs) topics are, in practice, not
  independently useful — governance and cost reasoning are meaningless
  without the fleet-operations concepts they're applied to, and both
  ultimately feed the same ADRs and system designs.
- An ever-growing WP count with no natural stopping point makes "when is
  the curriculum done" an open question rather than a planned one.

## Options considered

1. **Keep all 31+ planned WPs separate.** Rejected — produces a long
   tail of thin WPs (a single conceptual doc each) that don't meet the
   "substantial, reviewable unit" bar the rest of the curriculum holds
   to.
2. **Consolidate Level 5/6 into one capstone WP (WP-20), keep WP-01
   through WP-19 substantially as originally sequenced.** Chosen — no
   learning material is deleted; every original topic still has a
   deliberate place, either as its own WP or grouped into a related one
   (the full historical mapping is documented in the roadmap itself).

## Decision

The curriculum is built as exactly 20 work packages. WP-20 consolidates
every originally-separate Level 5/6 capstone topic (system design,
ADRs, PE decision framework, source-code reading, interview banks,
milestone assessments, partition lifecycle, cluster rebalancing/Cruise
Control, multi-cluster/DR, platform governance, cost engineering,
Kubernetes/Strimzi) into one final work package.

## Consequences

- WP-20's own scope (this document's parent WP) is unusually broad
  compared to WP-01 through WP-19 — addressed by building exactly one
  piece of new real infrastructure (`lab-19`) and treating the rest
  conceptually, each grounded in prior real evidence or an explicit
  scope boundary (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §1).
- The roadmap's own historical mapping table (original plan → current
  numbering) is the authoritative reference for any prior discussion or
  commit message citing an old WP number — a real cost of renumbering,
  accepted once, rather than repeatedly as new consolidations might
  otherwise be tempting later.

## Risks

A learner following an old, pre-consolidation reference to "WP-27
(Kubernetes/Strimzi)" would need the roadmap's mapping table to find
where that content actually landed — mitigated by that table being
part of the roadmap itself, not a separate document that could go out
of sync.

## Operational implications

Any future curriculum expansion should be evaluated against this same
bar — a new topic substantial enough to be its own WP, or genuinely
related enough to an existing one to extend it, rather than defaulting
to "add another WP number."

## Alternatives

See Options considered above.

## Validation

WP-01 through WP-20 each shipped as a real, reviewable PR with real labs
or real documentation (per this repository's own PR history) — the
20-WP structure held across the curriculum's entire actual build, not
just as an initial plan that was later abandoned.
