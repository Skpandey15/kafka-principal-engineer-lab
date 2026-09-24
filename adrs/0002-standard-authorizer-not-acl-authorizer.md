# ADR 0002: `StandardAuthorizer`, not the legacy `AclAuthorizer`

## Status

Accepted (WP-18; documented retrospectively in WP-20)

## Context

WP-18 needed real, enforced ACLs (grant, deny, revoke) on a KRaft-only
cluster. Kafka has historically shipped `kafka.security.authorizer.AclAuthorizer`,
which stores ACLs in ZooKeeper, alongside the newer
`org.apache.kafka.metadata.authorizer.StandardAuthorizer`, which stores
them in the KRaft metadata log.

## Problem

Which authorizer implementation should a KRaft-only curriculum
(ADR 0001) use for access control?

## Decision drivers

- ADR 0001 already commits this repository to KRaft-only, ZooKeeper-free
  clusters.
- `AclAuthorizer`'s ZooKeeper-backed ACL store is fundamentally
  incompatible with a cluster that has no ZooKeeper at all, without a
  ZooKeeper-migration bridge this repository does not otherwise need.

## Options considered

1. **`AclAuthorizer`.** Rejected outright — requires ZooKeeper, directly
   contradicting ADR 0001.
2. **`StandardAuthorizer`.** Stores ACLs in the same KRaft metadata log
   already securing topic/partition metadata — no additional
   infrastructure, no additional consistency model to reason about.

## Decision

`StandardAuthorizer` is the sole authorizer used in `lab-17-security`
and any future security-relevant lab.

## Consequences

- ACL state lives in the same Raft-replicated metadata log as
  everything else the controller quorum (WP-08) already manages —
  one consistency model for the whole cluster, not two.
- This repository never needs to teach or demonstrate ZooKeeper-based
  ACL migration, which would otherwise be required to explain
  `AclAuthorizer`'s real production deployment story.

## Risks

None material given ADR 0001 already forecloses the alternative.

## Operational implications

Any future security work in this repository (quotas, per-principal
governance, per `KAFKA_MULTI_CLUSTER_AND_DR.md` §6) builds on
`StandardAuthorizer`'s existing principal/ACL model rather than
introducing a second authorization mechanism.

## Alternatives

See Options considered above.

## Validation

`lab-17-security` demonstrates real grant, deny, and revoke behavior
against a running `StandardAuthorizer`-secured broker — not simulated,
a real client receiving a real authorization failure and then a real
success once granted.
