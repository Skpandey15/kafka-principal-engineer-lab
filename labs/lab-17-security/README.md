# Lab 17 — Security

## Quick Summary

- **Why this lab:** To secure a real, single-node Kafka broker end to end — real TLS (a self-signed CA and CA-signed cert with real SANs), real SASL/SCRAM-SHA-512 authentication, and real `StandardAuthorizer` ACL enforcement — with genuine deny/grant/revoke against an actual broker, not simulated.
- **How to run:** `cd platform/kafka-security && bash certs/generate-certs.sh && docker compose up -d`, then `./gradlew runSecurityDemo -Pusername=admin -Ppassword=admin-secret` (and again as `reader`, denied until you grant an ACL); `./gradlew test` for the 7-test suite against the real, already-running broker.
- **Expected input:** Docker, JDK 21+; a dedicated, single-node environment (`platform/kafka-security/`), not the shared WP-07 cluster, since TLS/SASL/ACLs are single-broker configuration concerns.
- **Expected output:** An authenticated admin producing and consuming successfully; a wrong SCRAM password failing authentication; a client with no truststore failing the TLS handshake outright; a reader denied until granted both topic AND group ACLs, then successfully denied again after revocation.
- **What we learned:** Six real findings building this lab's infrastructure, most notably: the broker certificate needs a SAN covering every hostname a client actually connects with, not just a CN; the advertised listener address must match what a client can reach for its *second*, metadata-directed connection, not just the first bootstrap one; `User:ANONYMOUS` must be in `super.users` because the broker's own internal controller-registration traffic runs as ANONYMOUS; and ACL checks are ordered — group authorization is checked before topic authorization, so a write-only ACL alone lets you produce but not consume, even with no group ACL denial explicitly configured.

## Objective

A real, single-node Kafka broker secured with TLS (a real, self-signed
CA and a CA-signed broker certificate) and SASL/SCRAM-SHA-512
authentication, plus real `StandardAuthorizer` ACL enforcement — real
deny, real grant, real revoke, all against an actual broker, not
simulated.

See [`docs/security/KAFKA_SECURITY.md`](../../docs/security/KAFKA_SECURITY.md)
for the full conceptual depth, SIX real findings from actually building
this lab's infrastructure, and 7 Principal Engineer questions. This
README covers setup, commands, and a condensed experiment walkthrough.

## Prerequisites

- Docker (this lab needs a NEW, dedicated environment,
  `platform/kafka-security/` — not the WP-07 cluster)
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

### Why single-node here

TLS handshakes, SASL authentication, and ACL enforcement are all
properties of ONE broker's configuration — they don't need multi-broker
replication to demonstrate, and a single cert/keystore avoids ALSO
having to secure inter-broker replication traffic (a real, separate
concern named but not built — see the conceptual doc, Section 9).

### Why not Testcontainers here

Same reasoning WP-16 already established: this WP's subject is real,
non-trivial-to-provision infrastructure (a self-signed CA, a CA-signed
cert with real SANs, real SCRAM credentials baked in at storage-format
time) — tests run against the ALREADY-RUNNING
`platform/kafka-security/` environment.

## Environment

**`platform/kafka-security/`** — a single combined broker+controller
node with TWO client-facing listeners (`INTERNAL`, for in-container
clients; `CLIENT`, advertised as `localhost:9096` for host-side
clients — see the conceptual doc, Section 6, for why one listener isn't
enough), both SASL_SSL, plus a PLAINTEXT-only `CONTROLLER` listener
(never published — see Section 7).

## Architecture

```text
platform/kafka-security/
  certs/generate-certs.sh   real CA + broker keystore/truststore (SAN-correct)
  config/server.properties  SASL_SSL, SCRAM-SHA-512, StandardAuthorizer
  entrypoint.sh             custom entrypoint -- formats storage WITH real SCRAM users

labs/lab-17-security/
  support/  SecureClientProps  -- real SASL_SSL/SCRAM client config builder
  acl/      SecurityDemoApp
```

## Setup

```bash
cd platform/kafka-security
bash certs/generate-certs.sh
docker compose up -d
# wait for the broker to report healthy
```

## Commands

```bash
./gradlew runSecurityDemo -Pusername=admin -Ppassword=admin-secret
./gradlew runSecurityDemo -Pusername=reader -Ppassword=reader-secret   # denied until you grant an ACL
```

## Implementation

See the conceptual doc for full source-level discussion, including SIX
real findings: a Windows/Docker path-mounting bug, a container
permission mismatch, TLS SAN verification (fixed in three separate
places), the authorizer gating the broker's own internal traffic, the
advertised-listener timeout, and a real ACL-check-ordering discovery
(group authorization checked before topic authorization).

## Verification

```bash
./gradlew test
```

7 automated integration tests against the real, already-running broker
— see "Automated tests" below.

## Experiment

### Experiment 1 — real TLS + SASL/SCRAM authentication

```bash
./gradlew test --tests "*anAuthenticatedAdminCanProduceAndConsume*"
./gradlew test --tests "*wrongPasswordForARealScramUser*"
./gradlew test --tests "*aClientWithNoTruststore*"
```

Real evidence: conceptual doc, Sections 3-4.

### Experiment 2 — real ACL deny, grant, and revoke

```bash
./gradlew test --tests "*aReaderWithNoGrantedAcls*"
./gradlew test --tests "*writeAclAlone*"
./gradlew test --tests "*grantingReadAndGroupAcls*"
./gradlew test --tests "*revokingAnAcl*"
```

Real evidence, including the group-vs-topic check-ordering finding:
conceptual doc, Section 5.

## Failure injection

No dedicated failure-matrix row names this WP.

## Troubleshooting

### `Timed out waiting for a node assignment`

A real finding — see the conceptual doc, Section 6: the advertised
listener address must match the address a client can ACTUALLY reach
for its second, metadata-directed connection, not just the first
bootstrap one.

### `SSL handshake failed`

A real finding — see the conceptual doc, Section 3: the broker
certificate needs a SAN covering every hostname a client actually
connects with, not just a CN.

### The broker never finishes starting, `ClusterAuthorizationException ... CLUSTER_ACTION`

A real finding — see the conceptual doc, Section 7: `User:ANONYMOUS`
needs to be in `super.users` too, since the broker's own internal
controller-registration traffic runs as ANONYMOUS on the PLAINTEXT
`CONTROLLER` listener.

## Cleanup

```bash
cd platform/kafka-security && docker compose down
```

## Automated tests

`./gradlew test` — 7 tests, against the real, already-running broker:

1. `anAuthenticatedAdminCanProduceAndConsumeOverRealTlsAndSasl`
2. `wrongPasswordForARealScramUserFailsAuthentication`
3. `aClientWithNoTruststoreFailsTheRealTlsHandshake`
4. `aReaderWithNoGrantedAclsIsDeniedProducing`
5. `writeAclAloneAllowsProducingButNotConsuming`
6. `grantingReadAndGroupAclsThenAllowsConsuming`
7. `revokingAnAclDeniesFurtherAccess`

Every timing-sensitive assertion (ACL propagation, especially) uses a
bounded condition-polling loop, per this repository's established test
convention.

## Production considerations

See the conceptual doc's Section 8 for what real secrets management
looks like (and why this lab's own checked-in credentials are safe
ONLY in this specific, local, single-user context), and Section 9 for
what this lab deliberately does not build (mTLS, controller-listener
security, credential rotation, quotas).

## Principal Engineer questions

See the conceptual doc's Section 10 for all 7 questions with detailed,
experimentally-grounded answers.
