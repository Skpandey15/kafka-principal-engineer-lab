# ADR 0004: Real Testcontainers integration tests, not mocked brokers, from WP-03 onward

## Status

Accepted (established WP-03; documented retrospectively in WP-20)

## Context

Verifying Kafka client code can be done two ways: mocking the
`KafkaProducer`/`KafkaConsumer` APIs (fast, no real broker needed), or
running tests against a real broker via Testcontainers (slower, but
exercises real broker behavior).

## Problem

Which testing strategy should this curriculum adopt as its default,
starting from the first lab with any non-trivial client code?

## Decision drivers

- This repository's stated rule against fake production claims
  (`CONTRIBUTING.md` #1) extends naturally to tests: a test that
  confirms "my code calls the client API as intended" is not the same
  claim as "my code behaves correctly against real Kafka."
- Every real finding this repository has documented — including three
  independent ones in WP-20 alone (`KAFKA_MULTI_CLUSTER_AND_DR.md`,
  §3.2) — was discovered because a test ran against real broker/MM2
  behavior and produced a real, sometimes-surprising result.

## Options considered

1. **Mocked/in-memory Kafka test doubles as the primary strategy.**
   Faster tests, but structurally cannot produce the class of finding
   this repository is built around — a mock returns whatever the test
   author assumed, which hides exactly the gap between assumption and
   real behavior this curriculum exists to close.
2. **Real Testcontainers-backed integration tests as the primary
   strategy.** Slower (real container startup, real KRaft formation,
   real MM2 herder startup), but every test result is evidence about
   actual Kafka behavior, not about the test author's mental model of
   it.

## Decision

Real, Testcontainers-backed integration tests are the default testing
strategy from WP-03 onward, documented in
`REFERENCE_REPOSITORIES.md`'s testing-strategy section. Mocked doubles,
if used at all, are confined to narrow, non-Kafka-behavior unit tests
outside this repository's actual scope.

## Consequences

- Every lab's test suite doubles as a source of real findings, not just
  a correctness gate — the auto-commit-overwrite bug, the checkpoint
  staleness bug, and the `offset.lag.max` precision bug (all WP-20,
  `lab-19`) would not have been discoverable any other way.
- Test run times are materially longer than a mocked equivalent would
  be — accepted deliberately as the cost of this guarantee, not
  something to optimize away by reverting to mocks.

## Risks

Flaky tests from real timing dependencies (container startup races,
replication lag) are a real risk of this approach — mitigated by this
repository's parallel convention of deterministic, bounded
condition-polling rather than fixed sleeps (see
`waitForTranslatedOffset` in `lab-19` for a direct example: polling
until a value reaches an expected state, not sleeping a guessed
duration).

## Operational implications

Every new lab going forward is expected to follow this same pattern —
a lab whose test suite does not exercise a real, running Kafka
component is the exception requiring justification, not the default.

## Alternatives

See Options considered above.

## Validation

The volume and specificity of real findings documented across this
repository's own `docs/` and lab READMEs — each one traceable to an
actual test run against actual infrastructure — is itself the ongoing
validation of this decision.
