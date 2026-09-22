# Lab 14 — Spring Kafka

## Objective

Map Spring Kafka's production abstractions onto the native Kafka
mechanisms every prior lab already built by hand: listener containers
and acknowledgment modes (WP-06), error handling and dead-letter
publishing (WP-13), non-blocking retry topics (contrasted against
WP-13's own blocking approach), and declarative transactions (WP-09).
Per `CONTRIBUTING.md` §18, every framework feature here is explained
alongside the Kafka mechanism it wraps -- see the conceptual doc.

See [`docs/spring-kafka/SPRING_KAFKA.md`](../../docs/spring-kafka/SPRING_KAFKA.md)
for the full conceptual depth, SEVEN real findings from building this
lab (including two genuine Spring Boot 4.x gotchas), and 7 Principal
Engineer questions. This README covers setup, commands, and a condensed
experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster for manual runs;
  its own automated tests use `@EmbeddedKafka` instead, needing no
  Docker at all)
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses -- see lab-03's
README, "Why a separate project," for the full rationale.

### Why Spring Boot here and nowhere else

Every prior lab is plain Java with a hand-written `main` method -- no
dependency-injection framework. This is the FIRST lab to use Spring
Boot, because Spring Kafka's own idioms
(`@KafkaListener`/`KafkaTemplate`/`@Transactional`) are built around
Spring's DI container; using them without Spring Boot would mean
hand-wiring the same `ApplicationContext` machinery Spring Boot already
automates.

## Environment

Reuses **`platform/kafka-cluster/`** (WP-07) for manual runs via
`runApp`. This lab's own automated tests run against
**`@EmbeddedKafka`** -- Spring Kafka's own first-class, in-process test
broker (a real, unmocked broker, just not containerized) -- the tool
`docs/references/REFERENCE_REPOSITORIES.md` names specifically for this
WP.

## Architecture

```text
platform/kafka-cluster/    (reused for manual runs only)

labs/lab-14-spring-kafka/
  config/    ListenerContainerFactoryConfig  -- manual-ack + error-handling factories, the app's own plain KafkaTemplate
  listener/  ManualAckListener, ErrorHandlingListener, RetryableTopicListener
  txn/       TransactionalProducerConfig, TransactionalOrderPublisher
  support/   OrderEvent
```

## Setup

```bash
cd platform/kafka-cluster && docker compose up -d
```

(Not needed to run `./gradlew test` -- only for `./gradlew bootRun`.)

## Commands

```bash
./gradlew bootRun
```

Runs the full Spring Boot application (all listeners active) against
the reused WP-07 cluster.

## Implementation

See the conceptual doc for full source-level discussion, including
SEVEN real findings from actually building this lab -- two genuine
Spring Boot 4.x surprises (autoconfiguration moved to its own artifact;
defining one custom `KafkaTemplate` bean suppresses Boot's default
entirely), and five Spring Kafka behavior confirmations (the real DLT
topic suffix, the wrapped exception header, binary-encoded
partition/offset headers, the real non-blocking retry-topic naming, and
a full-circle validation of WP-13's own hand-modeled header convention).

## Verification

```bash
./gradlew test
```

4 automated tests, against `@EmbeddedKafka` (a real, in-process broker) --
see "Automated tests" below.

## Experiment

### Experiment 1 — manual acknowledgment

```bash
./gradlew test --tests "*ManualAckListenerTest*"
```

Real committed-offset evidence via a direct `Admin.listConsumerGroupOffsets`
call: conceptual doc, Section 3.

### Experiment 2 — error handling and the dead-letter topic

```bash
./gradlew test --tests "*ErrorHandlingListenerTest*"
```

Real DLT topic naming, real (wrapped) exception headers, real
binary-encoded offset/partition headers: conceptual doc, Section 6.

### Experiment 3 — non-blocking retry topics

```bash
./gradlew test --tests "*RetryableTopicListenerTest*"
```

Real evidence of traversal through separate retry topics, contrasted
against WP-13's blocking approach: conceptual doc, Section 8.

### Experiment 4 — transactions

```bash
./gradlew test --tests "*TransactionalOrderPublisherTest*"
```

Conceptual doc, Section 9.

## Failure injection

No dedicated failure-matrix row names this WP. `@KafkaListener`'s
behavior under a rebalance mid-batch (a real question this WP's own
reference material raises) is addressed analytically in the conceptual
doc, Section 11, rather than with a dedicated automated test.

## Troubleshooting

### `NoSuchBeanDefinitionException` for `KafkaTemplate` at startup

A real Spring Boot 4.x finding -- see the conceptual doc, Section 2:
`org.springframework.boot:spring-boot-kafka` is a separate, required
dependency, not bundled into `spring-kafka` or the general
autoconfigure jar.

### `NoSuchBeanDefinitionException` for `KafkaTemplate` after adding your own custom one

A real finding -- see the conceptual doc, Section 4: defining ANY
custom `KafkaTemplate` bean suppresses Spring Boot's own autoconfigured
default entirely (matches by raw type, ignoring generics).

### `IllegalStateException: No transaction is in process`

A real finding -- see the conceptual doc, Section 5: don't set
`spring.kafka.producer.transaction-id-prefix` globally unless
essentially all of the app's Kafka traffic needs transactions; build a
separate, dedicated transactional template instead.

## Cleanup

```bash
cd platform/kafka-cluster && docker compose down
```

## Automated tests

`./gradlew test` -- 4 tests, against `@EmbeddedKafka`:

1. `ManualAckListenerTest#unacknowledgedRecordsOffsetIsNeverCommittedWhileAcknowledgedOnesIs`
2. `ErrorHandlingListenerTest#aPermanentlyFailingRecordIsRetriedThenRoutedToTheDeadLetterTopicWithRealHeaders`
3. `RetryableTopicListenerTest#anAlwaysFailingRecordTraversesEveryRetryTopicBeforeReachingTheDltHandler`
4. `TransactionalOrderPublisherTest#aRolledBackSendIsNeverVisibleUnderReadCommittedWhileACommittedOneIs`

Every timing-sensitive assertion uses a bounded condition-polling loop,
per this repository's established test convention. A benign,
Windows-specific `FileSystemException` appears in every test's log
during `@EmbeddedKafka` shutdown cleanup (see the conceptual doc,
Section 10) -- confirmed to never affect a test's pass/fail outcome.

## Production considerations

See the conceptual doc's Section 11 for what this lab deliberately does
NOT build (rebalance-mid-batch automation, Micrometer/Actuator
observability, batch-mode listeners) and why each is a reasonable scope
boundary.

## Principal Engineer questions

See the conceptual doc's Section 12 for all 7 questions with detailed,
experimentally-grounded answers.
