# Reference Repositories

This repository is not a copy of any tutorial or training repository. The projects
listed below are used as **authoritative references** — to validate architecture,
terminology, API usage, and operational practice — not as sources to copy from.
Where a lab in this repository is inspired by a pattern from one of these projects,
the lab's `README.md` says so explicitly and explains the pattern in its own words.

If you are ever unsure whether this repository is using a Kafka term or API
correctly, these are the sources to check against, in roughly this priority order.

## Primary source of truth

### [apache/kafka](https://github.com/apache/kafka)
The Kafka project itself. This is the ground truth for:
- Protocol behavior, configuration semantics, and default values.
- Terminology (this repository always prefers official Kafka terminology over
  vendor-specific or informal terms).
- Internal implementation details referenced in
  `docs/principal-engineer/KAFKA_SOURCE_CODE_READING_GUIDE.md` (added in a later
  work package).

When a doc in this repository states how a mechanism works internally (for
example, how the `RecordAccumulator` batches records, or how the group
coordinator manages a rebalance), it should be checked against the corresponding
package in this repository, not against secondary explanations of it.

## Client and framework references

### [spring-projects/spring-kafka](https://github.com/spring-projects/spring-kafka)
Reference for the production Spring Kafka material in `docs/spring-kafka/` and
`labs/lab-17-spring-kafka/`. Used to validate listener container configuration,
error-handling and retry-topic behavior, and testing patterns. This repository's
rule is: Spring Kafka is always explained in terms of the native Kafka mechanism
it wraps (see `docs/architecture/KAFKA_MENTAL_MODEL.md`) — this project is the
reference for getting that mapping right.

### [testcontainers/testcontainers-java](https://github.com/testcontainers/testcontainers-java)
Reference for how labs and services in this repository run Kafka, PostgreSQL, and
Schema Registry inside integration tests reproducibly, without requiring a
pre-existing shared environment. Used wherever a lab includes JUnit integration
tests against a real (containerized) broker instead of mocks.

## CDC and connectors

### [debezium/debezium](https://github.com/debezium/debezium)
Reference for `labs/lab-15-debezium-cdc` and the CDC material in
`docs/kafka-connect/`. Used to validate connector configuration, WAL-based
capture behavior for PostgreSQL, and the relationship between Debezium and Kafka
Connect's distributed-mode worker model.

## Applied examples (validation only)

### [confluentinc/examples](https://github.com/confluentinc/examples)
### [confluentinc/training-developer-src](https://github.com/confluentinc/training-developer-src)
Used to cross-check that this repository's labs reflect real-world,
production-oriented usage patterns rather than idiosyncratic choices — for
example, how a partition-key experiment or a Streams topology is typically
structured. Code in these repositories is **never copied**; where an idea is
adapted, the corresponding lab explains the underlying concept independently.

## Operational tooling (optional, situational)

### [provectus/kafka-ui](https://github.com/provectus/kafka-ui)
Referenced where a lab benefits from a visual cluster browser (topics,
consumer groups, partition state) in addition to the CLI tools taught in
`labs/lab-01-first-kafka-cluster`. The CLI is taught first and remains the primary
tool throughout this repository, because production troubleshooting frequently
happens without a UI available (see `docs/troubleshooting/`).

## How this list is used going forward

As new areas of the curriculum are implemented (Kafka Streams, security, CI),
this document will be extended with any additional authoritative references used
for that work package. Every addition follows the same rule: the reference
validates the work, it does not replace understanding the work.
