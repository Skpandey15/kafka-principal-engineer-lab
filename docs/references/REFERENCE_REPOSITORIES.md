# Reference Repositories — Architecture & Source-Reading Matrix

This repository is not a copy of any tutorial or training repository. The projects
below are used as **authoritative references** — to validate architecture,
terminology, API usage, and operational practice, and as source-reading targets
once a lab has shown you the *behavior* and you want to see where in real source
code that behavior actually lives. Where a lab in this repository is inspired by
a pattern from one of these projects, the lab's `README.md` says so explicitly
and explains the pattern in its own words. Code is never copied wholesale.

Every entry was checked against the live repository (existence, archive status,
last-push recency, and — where it matters — how ZooKeeper- vs. KRaft-oriented its
examples currently are) while writing this document. Where a project has moved,
been superseded, or gone stale, that is stated explicitly rather than silently
linking to the old location.

## Apache Kafka vs. the ecosystem around it — read this before the matrix

Several projects below are commonly (and incorrectly) treated as "part of
Kafka." They are not. Confusing an ecosystem convenience with a core Kafka
guarantee is a real, recurring production mistake, so this repository draws the
line explicitly:

| Layer | What it actually is | Examples below |
|---|---|---|
| **Apache Kafka itself** | The broker, the protocol, the Java client, KRaft, Kafka Connect's framework, Kafka Streams. Anything documented in `apache/kafka` and covered by Kafka's own compatibility guarantees. | `apache/kafka` |
| **Vendor ecosystem (Confluent)** | Confluent-specific tooling and packaging built *around* Kafka — Schema Registry, ksqlDB, Confluent Control Center, Confluent Cloud, and Confluent's own example/training repositories. Genuinely useful, but its guarantees, licensing, and roadmap are Confluent's, not the Apache Kafka project's. | `confluentinc/training-developer-src`, `confluentinc/examples`, `confluentinc/kafka-streams-examples` |
| **Independent / vendor-neutral community projects** | Projects that operate *on* Kafka but are governed independently of any single vendor — some under a foundation, some as community forks of an abandoned vendor project. | `debezium/debezium` (Commonhaus Foundation since December 2024, Red Hat still a contributor), `strimzi/strimzi-kafka-operator`, `cruise-control-for-kafka/cruise-control` (community-governed continuation of LinkedIn's original project), `testcontainers/testcontainers-java`, `kafbat/kafka-ui` (community fork), `spring-projects/spring-kafka` |

When a doc in this repository says "Kafka guarantees X," it means Apache Kafka
itself. When it uses a Confluent-specific concept (Schema Registry's
compatibility modes, for instance), it says "Confluent Schema Registry" or
"the Confluent ecosystem," not "Kafka."

## Priority overview

| Repository | Role in this curriculum | Authority | KRaft relevance |
|---|---|---|---|
| `apache/kafka` | Primary implementation truth | Apache Software Foundation | Current (KRaft-only since 4.0) |
| `spring-projects/spring-kafka` | Framework reference, used after native client mastery | Official Spring project | Current (delegates to the Kafka client) |
| `testcontainers/testcontainers-java` | First-class testing strategy from WP-03 onward | Official, widely adopted | Current |
| `debezium/debezium` | CDC / Kafka Connect reference | Commonhaus Foundation (vendor-neutral) | Current |
| `debezium/debezium-examples` | Worked CDC pipeline examples | Same project, examples repo | Mixed — see entry |
| `confluentinc/training-developer-src` | Structured developer-exercise reference | Confluent (vendor) | Current |
| `confluentinc/examples` | Broad applied-pattern cross-check | Confluent (vendor) | Mixed — see entry |
| `confluentinc/kafka-streams-examples` | Kafka Streams topology patterns | Confluent (vendor) | Legacy-heavy — see entry |
| `lydtechconsulting/kafka-idempotent-consumer` | Idempotent-consumer + outbox pattern reference | Community, unmaintained since Oct 2023 | Not relevant to the pattern itself |
| `kafbat/kafka-ui` | Operational UI (secondary to the CLI) | Community (active fork) | Current |
| `provectus/kafka-ui` | **Superseded — see "Rejected / de-emphasized"** | Vendor, abandoned Sept 2023 | N/A |
| `strimzi/strimzi-kafka-operator` | Future Kubernetes-operations reference | CNCF-adjacent community project | Current |
| `cruise-control-for-kafka/cruise-control` | Future cluster-balancing reference | Community (formerly LinkedIn-hosted) | Current |
| `pedrovgs/KafkaPlayground` | Supplementary, lowest-priority learning repo | Individual community maintainer | Unverified per-example — see entry |

Full field-by-field detail for each repository follows, grouped the way this
curriculum will actually reach for them.

---

## Primary source of truth

### `apache/kafka`

| Field | Detail |
|---|---|
| Repository | [apache/kafka](https://github.com/apache/kafka) |
| Authority | Apache Software Foundation — the project itself |
| Why we reference it | Ground truth for protocol behavior, configuration semantics, default values, and terminology. This repository always prefers official Kafka terminology over vendor-specific or informal terms, and this is the source that terminology is checked against. |
| Concepts learned | Producer internals (`RecordAccumulator`, `Sender`, `NetworkClient`), consumer internals (poll loop, group coordinator), KRaft (controller quorum, metadata log), replication (ISR, high watermark, leader election), storage (segments, indexes, compaction), the wire protocol, transactions and idempotence. |
| Target WP(s) | All of them, in some form — this is the repository every other reference is checked against. Most directly: WP-03 (Java client), WP-05 (consumer groups), WP-07 through WP-09 (replication, KRaft, transactions), and the source-reading track (folded into WP-20, the Principal Engineer capstone). |
| Source-reading target | `clients/src/main/java/org/apache/kafka/clients/producer/internals/` (`RecordAccumulator`, `Sender`), `.../clients/consumer/internals/` (fetcher, coordinator), `core/src/main/scala/kafka/server/` and `metadata/` for KRaft/controller logic, `storage/` for the log/segment implementation. Package layout changes between releases — always confirm the current layout against the tag you're reading, not against this list. |
| What NOT to copy | Nothing to avoid here — this is the primary source. The only caution is version drift: read against the tag matching the version pinned in `platform/kafka/docker-compose.yml`, not against `trunk`, unless you deliberately want to see upcoming changes. |
| KRaft relevance | Current. Kafka 4.0+ (which this repository targets) removed ZooKeeper support entirely; `apache/kafka` at the pinned tag is definitionally KRaft-only. |
| Principal Engineer value | Every claim this repository makes about Kafka's internal behavior should be traceable back to here. A Principal Engineer who can go from "I observed X in a lab" to "here is the exact source location responsible for X" has a categorically different depth of understanding than one who can only cite documentation. |

## Client and framework references

### `spring-projects/spring-kafka`

| Field | Detail |
|---|---|
| Repository | [spring-projects/spring-kafka](https://github.com/spring-projects/spring-kafka) |
| Authority | Official Spring project (VMware/Broadcom-sponsored, community-governed) |
| Why we reference it | Reference for the production Spring Kafka material in `docs/spring-kafka/` and `labs/lab-18-spring-kafka/` — listener container configuration, acknowledgment modes, error handling, retry topics, dead-letter publishing, transactions, and test support (`@EmbeddedKafka`, `KafkaTestUtils`). |
| Concepts learned | `KafkaTemplate`, `@KafkaListener` and listener container concurrency, container-level acknowledgment modes, `DefaultErrorHandler` and retry/backoff configuration, non-blocking retry topics and dead-letter routing, `KafkaTransactionManager`, Spring Boot auto-configuration of producer/consumer factories. |
| Target WP(s) | WP-15 (`lab-18-spring-kafka`), and cross-referenced from WP-13 (retry/DLQ) since Spring Kafka's non-blocking retry topics are a framework-level implementation of the same pattern taught natively first. |
| Source-reading target | `spring-kafka/src/main/java/org/springframework/kafka/listener/` (listener containers, error handlers), `.../core/KafkaTemplate.java`. |
| What NOT to copy | Do not let Spring Kafka's abstractions substitute for understanding the native `KafkaProducer`/`KafkaConsumer` behavior underneath — this repository's rule (`CONTRIBUTING.md` §18) is that every Spring Kafka feature introduced is explained in terms of the native Kafka mechanism it wraps. WP-15 is deliberately sequenced *after* WP-03, never before. |
| KRaft relevance | Current — Spring Kafka delegates all broker interaction to the Kafka Java client, so it has no ZooKeeper/KRaft-specific code path of its own to go stale. |
| Principal Engineer value | Most production Java Kafka code a Principal Engineer will review is Spring Kafka, not the raw client. Knowing precisely which native guarantee each Spring abstraction is and is not preserving (does `@KafkaListener`'s default ack mode match what you assume it does under a rebalance mid-batch?) is a common source of subtle production bugs this repository wants you able to reason about, not just recognize by name. |

## Testing strategy — first-class from WP-03 onward

### `testcontainers/testcontainers-java`

| Field | Detail |
|---|---|
| Repository | [testcontainers/testcontainers-java](https://github.com/testcontainers/testcontainers-java) |
| Authority | Official Testcontainers project, widely adopted across the JVM ecosystem |
| Why we reference it | This repository's testing philosophy, starting at WP-03, is built around it: tests run against a real, disposable, containerized Kafka broker, not a mocked one. |
| Concepts learned | The Kafka module's container lifecycle, bootstrapping test topics/consumer groups against a throwaway broker, and — for later WPs — the PostgreSQL and Schema Registry modules used alongside Debezium and schema-evolution labs. |
| Target WP(s) | WP-03 onward, as the default integration-test approach for every lab that includes automated tests. |
| Source-reading target | Not typically necessary — this is an API-consumption reference, not a source-reading target the way `apache/kafka` is. |
| What NOT to copy | Do not build this curriculum's test suite primarily around mocked `KafkaProducer`/`KafkaConsumer` instances. A mock can tell you your code calls the client API correctly; it cannot tell you whether your understanding of Kafka's actual behavior (batching, rebalancing, offset commit timing) is correct — and this repository's entire premise is that the second thing is what matters. |
| KRaft relevance | Current — the Testcontainers Kafka module runs the same KRaft-mode broker image this repository already uses. |
| Principal Engineer value | Knowing when to reach for Testcontainers versus Docker Compose is itself a judgment call worth having an opinion on — see the testing progression below. |

**Testing progression this repository follows, in order:**

```text
Pure unit tests
   ↓
Kafka-client unit-level tests, where a test's whole point is serialization/
partitioning logic that doesn't need a broker at all
   ↓
Real Kafka integration tests (Testcontainers), for anything that exercises
actual produce/consume/commit/rebalance behavior
   ↓
Testcontainers Kafka, specifically, once multiple test classes need
independent, isolated brokers per test run
   ↓
Failure/integration scenarios (killing a container mid-test, network
partitions) still inside Testcontainers, where the scope is one component
   ↓
Docker Compose multi-broker experiments, once the scenario is about the
*cluster* itself (replication, controller-quorum failure) rather than one
application's behavior against a cluster
   ↓
Production-like failure engineering, in the dedicated failure-injection labs
(WP-19, and the broker/controller-failure labs), which intentionally run
outside the automated test suite because their point is manual observation
```

**Testcontainers vs. Docker Compose, concretely:** Testcontainers belongs
*inside* a test suite — JUnit owns the container's lifecycle, and the
scenario under test is your application code's behavior against Kafka.
Docker Compose (as in `platform/kafka/`) belongs *outside* the test
suite — you own the lifecycle, and the scenario is Kafka's own behavior
(a broker dying, a controller quorum losing a voter) rather than your
application's. Reaching for Testcontainers to simulate a multi-broker
cluster failure, or reaching for a hand-run Compose cluster inside a CI
test job, are both signs of using the wrong tool for the question being
asked.

## CDC and connectors

### `debezium/debezium`

| Field | Detail |
|---|---|
| Repository | [debezium/debezium](https://github.com/debezium/debezium) |
| Authority | Commonhaus Foundation (vendor-neutral governance since December 2024; Red Hat remains an active contributor, but the project is no longer solely Red-Hat-governed) |
| Why we reference it | Reference for `labs/lab-15-debezium-cdc` and the CDC material in `docs/kafka-connect/` — connector configuration, WAL-based logical decoding for PostgreSQL, and how Debezium's connectors run inside Kafka Connect's distributed-mode worker model. |
| Concepts learned | Source connector configuration, PostgreSQL logical decoding / WAL concepts, Debezium's event envelope format, snapshotting vs. streaming, and connector-level failure/restart/recovery behavior. |
| Target WP(s) | WP-11 (`lab-15-debezium-cdc`), and foundational to WP-12's transactional outbox (which is CDC-based in this repository's design) and the future idempotent-consumer topic that follows it. |
| Source-reading target | Less about reading Java internals and more about reading connector configuration reference docs and the PostgreSQL connector's own documentation of what it captures and how. |
| What NOT to copy | Do not treat CDC as a substitute for understanding Kafka Connect's own worker/task/offset model first — within WP-11 (Kafka Connect & CDC), plain Kafka Connect fundamentals are sequenced before Debezium-specific CDC for that reason, as two phases of the same work package rather than two separate ones. |
| KRaft relevance | Current — Debezium is a Kafka Connect connector; it has no ZooKeeper dependency of its own and runs against any KRaft cluster. |
| Principal Engineer value | CDC is frequently the actual answer to "how do we get reliable events out of a system of record without a dual-write problem" — see the transactional outbox topic. Understanding what Debezium *cannot* guarantee (ordering across tables, exactly what "commit" means for an in-flight transaction at snapshot time) matters as much as what it can. |

### `debezium/debezium-examples`

| Field | Detail |
|---|---|
| Repository | [debezium/debezium-examples](https://github.com/debezium/debezium-examples) |
| Authority | Same project as above; the dedicated worked-examples repository |
| Why we reference it | Concrete, runnable pipeline examples (source database → Debezium → Kafka → consumer) to cross-check this repository's own CDC lab structure against. |
| Concepts learned | End-to-end pipeline wiring, Docker Compose patterns for a Connect worker plus a source database, and — in its outbox-pattern-specific example directories — a worked transactional outbox implementation. |
| Target WP(s) | WP-11 and WP-12. |
| Source-reading target | The `outbox` example directory specifically is worth reading end to end once you reach WP-12. |
| What NOT to copy | **This repository mixes ZooKeeper-era and KRaft-era example directories** — a repository-wide code search at the time of writing found real ZooKeeper references alongside newer KRaft ones. Check which specific example subdirectory you're looking at before treating its Docker Compose file as a KRaft reference; do not let an older subdirectory's ZooKeeper setup influence this project's KRaft-first architecture. |
| KRaft relevance | Mixed — verify per example directory, not per repository. |
| Principal Engineer value | Seeing a full worked outbox example, including its rough edges, is more useful before designing your own than reading the pattern's description in isolation. |

## Confluent ecosystem (vendor-specific — see the boundary note above)

### `confluentinc/training-developer-src`

| Field | Detail |
|---|---|
| Repository | [confluentinc/training-developer-src](https://github.com/confluentinc/training-developer-src) |
| Authority | Confluent (vendor); source code accompanying Confluent's paid "Kafka for Developers" course |
| Why we reference it | Structured, exercise-shaped reference for how a vendor with deep Kafka expertise sequences developer-level exercises — useful as a cross-check on this repository's own exercise sequencing, not as course material this repository depends on. |
| Concepts learned | Developer-level exercise patterns for producers, consumers, and Confluent-specific developer tooling. |
| Target WP(s) | WP-03 through WP-06, as a sequencing cross-check only. |
| Source-reading target | Not a source-reading target — an exercise-structure reference. |
| What NOT to copy | Any Confluent-Cloud-specific or Confluent-CLI-specific exercise content; this repository targets self-hosted, open-source Apache Kafka via Docker Compose, not Confluent Cloud. |
| KRaft relevance | Current — a repository-wide check found no ZooKeeper references and explicit KRaft references, consistent with recently-updated course material. |
| Principal Engineer value | Limited on its own; useful mainly as evidence of how a training organization sequences the same fundamentals this repository teaches. |

### `confluentinc/examples`

| Field | Detail |
|---|---|
| Repository | [confluentinc/examples](https://github.com/confluentinc/examples) |
| Authority | Confluent (vendor) |
| Why we reference it | Used to cross-check that this repository's labs reflect real-world, production-oriented usage patterns — for example, how a partition-key experiment or a Streams topology is typically structured in practice. |
| Concepts learned | Applied patterns across Kafka, Kafka Streams, and now Flink and broader Confluent Platform examples — the repository's scope has grown well beyond "Kafka examples." |
| Target WP(s) | WP-04 (partitioning), WP-14 (Streams), as a pattern cross-check only. |
| Source-reading target | Not a source-reading target. |
| What NOT to copy | **This repository contains substantial ZooKeeper-era example content** (a repository-wide check found roughly twenty times as many ZooKeeper references as KRaft references at the time of writing) and a growing amount of Confluent-Platform- and Flink-specific material that is out of this curriculum's scope entirely. Treat every example you pull an idea from as something to individually verify against current KRaft configuration — do not assume any given subdirectory is current just because the repository as a whole is actively maintained. |
| KRaft relevance | Mixed, and currently ZooKeeper-heavy — verify per example. |
| Principal Engineer value | Useful for pattern-spotting across a large surface area of real applied Kafka usage; not reliable as an architecture reference without independent verification of currency. |

### `confluentinc/kafka-streams-examples`

| Field | Detail |
|---|---|
| Repository | [confluentinc/kafka-streams-examples](https://github.com/confluentinc/kafka-streams-examples) |
| Authority | Confluent (vendor) |
| Why we reference it | Demo applications and code examples specifically for the Kafka Streams API — `KStream`/`KTable`/`GlobalKTable` usage, aggregation, joins, windowing, state stores, and `TopologyTestDriver`-based testing patterns. |
| Concepts learned | Streams DSL topology patterns, interactive queries, exactly-once Streams configuration, and `TopologyTestDriver` test structure. |
| Target WP(s) | WP-14 (`lab-17-kafka-streams`), pattern reference only. |
| Source-reading target | The `TopologyTestDriver`-based test classes are worth reading as a *testing pattern* reference even where the surrounding application example is dated. |
| What NOT to copy | **This repository is heavily ZooKeeper-era** (a repository-wide check found roughly twenty-six ZooKeeper references against a single KRaft reference at the time of writing) — its infrastructure/deployment examples predate KRaft entirely. Extract the *Streams topology and testing patterns* only; do not use its cluster setup as a reference for anything. |
| KRaft relevance | Legacy-heavy. Do not let this repository's ZooKeeper-oriented setup examples influence this project's KRaft-first architecture — see `CONTRIBUTING.md` engineering rule #3. |
| Principal Engineer value | The topology and state-store patterns remain conceptually sound even though the deployment scaffolding around them is dated; a Principal Engineer should be able to separate "this pattern is still correct" from "this infrastructure example is current," which is exactly the exercise this entry is included to force. |

## Reliability and idempotency patterns

### `lydtechconsulting/kafka-idempotent-consumer`

| Field | Detail |
|---|---|
| Repository | [lydtechconsulting/kafka-idempotent-consumer](https://github.com/lydtechconsulting/kafka-idempotent-consumer) |
| Authority | Community (a consulting firm's public demo repository) |
| Why we reference it | A focused, small Spring Boot application that demonstrates the idempotent-consumer pattern *together with* the transactional outbox pattern via Debezium — a concrete illustration of the reliability chain this curriculum builds toward (Connect → CDC → Debezium → Outbox → reliable publication → idempotent consumer). |
| Concepts learned | The specific failure shape this pattern defends against: consume → perform a business/DB side effect → crash before the offset commits → Kafka redelivers → the side effect executes again unless the consumer is idempotent. Also: idempotency keys, a processed-event table, and unique-constraint-based deduplication as concrete implementation techniques. |
| Target WP(s) | The future idempotent-consumer curriculum topic (conceptually anchored after WP-12, hands-on lab delivered in WP-13 alongside retry/DLQ — see the curriculum map). |
| Source-reading target | Its consumer and deduplication-table handling code, as a pattern reference — small enough to read in full. |
| What NOT to copy | **This repository has had no commits since October 2023** — treat it purely as a pattern illustration, not as a source of current dependency versions, current Spring Boot/Spring Kafka idioms, or current Debezium configuration syntax. Re-verify every version-specific detail against the current `spring-projects/spring-kafka` and `debezium/debezium` documentation before using it as a template. |
| KRaft relevance | Not applicable to the pattern itself — the idempotent-consumer pattern is orthogonal to whether the cluster underneath runs KRaft or ZooKeeper. Any ZooKeeper references found in its Docker Compose setup are a deployment-scaffolding detail, not part of the pattern being taught. |
| Principal Engineer value | This is exactly the failure this repository refuses to let learners hand-wave: **Kafka's own exactly-once semantics do not automatically make an arbitrary external business side effect exactly-once.** The idempotency boundary — where in your system does "processing this event twice is safe" actually become true? — is a Principal Engineer-level design question this pattern makes concrete instead of abstract. |

## Operational tooling

### `kafbat/kafka-ui`

| Field | Detail |
|---|---|
| Repository | [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) |
| Authority | Community (an active continuation of the original maintainers' work, after the original vendor paused development — see the rejected entry below) |
| Why we reference it | A visual cluster browser (topics, consumer groups, partition state, broker metrics) as a secondary, optional aid once CLI fundamentals are solid. |
| Concepts learned | Nothing Kafka-conceptual that the CLI doesn't already teach — its value is purely in visualizing state you already know how to query by hand. |
| Target WP(s) | Optional, from WP-02 onward, always positioned *after* the CLI-based lab it accompanies, never before. |
| Source-reading target | Not a source-reading target. |
| What NOT to copy | Do not let a UI become how a learner first encounters cluster state. This repository's rule: CLI first, always — a UI that hides *how* it knows what it's showing you produces engineers who can't troubleshoot when the UI itself is unavailable, which is precisely when troubleshooting skill matters most. |
| KRaft relevance | Current — actively maintained against current Kafka versions as of this writing. |
| Principal Engineer value | Low on its own; its real value is operational convenience once fundamentals are already solid, and as a live example of what a "good enough" cluster dashboard surfaces (which is a useful input when this curriculum later reaches `docs/observability/` and has to decide what a Grafana dashboard should show that a generic Kafka UI does not). |

**Learning sequence this repository holds to:** CLI → understand raw Kafka
state yourself → optionally, a UI like `kafbat/kafka-ui` for convenience →
metrics → dashboards and alerts. Never let a UI substitute for the earlier
steps.

## Kubernetes and cluster operations (future reference — not implemented yet)

### `strimzi/strimzi-kafka-operator`

| Field | Detail |
|---|---|
| Repository | [strimzi/strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator) |
| Authority | CNCF-adjacent community project (originated at Red Hat, now an independent community project with broad contribution) |
| Why we reference it | The canonical way most organizations run Kafka on Kubernetes; reserved for a future Kubernetes-operations module, explicitly **not** introduced into this repository's current implementation. |
| Concepts learned (future) | Running KRaft-mode Kafka on Kubernetes, the `KafkaNodePool` resource (the current architecture for defining broker/controller node roles and scaling, superseding the older single monolithic `Kafka` CR node-count model), rolling updates, persistent-volume-backed storage, certificate and user management via CRDs, and operator reconciliation behavior. |
| Target WP(s) | A future Kubernetes/operations work package, explicitly out of scope for the current curriculum stage — see the Level 5 roadmap section. |
| Source-reading target | The operator's reconciliation loop for the `Kafka` and `KafkaNodePool` custom resources, once that future module is reached. |
| What NOT to copy | Do not introduce Kubernetes into any current-stage lab. This repository's Docker Compose environment (`platform/kafka/`) remains the primary local environment through the entire core curriculum; Strimzi enters only once a dedicated future module exists to teach it properly. |
| KRaft relevance | Current — Strimzi's current-generation architecture is built around KRaft-mode Kafka; verify the exact CRD shape (`KafkaNodePool` vs. any predecessor) against whatever Strimzi version that future module ends up pinning, since this has been an area of active change. |
| Principal Engineer value | The eventual question this module exists to make concrete: *just because Kafka can run on Kubernetes, should this organization run Kafka on Kubernetes?* — a trade-off question, not a default. |

### `cruise-control-for-kafka/cruise-control`

| Field | Detail |
|---|---|
| Repository | [cruise-control-for-kafka/cruise-control](https://github.com/cruise-control-for-kafka/cruise-control) |
| Authority | Community-governed continuation of a project originally created and open-sourced by LinkedIn; the project has since moved to its own dedicated organization. The historical `linkedin/cruise-control` URL still exists but the actively maintained repository is at this new location — link to the new one. |
| Why we reference it | Reserved for a future cluster-rebalancing module. Automates the two related but distinct problems of *consumer-group* rebalancing (already covered in WP-05) and *cluster data/replica* rebalancing (broker resource balance, replica placement, rack awareness) — this repository will draw that distinction explicitly when the module is built. |
| Concepts learned (future) | Broker resource balance (CPU, disk, network), replica placement and leader distribution, rack-aware placement, and "optimization goals" as Cruise Control's mechanism for expressing rebalancing intent. |
| Target WP(s) | A future cluster-balancing work package — see the Level 5 roadmap section. Not implemented in the current curriculum. |
| Source-reading target | Deferred to that future module. |
| What NOT to copy | Do not treat "add a broker" as self-evidently solving a capacity or balance problem — the entire point of this future module is that it usually does not, on its own; see the Principal Engineer scenario in the roadmap's cluster-rebalancing topic. |
| KRaft relevance | Current — actively maintained with ongoing Kafka-version-compatibility work as of this writing. |
| Principal Engineer value | "We added brokers and the cluster didn't get faster" is a real, recurring production surprise. Cruise Control (and understanding *why* it's needed, not just *that* it exists) is squarely Principal-level operational judgment. |

## Supplementary community learning repositories

### `pedrovgs/KafkaPlayground`

| Field | Detail |
|---|---|
| Repository | [pedrovgs/KafkaPlayground](https://github.com/pedrovgs/KafkaPlayground) |
| Authority | Individual community maintainer, not an organization or vendor |
| Why we reference it | A small, actively-touched (as of early 2026) personal playground repository for experimenting with Kafka — included only as a supplementary pointer for learners who want to see another individual engineer's exploratory Kafka code, not as an authoritative pattern source. |
| Concepts learned | Varies by example; not independently catalogued here. |
| Target WP(s) | None assigned — explicitly lowest-priority and optional. |
| Source-reading target | Not a source-reading target. |
| What NOT to copy | Anything ZooKeeper-oriented it may contain. This repository's rule stands regardless of source: **older, ZooKeeper-oriented examples anywhere must never be allowed to influence this project's KRaft-first architecture** (`CONTRIBUTING.md` engineering rule #3). Verify the currency of any specific example here before drawing on it, the same as any other community repository. |
| KRaft relevance | Not verified in bulk for this document — treat as unverified per example, and confirm before use. |
| Principal Engineer value | Minimal on its own; included for completeness because it was named as a candidate, not because it is load-bearing for this curriculum. |

## Rejected / de-emphasized

### `provectus/kafka-ui` — superseded, do not use for new references

Provectus paused active development of `kafka-ui` in September 2023; the
project went without meaningful maintenance from that point (notably, a
remote-code-execution vulnerability, CVE-2023-52251, went unpatched for
roughly six months). The original maintainers' work continues as an active
community fork at **`kafbat/kafka-ui`** (see above), which is what this
repository references instead. Any documentation, blog post, or older
reference that still points at `provectus/kafka-ui` or the
`provectuslabs/kafka-ui` Docker image should be treated as pointing at
abandoned, security-relevant software — this is exactly the kind of "verify
before recommending" check `CONTRIBUTING.md` and this repository's memory
practices both call for, and it is why this document names the rejection
explicitly instead of silently swapping the link.

No other candidate repository evaluated for this document was found to be
archived, deleted, or renamed in a way that would make it unsafe to
reference; where a repository has real currency caveats short of outright
rejection (mixed ZooKeeper/KRaft content, no recent commits), that is
recorded in its own entry above rather than here.

## How this list is used going forward

As new areas of the curriculum are implemented, this document is extended
with any additional authoritative references used for that work package,
using the same field structure as above — never a bare link with a vague
one-line justification. Before adding a new entry, its live repository state
(existence, archive status, recency, KRaft alignment) is checked the same way
the entries above were checked, and before trusting an *existing* entry's
claims months or years later, re-verify rather than assuming the state
recorded here is still current — vendor projects get paused, forks become the
active project, and "current" example code ages into "legacy" example code
exactly the way `confluentinc/kafka-streams-examples` and
`provectus/kafka-ui` already have.
