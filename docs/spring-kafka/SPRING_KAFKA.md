# Spring Kafka (WP-15)

## Prerequisites

- WP-03 (native producer/consumer) -- per `CONTRIBUTING.md` §18 ("Do not
  hide Kafka behavior behind frameworks... the underlying Kafka
  mechanism it wraps is explained alongside it"), this WP is
  deliberately sequenced after native client mastery, never before.
- WP-06 (offset management), WP-09 (transactions), WP-13 (retry/DLQ) --
  every Spring abstraction this WP covers maps directly onto a native
  mechanism one of those WPs already built by hand.

## 1. Why this is the first Spring Boot lab in the repository

Every prior lab is plain Java with a hand-written `main` method and no
dependency-injection framework. This WP introduces Spring Boot 4.1.1
specifically because Spring Kafka's own idioms (`@KafkaListener`,
`KafkaTemplate`, `@Transactional`) are built around Spring's DI
container -- using them without Spring Boot would mean hand-wiring the
exact same `ApplicationContext` machinery Spring Boot already automates,
defeating the point of studying "how a production framework maps onto
the primitives you already know."

## 2. A real Spring Boot 4.x finding: autoconfiguration moved to its own artifact

A first version of this lab's `build.gradle` added only
`org.springframework.kafka:spring-kafka` and hit a real
`NoSuchBeanDefinitionException` for `KafkaTemplate` at context startup --
Spring Boot's `KafkaAutoConfiguration` (the class that turns
`spring.kafka.*` properties into `KafkaTemplate`/`ConsumerFactory`/
`ProducerFactory`/listener-container-factory beans) was nowhere in the
autoconfiguration candidate list at all. The real cause, confirmed on
Maven Central: starting in Spring Boot 4.0, per-technology
autoconfiguration was split OUT of the old monolithic
`spring-boot-autoconfigure` jar into dedicated artifacts --
`org.springframework.boot:spring-boot-kafka` is a real, separate
dependency now required alongside `spring-kafka` itself.

## 3. Listener containers and the native mechanism they wrap

`@KafkaListener` methods run inside a `ConcurrentMessageListenerContainer`
-- a real poll loop with real consumer-group membership, exactly what
WP-04/WP-05 already cover, just managed for you. Spring Kafka's actual
DEFAULT acknowledgment mode is `AckMode.BATCH` (commit once per poll
batch) -- the native equivalent of WP-06's own batch-commit boundary
concept, not something new. `manualAckContainerFactory` switches one
listener to `AckMode.MANUAL`, the native equivalent of WP-06's manual
`commitSync()` per record.

`unacknowledgedRecordsOffsetIsNeverCommittedWhileAcknowledgedOnesIs` is
real, verified evidence: two records are sent, the first acknowledged,
the second deliberately never acknowledged, and a real
`Admin.listConsumerGroupOffsets` call against the embedded broker
confirms the committed offset stops exactly at the acknowledged record --
the unacknowledged one's offset is never durably recorded, precisely
like calling `commitSync()` for some records and skipping it for
another.

## 4. A real finding: owning ONE custom `KafkaTemplate` bean means owning ALL of them

Building `TransactionalProducerConfig`'s dedicated transactional
template, a `NoSuchBeanDefinitionException` for `KafkaTemplate<Object,
Object>` appeared in an UNRELATED bean (`errorHandlingContainerFactory`).
Root cause: `spring-boot-kafka`'s `KafkaAutoConfiguration#kafkaTemplate(...)`
method is `@ConditionalOnMissingBean`, and that condition matches by
RAW type (`KafkaTemplate.class`), ignoring generic parameters entirely --
defining ANY custom `KafkaTemplate` bean anywhere in the app (even one
typed `<String, String>`, deliberately tried to dodge this) suppresses
Boot's own default bean completely. There is no partial opt-in: once you
define one custom `KafkaTemplate` bean, you're responsible for every
`KafkaTemplate` this app needs. The fix, `ListenerContainerFactoryConfig`'s
own explicit `kafkaTemplate` bean, is a direct consequence of this
finding, not an arbitrary design choice.

A related, narrower version of the same lesson on the CONSUMER side:
Spring's generics-aware autowiring means an `@Autowired
ConsumerFactory<String, String>` injection point is NOT satisfied by a
bean declared `ConsumerFactory<Object, Object>` (Boot's own default) --
both container factories in this lab build their own `ConsumerFactory`
explicitly rather than fight that mismatch.

## 5. A real finding: a global `transaction-id-prefix` breaks ordinary sends

A first version of this lab set `spring.kafka.producer.transaction-id-prefix`
directly in `application.properties`, expecting it to "turn on"
transactions conveniently. It did -- for EVERY send through the default
`KafkaTemplate`, including this lab's own plain test producer calls and
the `DeadLetterPublishingRecoverer`'s DLT publishing. Both failed with a
real `IllegalStateException: No transaction is in process`. A
transactional `KafkaTemplate` refuses any `send()` outside an active
transaction scope -- it does NOT implicitly wrap a standalone call in
its own local transaction the way an unguarded first assumption might
expect. The fix, `TransactionalProducerConfig`'s SEPARATE dedicated
producer factory/template/transaction manager, is also the more
realistic production shape: most of a real app's Kafka traffic doesn't
need transactional semantics, so making only the traffic that needs it
transactional (not the whole app) is the correct design, not just a
workaround.

## 6. Error handling and dead-letter publishing: the native mechanism, and two more real findings

`errorHandlingContainerFactory`'s `DefaultErrorHandler(recoverer,
FixedBackOff(200L, 2L))` wraps the EXACT pattern WP-13's
`RetryingRecordProcessor` built by hand: bounded retry, then a DLQ.
`aPermanentlyFailingRecordIsRetriedThenRoutedToTheDeadLetterTopicWithRealHeaders`
confirms 3 real attempts (1 + 2 retries) before recovery, and two real
findings surfaced verifying the DLT record itself:

1. **The default DLT topic suffix is `-dlt`, not `.DLT`** -- confirmed
   against the real constant inside `DeadLetterPublishingRecoverer.class`,
   not assumed from an older tutorial's convention.
2. **The listener's exception is WRAPPED.** `kafka_dlt-exception-fqcn`
   holds `org.springframework.kafka.listener.ListenerExecutionFailedException`
   (Spring's own wrapper), not the listener's raw `IllegalStateException`
   -- the original cause lives in a SEPARATE header,
   `kafka_dlt-exception-cause-fqcn`.
3. **`kafka_dlt-original-partition`/`-offset` are raw binary**
   (big-endian `int`/`long`), not UTF-8 text -- unlike WP-13's own
   hand-rolled `kafka_dlt-*` headers, which encoded these as plain
   strings. WP-13's header NAMING convention (confirmed correct, see
   Section 7 below) does not mean its header VALUE ENCODING matches --
   a real, worthwhile distinction this WP's test had to decode correctly
   (`ByteBuffer.wrap(bytes).getInt()`, not `new String(bytes)`).

## 7. Full-circle validation: WP-13's hand-rolled convention, confirmed real

WP-13's `RetryingRecordProcessor` modeled its own DLQ headers
(`kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`,
`kafka_dlt-original-topic`, `kafka_dlt-original-partition`,
`kafka_dlt-original-offset`) after "Spring Kafka's real
`DeadLetterPublishingRecoverer` convention," described but never
verified against the actual library at the time (WP-13 didn't depend on
spring-kafka at all). This WP is the first chance to check that claim
against the real thing: confirmed correct via a direct search of
`KafkaHeaders.class`'s own string constants -- every header NAME WP-13
chose is byte-for-byte the real Spring Kafka convention (Section 6
above notes the one real difference: VALUE encoding, not naming).

## 8. Non-blocking retry topics: the architectural trade-off against WP-13

`@RetryableTopic` is Spring Kafka's OWN answer to the same retry
problem WP-13 solved with in-process, same-partition
`Thread.sleep`-based retry. The mechanism is fundamentally different:
`@RetryableTopic` creates REAL, SEPARATE topics per retry attempt
(`retryable-in-retry-0`, `retryable-in-retry-1`, ... confirmed via
`retryTopicSuffix`'s real default, `-retry`), each with its OWN
dedicated consumer.
`anAlwaysFailingRecordTraversesEveryRetryTopicBeforeReachingTheDltHandler`
proves a permanently-failing record really does traverse all 3
attempts (the original topic plus 2 retry topics) before reaching
`@DltHandler`, with the original payload intact throughout.

| | WP-13's blocking retry | `@RetryableTopic` (non-blocking) |
|---|---|---|
| Where retries happen | Same partition, same poll loop, `Thread.sleep` between attempts | Separate topics, separate consumers |
| Effect on OTHER records on the same partition | Blocked until the retry window elapses | Unaffected -- keep flowing immediately |
| Per-key ordering across a retry | Preserved (never left the original partition) | NOT preserved (a retried record reprocesses on a different topic/partition than its original neighbors) |
| Operational footprint | None beyond the DLQ topic | One extra real topic per retry attempt, each independently monitorable/lag-observable |

Neither is strictly better -- it's a real trade-off between per-key
ordering and not blocking a partition's other traffic, worth choosing
deliberately per use case rather than defaulting to either.

## 9. Transactions: the native mechanism, declaratively

`TransactionalOrderPublisher`'s `@Transactional` method wraps the exact
begin/commit/abort sequence WP-09 built by hand around a
`transactional.id`-configured producer.
`aRolledBackSendIsNeverVisibleUnderReadCommittedWhileACommittedOneIs`
confirms the same guarantee WP-09 already proved natively: a
`read_committed` consumer never sees the rolled-back send, and does see
the committed one -- Spring's `@Transactional` doesn't change WHAT
Kafka guarantees, only how much hand-written ceremony reaching that
guarantee requires.

## 10. A benign, environment-specific finding: Windows file-locking on embedded-broker shutdown

Every test in this suite logs a real
`FileSystemException: ... The process cannot access the file because it
is being used by another process` during `@EmbeddedKafka`'s shutdown
cleanup, on this Windows development environment -- a real OS-level
file-locking difference (Windows vs. Unix) during the embedded broker's
own temp-directory teardown. It never affects a single test's PASS/FAIL
outcome (confirmed across every run in this lab) -- noted here as a
real, observed environment quirk, not silently ignored, but explicitly
not something this lab attempts to "fix" since it doesn't indicate an
actual defect.

## 11. Lab limitations

- `@KafkaListener`'s behavior under a rebalance mid-batch (a real
  question `docs/references/REFERENCE_REPOSITORIES.md` itself raises
  for this WP: "does `@KafkaListener`'s default ack mode match what you
  assume it does under a rebalance mid-batch?") is answered
  analytically in Section 3's framing (BATCH ack mode's native
  equivalent is WP-06's own batch-commit boundary, and WP-05 already
  covers rebalance-timing mechanics in depth) rather than with a
  dedicated new automated test -- a real rebalance-mid-batch test is
  notoriously hard to make deterministic, and no failure-matrix row
  names this WP specifically.
- No Spring Boot Actuator / Micrometer observability integration --
  Spring Kafka ships real Micrometer instrumentation
  (`KafkaTemplateObservationConvention`, confirmed present in
  `KafkaAutoConfiguration`'s own method signatures) that this lab
  doesn't exercise; that's WP-16's territory (observability).
- No `@KafkaListener` batch-mode (`List<ConsumerRecord<...>>` per
  invocation, processing a whole poll batch as one collection) -- only
  the default per-record invocation style is used, to keep every
  listener's logic directly comparable to the native, single-record
  processing style every prior lab already uses.

## 12. Principal Engineer questions

**1. Why does Section 4's finding (owning one custom `KafkaTemplate`
means owning all of them) matter beyond this lab?** It's a real,
easy-to-hit production trap: a team adding ONE custom Kafka producer
bean for a new feature can silently break every OTHER part of the app
that relied on Spring Boot's autoconfigured default -- with no compile
error, only a runtime `NoSuchBeanDefinitionException` the first time
the app actually starts (or, worse, the first time a code path that
needed the default bean actually executes, if it's not eagerly
initialized).

**2. Given Section 5's finding, when WOULD a global
`transaction-id-prefix` be the right choice?** When essentially all of
an application's Kafka traffic genuinely needs transactional semantics
-- a service whose ENTIRE purpose is atomic multi-topic writes, for
example. For a typical service mixing transactional and non-transactional
traffic (this lab's own shape), a dedicated, separate transactional
template is the correct default, not the global property.

**3. Section 6 shows the DLT exception header holds a WRAPPER exception,
not the listener's own. Why would Spring Kafka do this instead of
unwrapping to the original cause?** `ListenerExecutionFailedException`
carries context the raw cause alone wouldn't -- which listener method
failed, and Spring Kafka's own error-handling pipeline can reason about
it uniformly regardless of what the underlying business exception type
is. The ORIGINAL cause is still fully preserved, just in `-cause-fqcn`
instead of `-fqcn` -- nothing is lost, it's additional structure, not a
loss of information.

**4. Section 8's trade-off table says `@RetryableTopic` doesn't
preserve per-key ordering across a retry -- when would WP-13's
blocking, same-partition approach be the better choice specifically
BECAUSE of that?** Any workload where processing order matters WITHIN a
single key across a period that includes a retry -- e.g., a sequence of
state-transition events for the same entity, where processing event 2
before a delayed retry of event 1 finishes would apply them
out-of-order. `@RetryableTopic`'s non-blocking design trades that
ordering guarantee for throughput isolation; WP-13's design keeps
ordering by accepting that one troublesome key can slow its neighbors.

**5. This lab's `@RetryableTopic` listener always throws -- what would
change if it sometimes succeeded?** Nothing about the MECHANISM --
`attemptedValues()` would simply stop growing once a retry attempt
succeeds, and no `@DltHandler` invocation would ever happen for that
record. The topic-traversal machinery doesn't know or care whether an
attempt is "the first" or "a retry" from the listener's own code's
perspective -- `@KafkaListener` is invoked identically regardless of
which topic in the retry chain the record actually arrived on.

**6. Why does `TransactionalOrderPublisher`'s constructor need
`@Qualifier("transactionalKafkaTemplate")` at all, given Section 4 says
there can only be ONE `KafkaTemplate` the app owns once you define your
own?** Section 4's finding is about Spring BOOT's autoconfigured
default being suppressed -- it doesn't prevent an application from
defining MULTIPLE of its OWN `KafkaTemplate` beans (this lab has two:
the plain one and the transactional one). The qualifier is needed
because, with two beans of the exact same type in the context,
Spring's injection-by-type alone is ambiguous -- ordinary Spring DI
behavior, unrelated to the autoconfiguration-suppression finding
itself.

**7. What would change about this WP's tests if they used Testcontainers
(a real, containerized broker) instead of `@EmbeddedKafka`?** Almost
nothing about what's being PROVEN -- `@EmbeddedKafka` runs a real,
unmocked broker, just in-process rather than in a container, so every
finding in this doc (the DLT suffix, the header encoding, the
committed-offset behavior) would be identical either way. The practical
difference is speed (no container startup) and Spring Kafka's own
first-class integration (`spring.embedded.kafka.brokers`,
`@DynamicPropertySource` wiring) -- exactly why this WP's reference
material names `@EmbeddedKafka` specifically as the right tool here.
