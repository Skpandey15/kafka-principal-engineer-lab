package com.kafkalab.eventconsole.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.consumer.config.EventStoreSetup;
import com.kafkalab.eventconsole.consumer.consume.EventConsumer;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;

/**
 * The consumer service against a real broker and a real MongoDB, with the retry worker switched
 * OFF so that the state a bad event is stored in can be inspected before anything moves it on
 * (the worker has its own suite: {@link RetryWorkerIntegrationTest}).
 *
 * <p>Note what is NOT here: no producer service. Records are written straight to the topic with a
 * plain Kafka producer, which is the point of the segregation -- the consumer must work with
 * whatever writes to the topic, and fails or recovers on its own terms.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConsumerIntegrationTest {

    private static final String TOPIC = "consumer-test";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole_consumer"));
        registry.add("app.topic", () -> TOPIC);
        registry.add("app.consumer-backoff-initial-ms", () -> "50");
        registry.add("app.consumer-backoff-max-ms", () -> "200");
        // Nothing may move a FAILED event on while a test is looking at it.
        registry.add("app.retry.enabled", () -> "false");
    }

    @LocalServerPort
    int port;

    @Value("${spring.kafka.consumer.group-id}")
    String groupId;

    @Autowired
    EventConsumer consumer;

    @Autowired
    EventRetryRepository retries;

    @Autowired
    EventStoreSetup setup;

    @MockitoSpyBean
    EventQueryRepository events;

    @Autowired
    MongoTemplate mongo;

    RestClient http;

    @BeforeEach
    void setUp() {
        http = RestClient.create("http://localhost:" + port);
        events.deleteAll();
    }

    // --- helpers -----------------------------------------------------------------------------

    /** Writes records to the topic like any producer would; waits for the broker's acknowledgement. */
    private void produce(List<String[]> keyValues) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Future<?>> acks = new ArrayList<>();
            for (String[] kv : keyValues) {
                acks.add(producer.send(new ProducerRecord<>(TOPIC, kv[0], kv[1])));
            }
            for (var ack : acks) {
                ack.get();
            }
        }
    }

    private Map<String, Object> get(String uri) {
        return http.get().uri(uri).retrieve().body(new ParameterizedTypeReference<>() {
        });
    }

    private long storedTotal() {
        return ((Number) get("/api/events/stats").get("total")).longValue();
    }

    /** JSON numbers arrive as Integer; normalise so assertions can compare with long literals. */
    @SuppressWarnings("unchecked")
    private Map<String, Long> byStatus() {
        Map<String, Long> out = new HashMap<>();
        ((Map<String, Number>) get("/api/events/stats").get("byStatus")).forEach((k, v) -> out.put(k, v.longValue()));
        return out;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> items(String uri) {
        return (List<Map<String, Object>>) get(uri).get("items");
    }

    private List<String> readDeadLetters(Duration wait, int stopAfter) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<String> dead = new ArrayList<>();
        try (KafkaConsumer<String, String> dlt = new KafkaConsumer<>(props)) {
            dlt.subscribe(List.of(TOPIC + ".DLT"));
            long deadline = System.nanoTime() + wait.toNanos();
            while (dead.size() < stopAfter && System.nanoTime() < deadline) {
                dlt.poll(Duration.ofMillis(500)).forEach(r -> dead.add(r.value()));
            }
        }
        return dead;
    }

    /** Records the consumer group has not committed past yet, summed over partitions. */
    private long lag() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (int p = 0; p < 3; p++) {
                latest.put(new TopicPartition(TOPIC, p), OffsetSpec.latest());
            }
            long lag = 0;
            for (var end : admin.listOffsets(latest).all().get().entrySet()) {
                OffsetAndMetadata done = committed.get(end.getKey());
                lag += end.getValue().offset() - (done == null ? 0 : done.offset());
            }
            return lag;
        }
    }

    private EventDocument failedEvent(String id, Instant nextRetryAt) {
        return new EventDocument(id, TOPIC, 0, Math.abs(id.hashCode()), "k", "bad", Instant.now(), Instant.now(),
                EventStatus.FAILED, 0, "boom", nextRetryAt, null, null, null, null);
    }

    private EventDocument stored(String id) {
        return mongo.findById(id, EventDocument.class);
    }

    // --- consuming ---------------------------------------------------------------------------

    @Test
    void recordsWrittenByAnyProducerAreStoredAndTheSameKeyStaysOnOnePartition() throws Exception {
        List<String[]> batch = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            batch.add(new String[] { "cust-" + (i % 5), "{\"seq\":" + i + "}" });
        }
        produce(batch);

        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 100);

        List<Map<String, Object>> items = items("/api/events?size=200");
        assertThat(items).hasSize(100);
        assertThat(items).allSatisfy(item -> assertThat(item).containsEntry("status", "SUCCESS"));
        Map<String, Set<Object>> partitionsByKey = new HashMap<>();
        for (Map<String, Object> item : items) {
            partitionsByKey.computeIfAbsent((String) item.get("key"), k -> new HashSet<>()).add(item.get("partition"));
        }
        assertThat(partitionsByKey).hasSize(5);
        partitionsByKey.values().forEach(partitions -> assertThat(partitions).hasSize(1));

        @SuppressWarnings("unchecked")
        Map<String, Number> byPartition = (Map<String, Number>) get("/api/events/stats").get("byPartition");
        assertThat(byPartition.values().stream().mapToLong(Number::longValue).sum()).isEqualTo(100);
    }

    @Test
    void keyAndValueSearchAndKeylessRecordsWork() throws Exception {
        produce(List.of(new String[] { "inv-1", "{\"sku\":\"A-100\"}" }, new String[] { "inv-2", "{\"sku\":\"B-200\"}" },
                new String[] { null, "{\"note\":\"keyless-event\"}" }));
        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 3);

        assertThat(get("/api/events?key=inv-2").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=A-100").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=keyless").get("total")).isEqualTo(1);
        assertThat(get("/api/events?key=does-not-exist").get("total")).isEqualTo(0);
    }

    @Test
    void redeliveringRecordsNeverCreatesDuplicatesEvenInAMixedBatch() {
        ConsumerRecord<String, String> a = new ConsumerRecord<>(TOPIC, 2, 4242L, "k", "{}");
        ConsumerRecord<String, String> b = new ConsumerRecord<>(TOPIC, 2, 4243L, "k", "{}");
        ConsumerRecord<String, String> c = new ConsumerRecord<>(TOPIC, 1, 7L, "k", "{}");

        assertThat(consumer.storeBatch(List.of(a))).isEqualTo(1);
        assertThat(consumer.storeBatch(List.of(a))).isZero();
        // One already-stored record mixed with two new ones: only the new ones land, and the
        // duplicate does not stop the rest of the (unordered) bulk write.
        assertThat(consumer.storeBatch(List.of(a, b, c))).isEqualTo(2);

        assertThat(events.count()).isEqualTo(3);
    }

    @Test
    void aRedeliveredFailedEventDoesNotResetItsRetryProgress() {
        ConsumerRecord<String, String> bad = new ConsumerRecord<>(TOPIC, 2, 9001L, "k", "not json");
        consumer.storeBatch(List.of(bad));
        String id = EventDocument.idFor(TOPIC, 2, 9001L);
        assertThat(retries.markRetryFailed(id, 0, "second failure", Instant.now().plusSeconds(30))).isTrue();

        // The batch is delivered again (crash before the offset commit). The existing document wins.
        assertThat(consumer.storeBatch(List.of(bad))).isZero();

        assertThat(stored(id).retries()).isEqualTo(1);
        assertThat(stored(id).lastError()).isEqualTo("second failure");
    }

    // --- failures are data, not blockers -----------------------------------------------------

    @Test
    void anInvalidEventIsStoredAsFailedWithItsReasonAndNeverBlocksOrLosesItsNeighbours() throws Exception {
        produce(List.of(new String[] { "a", "{\"ok\":\"before\"}" }, new String[] { "b", "poison-record" },
                new String[] { "c", "{\"ok\":\"after\"}" }));

        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 3);
        assertThat(byStatus()).containsEntry("SUCCESS", 2L).containsEntry("FAILED", 1L).containsEntry("DEAD", 0L);

        Map<String, Object> bad = items("/api/events?status=FAILED").getFirst();
        assertThat(bad).containsEntry("value", "poison-record").containsEntry("retries", 0);
        assertThat((String) bad.get("lastError")).startsWith("EventProcessingException: value is not valid JSON");
        assertThat(bad.get("nextRetryAt")).isNotNull();

        // The consumer moved past it: its offset is committed, so a restart would not replay it...
        await().atMost(Duration.ofSeconds(30)).until(() -> lag() == 0);
        // ...and it keeps consuming afterwards.
        produce(List.<String[]>of(new String[] { "d", "{\"ok\":\"later\"}" }));
        await().atMost(Duration.ofSeconds(30)).until(() -> storedTotal() == 4);
        // Nothing was dead-lettered: DEAD is only reachable through the retry worker.
        assertThat(readDeadLetters(Duration.ofSeconds(3), Integer.MAX_VALUE)).isEmpty();
    }

    @Test
    void aDatabaseOutageStallsTheConsumerInsteadOfFailingOrDeadLetteringHealthyEvents() throws Exception {
        // MongoDB "down" for many failed attempts: a poison-handling policy would have given up
        // by now. An outage is not the events' fault, so the consumer must just wait.
        AtomicInteger failuresLeft = new AtomicInteger(8);
        doAnswer(invocation -> {
            if (failuresLeft.getAndDecrement() > 0) {
                throw new DataAccessResourceFailureException("simulated: mongo unreachable");
            }
            return invocation.callRealMethod();
        }).when(events).insertAllIgnoringDuplicates(argThat(l -> l != null && !l.isEmpty()));

        List<String[]> batch = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            batch.add(new String[] { "k-" + i, "{\"outage\":" + i + "}" });
        }
        produce(batch);

        await().atMost(Duration.ofSeconds(90)).until(() -> storedTotal() == 30);
        assertThat(failuresLeft.get()).isLessThanOrEqualTo(0);
        assertThat(byStatus()).containsEntry("SUCCESS", 30L).containsEntry("FAILED", 0L).containsEntry("DEAD", 0L);
        assertThat(readDeadLetters(Duration.ofSeconds(4), Integer.MAX_VALUE)).isEmpty();
    }

    @Test
    void clearingTheReadModelDoesNotRemoveRecordsFromKafka() throws Exception {
        List<String[]> batch = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            batch.add(new String[] { "u-" + i, "{\"clear\":" + i + "}" });
        }
        produce(batch);
        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 20);

        Map<String, Object> deleted = http.delete().uri("/api/events").retrieve().body(new ParameterizedTypeReference<>() {
        });
        assertThat(((Number) deleted.get("deleted")).longValue()).isEqualTo(20);
        assertThat(storedTotal()).isZero();

        // A brand-new consumer group, reading from the start, still finds every record.
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        int seen = 0;
        try (KafkaConsumer<String, String> verifier = new KafkaConsumer<>(props)) {
            verifier.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (seen < 20 && System.nanoTime() < deadline) {
                seen += verifier.poll(Duration.ofMillis(500)).count();
            }
        }
        assertThat(seen).isGreaterThanOrEqualTo(20);
    }

    // --- the retry queue's storage rules (what lets several workers share it safely) ----------

    @Test
    void severalWorkersClaimingAtOnceNeverGetTheSameEventAndNoneIsMissed() throws Exception {
        Instant now = Instant.now();
        for (int i = 0; i < 200; i++) {
            mongo.insert(failedEvent("race-" + i, now.minusSeconds(5)));
        }

        ConcurrentLinkedQueue<String> claimed = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> workers = new ArrayList<>();
            for (int w = 0; w < 8; w++) {
                workers.add(pool.submit(() -> {
                    while (true) {
                        var next = retries.claimDueForRetry(Instant.now(), Duration.ofMinutes(5));
                        if (next.isEmpty()) {
                            return;
                        }
                        claimed.add(next.get().id());
                    }
                }));
            }
            for (Future<?> worker : workers) {
                worker.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(claimed).hasSize(200).doesNotHaveDuplicates();
    }

    @Test
    void aClaimedEventIsInvisibleUntilItsLeaseExpiresAndAnEventNotYetDueIsNeverClaimed() {
        Instant t0 = Instant.now();
        mongo.insert(failedEvent("lease-due", t0.minusSeconds(1)));
        mongo.insert(failedEvent("lease-future", t0.plusSeconds(3600)));

        assertThat(retries.claimDueForRetry(t0, Duration.ofSeconds(30))).map(EventDocument::id).contains("lease-due");
        // A second worker, a moment later: the first holds the lease, and the other event is not due.
        assertThat(retries.claimDueForRetry(t0.plusSeconds(10), Duration.ofSeconds(30))).isEmpty();
        // The first worker died and never reported back: once the lease runs out the event is up for grabs again.
        assertThat(retries.claimDueForRetry(t0.plusSeconds(31), Duration.ofSeconds(30))).map(EventDocument::id)
                .contains("lease-due");
    }

    @Test
    void aWorkerThatLostItsLeaseCannotOverwriteWhatTheWorkerThatTookOverDecided() {
        Instant now = Instant.now();
        mongo.insert(failedEvent("stale", now.minusSeconds(1)));
        EventDocument claimedByA = retries.claimDueForRetry(now, Duration.ofSeconds(30)).orElseThrow();
        // A stalls past its lease; B claims the same event and finishes it.
        EventDocument claimedByB = retries.claimDueForRetry(now.plusSeconds(31), Duration.ofSeconds(30)).orElseThrow();
        assertThat(retries.markSucceeded(claimedByB.id(), claimedByB.retries(), now)).isTrue();

        // A wakes up and reports its (stale) failure: it must match nothing.
        assertThat(retries.markRetryFailed(claimedByA.id(), claimedByA.retries(), "late", now.plusSeconds(60))).isFalse();
        assertThat(retries.markDead(claimedByA.id(), claimedByA.retries(), "late", now)).isFalse();

        EventDocument finalState = stored("stale");
        assertThat(finalState.status()).isEqualTo(EventStatus.SUCCESS);
        assertThat(finalState.retries()).isEqualTo(1);
        assertThat(finalState.lastError()).isEqualTo("boom");
    }

    @Test
    void anEventThatBecomesDeadIsWaitingForItsDeadLetterConfirmationAndCanBeRequeuedOnlyThen() {
        Instant now = Instant.now();
        mongo.insert(failedEvent("dying", now.minusSeconds(1)));
        EventDocument claimed = retries.claimDueForRetry(now, Duration.ofSeconds(30)).orElseThrow();

        // Requeue is for DEAD events only: this one is still being worked on.
        assertThat(retries.requeue("dying", now)).isFalse();

        assertThat(retries.markDead(claimed.id(), claimed.retries(), "gave up", now)).isTrue();
        EventDocument dead = stored("dying");
        assertThat(dead.status()).isEqualTo(EventStatus.DEAD);
        assertThat(dead.retries()).isEqualTo(1);
        assertThat(dead.dltPublished()).isFalse();
        assertThat(dead.deadAt()).isNotNull();
        assertThat(dead.nextRetryAt()).isNull();

        // The dead-letter sweep sees it; once confirmed it is no longer offered.
        assertThat(retries.claimDeadForDlt(now, Duration.ofSeconds(30))).map(EventDocument::id).contains("dying");
        assertThat(retries.claimDeadForDlt(now, Duration.ofSeconds(30))).isEmpty();
        assertThat(retries.markDltPublished("dying")).isTrue();
        assertThat(retries.claimDeadForDlt(now.plusSeconds(3600), Duration.ofSeconds(30))).isEmpty();

        // An operator gives it a fresh start.
        assertThat(retries.requeue("dying", now)).isTrue();
        EventDocument requeued = stored("dying");
        assertThat(requeued.status()).isEqualTo(EventStatus.FAILED);
        assertThat(requeued.retries()).isZero();
        assertThat(requeued.deadAt()).isNull();
        assertThat(requeued.dltPublished()).isNull();
        assertThat(retries.requeue("dying", now)).isFalse();
    }

    // --- the HTTP surface --------------------------------------------------------------------

    @Test
    void theApiFiltersByStatusAndReportsCountsPerStatus() {
        Instant now = Instant.now();
        consumer.storeBatch(List.of(new ConsumerRecord<>(TOPIC, 0, 1L, "k", "{}"), new ConsumerRecord<>(TOPIC, 0, 2L, "k", "{}"),
                new ConsumerRecord<>(TOPIC, 0, 3L, "k", "not json")));
        mongo.insert(failedEvent("api-dead", now));
        retries.claimDueForRetry(now.plusSeconds(1), Duration.ofSeconds(30));
        retries.markDead("api-dead", 0, "gave up", now);

        assertThat(byStatus()).containsEntry("SUCCESS", 2L).containsEntry("FAILED", 1L).containsEntry("DEAD", 1L);
        assertThat(get("/api/events?status=SUCCESS").get("total")).isEqualTo(2);
        assertThat(get("/api/events?status=FAILED").get("total")).isEqualTo(1);
        assertThat(get("/api/events?status=DEAD").get("total")).isEqualTo(1);
        assertThat(get("/api/events").get("total")).isEqualTo(4);
        assertThat(http.get().uri("/api/events?status=NONSENSE").exchange((req, res) -> res.getStatusCode()).value())
                .isEqualTo(400);
    }

    @Test
    void anOperatorCanRequeueADeadEventOverHttpAndNothingElse() {
        Instant now = Instant.now();
        mongo.insert(failedEvent("http-dead", now));
        retries.claimDueForRetry(now.plusSeconds(1), Duration.ofSeconds(30));
        retries.markDead("http-dead", 0, "gave up", now);
        mongo.insert(failedEvent("http-failed", now.plusSeconds(3600)));

        int ok = http.post().uri("/api/events/http-dead/requeue").exchange((req, res) -> res.getStatusCode()).value();
        int stillFailed = http.post().uri("/api/events/http-failed/requeue").exchange((req, res) -> res.getStatusCode()).value();
        int unknown = http.post().uri("/api/events/no-such-event/requeue").exchange((req, res) -> res.getStatusCode()).value();

        assertThat(ok).isEqualTo(200);
        assertThat(stillFailed).isEqualTo(404);
        assertThat(unknown).isEqualTo(404);
        assertThat(stored("http-dead").status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored("http-failed").nextRetryAt()).isAfter(now.plusSeconds(3000));
    }

    @Test
    void theConsumerExposesNoWriteEndpointForEventsAndNoInfrastructureDetail() {
        // It cannot be used to inject events: only reads, requeueing, and clearing its own read model.
        assertThat(http.post().uri("/api/events").header("Content-Type", "application/json").body("{}")
                .exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(405);
        assertThat(http.post().uri("/api/publish/bulk").header("Content-Type", "application/json").body("{}")
                .exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(404);

        Map<String, Object> config = get("/api/events/config");
        assertThat(config).containsEntry("topic", TOPIC).containsKey("consumerGroup")
                .containsEntry("deadLetterTopic", TOPIC + ".DLT").containsEntry("maxRetries", 5);
        assertThat(config).doesNotContainKey("bootstrapServers");
    }

    @Test
    void frameworkErrorsKeepTheirRealStatusCodesInsteadOfBecoming500() {
        assertThat(http.get().uri("/api/nope").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(404);
        assertThat(http.put().uri("/api/events").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(405);
    }

    // --- indexes and migration -----------------------------------------------------------------

    @Test
    void onlySuccessfulEventsExpireBecauseTheTtlIndexIsPartial() {
        List<IndexInfo> indexes = mongo.indexOps(EventDocument.class).getIndexInfo();
        assertThat(indexes).extracting(IndexInfo::getName)
                .contains("newest_first", "status_next_retry", EventStoreSetup.TTL_INDEX)
                .doesNotContain(EventStoreSetup.LEGACY_TTL_INDEX);

        IndexInfo ttl = indexes.stream().filter(i -> i.getName().equals(EventStoreSetup.TTL_INDEX)).findFirst().orElseThrow();
        assertThat(ttl.getExpireAfter()).contains(Duration.ofDays(7));
        assertThat(ttl.getPartialFilterExpression()).contains("status").contains("SUCCESS");
    }

    @Test
    void startupDropsTheOldUnsafeIndexBackfillsStatusAndAppliesATtlChangeWithoutDowntime() {
        // Recreate the situation of a database written by the previous version: the old TTL index
        // (which would expire FAILED/DEAD events too) and events that have no status field.
        var indexOps = mongo.indexOps(EventDocument.class);
        indexOps.dropIndex(EventStoreSetup.TTL_INDEX);
        indexOps.createIndex(new Index().named(EventStoreSetup.LEGACY_TTL_INDEX).on("consumedAt", Sort.Direction.ASC)
                .expire(Duration.ofDays(7)));
        mongo.getCollection("events").insertOne(new Document("_id", "legacy-1").append("topic", TOPIC).append("partition", 0)
                .append("offset", 1L).append("value", "{}").append("consumedAt", new java.util.Date()));

        setup.afterPropertiesSet();

        assertThat(indexOps.getIndexInfo()).extracting(IndexInfo::getName)
                .contains(EventStoreSetup.TTL_INDEX).doesNotContain(EventStoreSetup.LEGACY_TTL_INDEX);
        assertThat(mongo.getCollection("events").find(new Document("_id", "legacy-1")).first().getString("status"))
                .isEqualTo("SUCCESS");
        // Running it again changes nothing.
        assertThat(setup.migrateLegacyEvents()).isZero();

        // A changed TTL is applied in place (MongoDB rejects re-creating the index with new options).
        setup.ensureTtlIndex(Duration.ofDays(1));
        assertThat(indexOps.getIndexInfo().stream().filter(i -> i.getName().equals(EventStoreSetup.TTL_INDEX)).findFirst()
                .orElseThrow().getExpireAfter()).contains(Duration.ofDays(1));
        setup.ensureTtlIndex(Duration.ofDays(7));
        assertThat(indexOps.getIndexInfo().stream().filter(i -> i.getName().equals(EventStoreSetup.TTL_INDEX)).findFirst()
                .orElseThrow().getExpireAfter()).contains(Duration.ofDays(7));
    }
}
