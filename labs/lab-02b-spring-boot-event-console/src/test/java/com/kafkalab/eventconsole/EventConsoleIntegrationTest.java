package com.kafkalab.eventconsole;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.consume.EventConsumer;
import com.kafkalab.eventconsole.model.EventDocument;
import com.kafkalab.eventconsole.repo.EventQueryRepository;

/**
 * Real Kafka broker + real MongoDB + the real Spring Boot app on a real port.
 * Nothing mocked: the HTTP call publishes to Kafka, the @KafkaListener consumes
 * it, and the assertions read what actually landed in MongoDB through the API.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventConsoleIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole"));
        registry.add("app.topic", () -> "event-console-test");
        registry.add("app.consumer-max-retries", () -> "3");
        registry.add("app.consumer-backoff-initial-ms", () -> "50");
        registry.add("app.consumer-backoff-max-ms", () -> "200");
    }

    @LocalServerPort
    int port;

    @Autowired
    EventConsumer consumer;

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

    private Map<String, Object> post(Map<String, Object> body) {
        return http.post().uri("/api/publish/bulk").body(body).retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    private Map<String, Object> get(String uri) {
        return http.get().uri(uri).retrieve().body(new ParameterizedTypeReference<>() {
        });
    }

    private long storedTotal() {
        return ((Number) get("/api/events/stats").get("total")).longValue();
    }

    @Test
    void bulkPublishIsConsumedIntoMongoAndTheSameKeyStaysOnOnePartition() {
        Map<String, Object> job = post(Map.of(
                "mode", "GENERATE", "count", 100, "keyStrategy", "CYCLE", "keyCount", 5, "keyPrefix", "cust-"));

        // The callback-confirmed result, not "send() returned".
        assertThat(job.get("requested")).isEqualTo(100);
        assertThat(job.get("acked")).isEqualTo(100);
        assertThat(job.get("failed")).isEqualTo(0);
        @SuppressWarnings("unchecked")
        Map<String, Number> perPartition = (Map<String, Number>) job.get("partitionCounts");
        assertThat(perPartition.values().stream().mapToLong(Number::longValue).sum()).isEqualTo(100);

        // The consumer is asynchronous: wait for the read model to catch up.
        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 100);

        Map<String, Object> page = get("/api/events?size=200");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) page.get("items");
        assertThat(items).hasSize(100);

        // Per-key partition affinity: every event for one key sits on exactly one partition.
        Map<String, Set<Object>> partitionsByKey = new HashMap<>();
        for (Map<String, Object> item : items) {
            partitionsByKey.computeIfAbsent((String) item.get("key"), k -> new HashSet<>()).add(item.get("partition"));
        }
        assertThat(partitionsByKey).hasSize(5);
        partitionsByKey.values().forEach(partitions -> assertThat(partitions).hasSize(1));

        // The stored per-partition split matches what Kafka acknowledged.
        @SuppressWarnings("unchecked")
        Map<String, Number> stored = (Map<String, Number>) get("/api/events/stats").get("byPartition");
        stored.forEach((p, n) -> assertThat(n.longValue()).isEqualTo(perPartition.get(p).longValue()));
    }

    @Test
    void pastedEventsArePublishedExactlyAndSearchableByKeyAndValue() {
        Map<String, Object> job = post(Map.of("mode", "PASTE", "events", List.of(
                Map.of("key", "inv-1", "value", "{\"sku\":\"A-100\"}"),
                Map.of("key", "inv-2", "value", "{\"sku\":\"B-200\"}"),
                Map.of("value", "keyless-event"))));
        assertThat(job.get("acked")).isEqualTo(3);

        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 3);

        assertThat(get("/api/events?key=inv-2").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=A-100").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=keyless").get("total")).isEqualTo(1);
        assertThat(get("/api/events?key=does-not-exist").get("total")).isEqualTo(0);
    }

    @Test
    void redeliveringRecordsNeverCreatesDuplicatesEvenInAMixedBatch() {
        ConsumerRecord<String, String> a = new ConsumerRecord<>("event-console-test", 2, 4242L, "k", "v");
        ConsumerRecord<String, String> b = new ConsumerRecord<>("event-console-test", 2, 4243L, "k", "v");
        ConsumerRecord<String, String> c = new ConsumerRecord<>("event-console-test", 1, 7L, "k", "v");

        assertThat(consumer.storeBatch(List.of(a))).isEqualTo(1);
        // Same record again: nothing new.
        assertThat(consumer.storeBatch(List.of(a))).isZero();
        // A batch that mixes one already-stored record with two new ones: only the new ones land,
        // and the duplicate does not stop the rest of the (unordered) bulk write.
        assertThat(consumer.storeBatch(List.of(a, b, c))).isEqualTo(2);

        assertThat(events.count()).isEqualTo(3);
    }

    @Test
    void aRecordThatCannotBeStoredIsDeadLetteredWithoutLosingItsNeighbours() {
        // Simulate MongoDB rejecting exactly one record, every time. It is a "poison"
        // record from the consumer's point of view: retrying will never succeed.
        doThrow(new IllegalStateException("simulated mongo failure")).when(events)
                .insertAllIgnoringDuplicates(argThat(l -> l != null && l.stream().anyMatch(d -> d.value().contains("poison"))));
        doThrow(new IllegalStateException("simulated mongo failure")).when(events)
                .insertIfAbsent(argThat(d -> d != null && d.value().contains("poison")));

        post(Map.of("mode", "PASTE", "events", List.of(
                Map.of("value", "good-before"),
                Map.of("value", "poison-record"),
                Map.of("value", "good-after"))));

        // Neighbours on both sides of the bad record are still stored...
        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 2);
        assertThat(get("/api/events?q=good-before").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=good-after").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=poison").get("total")).isEqualTo(0);

        // ...and the bad record was not dropped: it is on the dead-letter topic.
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<String> dead = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> dlt = new KafkaConsumer<>(props)) {
            dlt.subscribe(List.of("event-console-test.DLT"));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (dead.isEmpty() && System.nanoTime() < deadline) {
                dlt.poll(Duration.ofMillis(500)).forEach(r -> dead.add(r.value()));
            }
        }
        assertThat(dead).containsExactly("poison-record");
    }

    @Test
    void aDatabaseOutageStallsTheConsumerInsteadOfDeadLetteringHealthyRecords() {
        // MongoDB "down" for MORE failed attempts than the retry budget (app.consumer-max-retries=3).
        // A poison record would be dead-lettered by now; an outage must not be.
        AtomicInteger failuresLeft = new AtomicInteger(8);
        doAnswer(invocation -> {
            if (failuresLeft.getAndDecrement() > 0) {
                throw new org.springframework.dao.DataAccessResourceFailureException("simulated: mongo unreachable");
            }
            return invocation.callRealMethod();
        }).when(events).insertAllIgnoringDuplicates(argThat(l -> l != null && !l.isEmpty()));

        post(Map.of("mode", "GENERATE", "count", 30, "keyStrategy", "UNIQUE", "valuePrefix", "outage"));

        // The consumer recovers by itself once the "database" is back, and every record lands.
        await().atMost(Duration.ofSeconds(90)).until(() -> storedTotal() == 30);
        assertThat(failuresLeft.get()).isLessThanOrEqualTo(0);

        // Nothing healthy was sent to the dead-letter topic.
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-empty-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<String> dead = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> dlt = new KafkaConsumer<>(props)) {
            dlt.subscribe(List.of("event-console-test.DLT"));
            long deadline = System.nanoTime() + Duration.ofSeconds(6).toNanos();
            while (System.nanoTime() < deadline) {
                dlt.poll(Duration.ofMillis(500)).forEach(r -> dead.add(r.value()));
            }
        }
        assertThat(dead).noneMatch(v -> v.contains("outage"));
    }

    @Test
    void theReadModelHasItsIndexesAndAnExpiryTtl() {
        List<IndexInfo> indexes = mongo.indexOps(EventDocument.class).getIndexInfo();
        assertThat(indexes).extracting(IndexInfo::getName).contains("newest_first", "ttl_consumedAt");
        IndexInfo ttl = indexes.stream().filter(i -> i.getName().equals("ttl_consumedAt")).findFirst().orElseThrow();
        assertThat(ttl.getExpireAfter()).isPresent();
        assertThat(ttl.getExpireAfter().get()).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void frameworkErrorsKeepTheirRealStatusCodesInsteadOfBecoming500() {
        assertThat(http.get().uri("/api/nope").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(404);
        assertThat(http.put().uri("/api/publish/bulk").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(405);
        assertThat(http.post().uri("/api/publish/bulk").header("Content-Type", "text/plain").body("x")
                .exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(415);
    }

    @Test
    void oversizedBodiesAreRejectedBeforeBeingParsed() {
        String huge = "x".repeat(9 * 1024 * 1024);
        int status = http.post().uri("/api/publish/bulk").header("Content-Type", "application/json")
                .body("{\"mode\":\"PASTE\",\"pad\":\"" + huge + "\"}")
                .exchange((req, res) -> res.getStatusCode()).value();
        assertThat(status).isEqualTo(413);
    }

    @Test
    void unsafeCharactersInGeneratedFieldsAreRejected() {
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 1, "valuePrefix", "a\"},{\"x"))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 1, "keyPrefix", "bad key!"))).isEqualTo(400);
    }

    @Test
    void invalidRequestsAreRejectedWith400() {
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 0))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 60000))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE"))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "PASTE", "events", List.of()))).isEqualTo(400);
        assertThat(statusOf(Map.of("count", 5))).isEqualTo(400);
    }

    private int statusOf(Map<String, Object> body) {
        return http.post().uri("/api/publish/bulk").body(body)
                .exchange((req, res) -> res.getStatusCode()).value();
    }

    @Test
    void clearingTheReadModelDoesNotRemoveRecordsFromKafka() {
        post(Map.of("mode", "GENERATE", "count", 20, "keyStrategy", "UNIQUE"));
        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 20);

        Map<String, Object> deleted = http.delete().uri("/api/events").retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
        assertThat(((Number) deleted.get("deleted")).longValue()).isEqualTo(20);
        assertThat(storedTotal()).isZero();

        // Prove it against the broker itself: a brand-new consumer group, reading
        // from the start, still finds every record. Consuming (and clearing our
        // copy) never deleted anything from the log.
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        int seen = 0;
        try (KafkaConsumer<String, String> verifier = new KafkaConsumer<>(props)) {
            verifier.subscribe(List.of("event-console-test"));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (seen < 20 && System.nanoTime() < deadline) {
                seen += verifier.poll(Duration.ofMillis(500)).count();
            }
        }
        assertThat(seen).isGreaterThanOrEqualTo(20);
    }
}
