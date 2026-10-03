package com.kafkalab.eventconsole.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.dao.DataAccessResourceFailureException;
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
import com.kafkalab.eventconsole.consumer.consume.EventConsumer;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;

/**
 * The consumer service against a real broker and a real MongoDB. Note what is NOT here: no
 * producer service. Records are written straight to the topic with a plain Kafka producer,
 * which is the point of the segregation -- the consumer must work with whatever writes to the
 * topic, and fails or recovers on its own terms.
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
        // Fast retries so the poison/outage tests finish quickly.
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

    /** Writes records to the topic like any producer would; waits for the broker's acknowledgement. */
    private void produce(List<String[]> keyValues) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<java.util.concurrent.Future<?>> acks = new ArrayList<>();
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

    @Test
    void recordsWrittenByAnyProducerAreStoredAndTheSameKeyStaysOnOnePartition() throws Exception {
        List<String[]> batch = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            batch.add(new String[] { "cust-" + (i % 5), "{\"seq\":" + i + "}" });
        }
        produce(batch);

        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 100);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) get("/api/events?size=200").get("items");
        assertThat(items).hasSize(100);
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
                new String[] { null, "keyless-event" }));
        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 3);

        assertThat(get("/api/events?key=inv-2").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=A-100").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=keyless").get("total")).isEqualTo(1);
        assertThat(get("/api/events?key=does-not-exist").get("total")).isEqualTo(0);
    }

    @Test
    void redeliveringRecordsNeverCreatesDuplicatesEvenInAMixedBatch() {
        ConsumerRecord<String, String> a = new ConsumerRecord<>(TOPIC, 2, 4242L, "k", "v");
        ConsumerRecord<String, String> b = new ConsumerRecord<>(TOPIC, 2, 4243L, "k", "v");
        ConsumerRecord<String, String> c = new ConsumerRecord<>(TOPIC, 1, 7L, "k", "v");

        assertThat(consumer.storeBatch(List.of(a))).isEqualTo(1);
        assertThat(consumer.storeBatch(List.of(a))).isZero();
        // One already-stored record mixed with two new ones: only the new ones land, and the
        // duplicate does not stop the rest of the (unordered) bulk write.
        assertThat(consumer.storeBatch(List.of(a, b, c))).isEqualTo(2);

        assertThat(events.count()).isEqualTo(3);
    }

    @Test
    void aRecordThatCannotBeStoredIsDeadLetteredWithoutLosingItsNeighbours() throws Exception {
        // MongoDB "rejects" exactly one record, every time: a poison record from the consumer's view.
        doThrow(new IllegalStateException("simulated mongo failure")).when(events)
                .insertAllIgnoringDuplicates(argThat(l -> l != null && l.stream().anyMatch(d -> d.value().contains("poison"))));
        doThrow(new IllegalStateException("simulated mongo failure")).when(events)
                .insertIfAbsent(argThat(d -> d != null && d.value().contains("poison")));

        produce(List.of(new String[] { null, "good-before" }, new String[] { null, "poison-record" },
                new String[] { null, "good-after" }));

        await().atMost(Duration.ofSeconds(60)).until(() -> storedTotal() == 2);
        assertThat(get("/api/events?q=good-before").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=good-after").get("total")).isEqualTo(1);
        assertThat(get("/api/events?q=poison").get("total")).isEqualTo(0);

        // ...and the bad record was not dropped: it is on the dead-letter topic.
        assertThat(readDeadLetters(Duration.ofSeconds(60), 1)).containsExactly("poison-record");
    }

    @Test
    void aDatabaseOutageStallsTheConsumerInsteadOfDeadLetteringHealthyRecords() throws Exception {
        // MongoDB "down" for MORE failed attempts than the retry budget (3): a poison record
        // would be dead-lettered by now; an outage must not be.
        AtomicInteger failuresLeft = new AtomicInteger(8);
        doAnswer(invocation -> {
            if (failuresLeft.getAndDecrement() > 0) {
                throw new DataAccessResourceFailureException("simulated: mongo unreachable");
            }
            return invocation.callRealMethod();
        }).when(events).insertAllIgnoringDuplicates(argThat(l -> l != null && !l.isEmpty()));

        List<String[]> batch = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            batch.add(new String[] { "k-" + i, "outage-" + i });
        }
        produce(batch);

        await().atMost(Duration.ofSeconds(90)).until(() -> storedTotal() == 30);
        assertThat(failuresLeft.get()).isLessThanOrEqualTo(0);
        assertThat(readDeadLetters(Duration.ofSeconds(6), Integer.MAX_VALUE)).noneMatch(v -> v.contains("outage"));
    }

    @Test
    void clearingTheReadModelDoesNotRemoveRecordsFromKafka() throws Exception {
        List<String[]> batch = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            batch.add(new String[] { "u-" + i, "clear-" + i });
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

    @Test
    void theReadModelHasItsIndexesAndAnExpiryTtl() {
        List<IndexInfo> indexes = mongo.indexOps(EventDocument.class).getIndexInfo();
        assertThat(indexes).extracting(IndexInfo::getName).contains("newest_first", "ttl_consumedAt");
        IndexInfo ttl = indexes.stream().filter(i -> i.getName().equals("ttl_consumedAt")).findFirst().orElseThrow();
        assertThat(ttl.getExpireAfter()).contains(Duration.ofDays(7));
    }

    @Test
    void theConsumerExposesNoWriteEndpointForEventsAndNoInfrastructureDetail() {
        // It cannot be used to inject events: only reads, and clearing its own read model.
        assertThat(http.post().uri("/api/events").header("Content-Type", "application/json").body("{}")
                .exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(405);
        assertThat(http.post().uri("/api/publish/bulk").header("Content-Type", "application/json").body("{}")
                .exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(404);

        Map<String, Object> config = get("/api/events/config");
        assertThat(config).containsEntry("topic", TOPIC).containsKey("consumerGroup");
        assertThat(config).doesNotContainKey("bootstrapServers");
    }

    @Test
    void frameworkErrorsKeepTheirRealStatusCodesInsteadOfBecoming500() {
        assertThat(http.get().uri("/api/nope").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(404);
        assertThat(http.put().uri("/api/events").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(405);
    }
}
