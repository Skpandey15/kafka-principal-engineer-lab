package com.kafkalab.eventconsole.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;

/**
 * The whole failure path end to end, with the retry worker RUNNING against a real broker and a
 * real MongoDB: consume -> FAILED -> retried with backoff -> SUCCESS, or DEAD + dead-letter topic.
 * Backoff is shortened to 100-300 ms so it runs in seconds; the schedule itself is unit-tested.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RetryWorkerIntegrationTest {

    private static final String TOPIC = "retry-test";
    private static final int MAX_RETRIES = 3;

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole_consumer_retry"));
        registry.add("app.topic", () -> TOPIC);
        registry.add("app.retry.enabled", () -> "true");
        registry.add("app.retry.max-retries", () -> String.valueOf(MAX_RETRIES));
        registry.add("app.retry.backoff-initial-ms", () -> "100");
        registry.add("app.retry.backoff-max-ms", () -> "300");
        registry.add("app.retry.poll-interval-ms", () -> "100");
        registry.add("app.retry.lease-ms", () -> "1000");
        registry.add("app.retry.dlt-send-timeout-ms", () -> "5000");
    }

    @LocalServerPort
    int port;

    @MockitoSpyBean
    EventProcessor processor;

    @MockitoSpyBean
    EventRetryRepository retries;

    @MockitoSpyBean
    KafkaTemplate<?, ?> kafkaTemplate;

    @Autowired
    EventQueryRepository events;

    RestClient http;

    @BeforeEach
    void setUp() {
        http = RestClient.create("http://localhost:" + port);
        events.deleteAll();
    }

    // --- helpers -----------------------------------------------------------------------------

    private void produce(String key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, key, value)).get();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> event(String token) {
        Map<String, Object> page = http.get().uri("/api/events?q=" + token).retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() {
                });
        List<Map<String, Object>> items = (List<Map<String, Object>>) page.get("items");
        return items.isEmpty() ? null : items.getFirst();
    }

    private boolean is(String token, String status) {
        Map<String, Object> e = event(token);
        return e != null && status.equals(e.get("status"));
    }

    private List<ConsumerRecord<String, String>> deadLetters(String token, Duration wait) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> dlt = new KafkaConsumer<>(props)) {
            dlt.subscribe(List.of(TOPIC + ".DLT"));
            long deadline = System.nanoTime() + wait.toNanos();
            while (System.nanoTime() < deadline) {
                dlt.poll(Duration.ofMillis(300)).forEach(r -> {
                    if (r.value().contains(token)) {
                        found.add(r);
                    }
                });
            }
        }
        return found;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value());
    }

    private static String token() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    // --- tests -------------------------------------------------------------------------------

    @Test
    void aTransientFailureIsRetriedWithBackoffUntilItSucceeds() throws Exception {
        String token = token();
        String value = "{\"flaky\":\"" + token + "\"}";
        AtomicInteger attempts = new AtomicInteger();
        doAnswer(invocation -> {
            String v = invocation.getArgument(1);
            if (v != null && v.contains(token) && attempts.incrementAndGet() <= 2) {
                throw new EventProcessingException("downstream temporarily unavailable");
            }
            return invocation.callRealMethod();
        }).when(processor).process(any(), any());

        produce("flaky-key", value);

        await().atMost(Duration.ofSeconds(30)).until(() -> is(token, "SUCCESS"));
        Map<String, Object> done = event(token);
        assertThat(attempts.get()).as("1 at consume time + 2 retries, the second of which succeeded").isEqualTo(3);
        assertThat(done).containsEntry("retries", 2);
        assertThat(done.get("processedAt")).isNotNull();
        assertThat(done.get("nextRetryAt")).isNull();
        assertThat(done.get("lockedUntil")).isNull();
        // What went wrong is kept as history even though it was fixed.
        assertThat((String) done.get("lastError")).contains("downstream temporarily unavailable");
        // It never became a dead letter.
        assertThat(deadLetters(token, Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    void anEventThatKeepsFailingBecomesDeadAfterTheLastRetryAndIsDeadLetteredExactlyOnce() throws Exception {
        String token = token();
        String value = "not json " + token;

        produce("doomed-key", value);

        await().atMost(Duration.ofSeconds(30)).until(() -> is(token, "DEAD"));
        await().atMost(Duration.ofSeconds(30)).until(() -> Boolean.TRUE.equals(event(token).get("dltPublished")));
        Map<String, Object> dead = event(token);
        assertThat(dead).containsEntry("retries", MAX_RETRIES).containsEntry("value", value);
        assertThat(dead.get("deadAt")).isNotNull();
        assertThat(dead.get("nextRetryAt")).isNull();

        // Exactly one dead-letter record, carrying the payload and the story of how it died.
        List<ConsumerRecord<String, String>> letters = deadLetters(token, Duration.ofSeconds(5));
        assertThat(letters).hasSize(1);
        ConsumerRecord<String, String> letter = letters.getFirst();
        assertThat(letter.key()).isEqualTo("doomed-key");
        assertThat(letter.value()).isEqualTo(value);
        assertThat(header(letter, "x-event-id")).isEqualTo(dead.get("id"));
        assertThat(header(letter, "x-retries")).isEqualTo(String.valueOf(MAX_RETRIES));
        assertThat(header(letter, "x-last-error")).contains("value is not valid JSON");

        // 1 attempt at consume time + exactly MAX_RETRIES retries; then it was left alone.
        verify(processor, times(1 + MAX_RETRIES)).process(any(), eq(value));
        Thread.sleep(1_500);
        verify(processor, times(1 + MAX_RETRIES)).process(any(), eq(value));
    }

    @Test
    void aDeadEventCanBeRequeuedAndSucceedsOnceTheCauseIsFixed() throws Exception {
        String token = token();
        String value = "{\"fixable\":\"" + token + "\"}";
        AtomicBoolean fixed = new AtomicBoolean(false);
        doAnswer(invocation -> {
            String v = invocation.getArgument(1);
            if (v != null && v.contains(token) && !fixed.get()) {
                throw new EventProcessingException("rule not deployed yet");
            }
            return invocation.callRealMethod();
        }).when(processor).process(any(), any());

        produce("fixable-key", value);
        await().atMost(Duration.ofSeconds(30)).until(() -> is(token, "DEAD"));

        fixed.set(true); // "the fix was deployed"
        Object id = event(token).get("id");
        int status = http.post().uri("/api/events/{id}/requeue", id).exchange((req, res) -> res.getStatusCode()).value();
        assertThat(status).isEqualTo(200);

        await().atMost(Duration.ofSeconds(30)).until(() -> is(token, "SUCCESS"));
        // Fresh budget: it needed one retry, not MAX_RETRIES + 1.
        assertThat(event(token)).containsEntry("retries", 1);
        // The dead-letter copy written while it was dead is history; requeueing does not delete it.
        assertThat(deadLetters(token, Duration.ofSeconds(2))).hasSize(1);
    }

    @Test
    void aBrokerOutageWhileDeadLetteringKeepsTheEventUnconfirmedAndItIsWrittenLaterExactlyOnce() throws Exception {
        String token = token();
        AtomicInteger sends = new AtomicInteger();
        doAnswer(invocation -> {
            if (sends.incrementAndGet() <= 3) {
                throw new IllegalStateException("simulated: broker unavailable");
            }
            return invocation.callRealMethod();
        }).when(kafkaTemplate).send(any(ProducerRecord.class));

        produce("dlt-key", "not json " + token);

        await().atMost(Duration.ofSeconds(30)).until(() -> is(token, "DEAD"));
        // While Kafka refuses the write, MongoDB says honestly that the dead letter is NOT confirmed.
        assertThat(event(token)).containsEntry("dltPublished", false);

        await().atMost(Duration.ofSeconds(60)).until(() -> Boolean.TRUE.equals(event(token).get("dltPublished")));
        verify(kafkaTemplate, atLeast(4)).send(any(ProducerRecord.class));
        // The failed attempts never reached the broker, so there is exactly one copy.
        assertThat(deadLetters(token, Duration.ofSeconds(4))).hasSize(1);
    }

    @Test
    void aDatabaseOutageWhileRecordingAnAttemptDoesNotUseUpTheEventsRetries() throws Exception {
        String token = token();
        String value = "not json " + token;
        // MongoDB refuses to record the outcome of the first four attempts (an outage), then recovers.
        AtomicInteger refused = new AtomicInteger();
        doAnswer(invocation -> {
            if (refused.incrementAndGet() <= 4) {
                throw new DataAccessResourceFailureException("simulated: mongo unreachable");
            }
            return invocation.callRealMethod();
        }).when(retries).markRetryFailed(anyString(), anyInt(), anyString(), any());

        produce("outage-key", value);

        await().atMost(Duration.ofSeconds(90)).until(() -> is(token, "DEAD"));
        // The event was processed (and failed) more often than its budget allows -- the outage
        // attempts could not be recorded, so they were not counted against it.
        assertThat(event(token)).containsEntry("retries", MAX_RETRIES);
        verify(processor, atLeast(1 + MAX_RETRIES + 4)).process(any(), eq(value));
        assertThat(refused.get()).isGreaterThanOrEqualTo(5);
    }
}
