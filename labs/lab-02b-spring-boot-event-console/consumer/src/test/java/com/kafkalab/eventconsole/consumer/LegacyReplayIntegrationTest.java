package com.kafkalab.eventconsole.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.consumer.testsupport.FakeSchemaRegistry;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The situation that bit the first time the contract was introduced into a topic that already had
 * history: a NEW consumer (a rebuilt read model) reads the whole topic from the start, and every
 * record written before the contract has no schema header.
 *
 * <p>The history is written BEFORE the application starts (so the consumer genuinely replays it), the
 * cut-over is the topic's end offset at that moment, and then records are written AFTER it: some
 * that bypass the producer service (no header) and some that follow the contract.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LegacyReplayIntegrationTest {

    private static final String TOPIC = "legacy-replay-test";
    private static final int HISTORY = 12;

    static final FakeSchemaRegistry REGISTRY = new FakeSchemaRegistry().withJsonSchema(1, FakeSchemaRegistry.V1);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    /** The topic's end offset on partition 0 when the "contract" was introduced. */
    static long cutover;

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole_consumer_legacy"));
        registry.add("app.topic", () -> TOPIC);
        registry.add("app.schema.registry-url", REGISTRY::url);
        registry.add("app.schema.legacy-until-offsets[0]", () -> String.valueOf(cutover));
        registry.add("app.retry.enabled", () -> "false");
    }

    @LocalServerPort
    int port;

    @Autowired
    MeterRegistry metrics;

    /** Writes history that predates the contract: free text and JSON alike, no header, all on partition 0. */
    @BeforeAll
    static void writeHistoryBeforeTheApplicationExists() throws Exception {
        try (KafkaProducer<String, String> producer = producer()) {
            for (int i = 0; i < HISTORY; i++) {
                String value = i % 3 == 0 ? "a plain-text event from before schemas #" + i : "{\"whatever\":" + i + "}";
                producer.send(new ProducerRecord<>(TOPIC, 0, null, value)).get();
            }
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition p0 = new TopicPartition(TOPIC, 0);
            cutover = consumer.endOffsets(List.of(p0)).get(p0);
        }
        assertThat(cutover).isEqualTo(HISTORY);
    }

    private static KafkaProducer<String, String> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private RestClient http() {
        return RestClient.create("http://localhost:" + port);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Number> byStatus() {
        Map<String, Object> stats = http().get().uri("/api/events/stats").retrieve().body(new ParameterizedTypeReference<>() {
        });
        return (Map<String, Number>) stats.get("byStatus");
    }

    @Test
    void replayingHistoryAcceptsWhatPredatesTheContractButStillCatchesWhatBypassesItAfterwards() throws Exception {
        // The application has just started with a brand-new consumer group: it is replaying the history.
        await().atMost(Duration.ofSeconds(60)).until(() -> byStatus().get("SUCCESS").intValue() == HISTORY);
        assertThat(byStatus().get("FAILED").intValue()).as("none of the history failed").isZero();
        assertThat(metrics.counter("eventconsole.consume.legacy").count()).isEqualTo(HISTORY);

        // Now, AFTER the cut-over: three that bypass the producer service, two that follow the contract.
        try (KafkaProducer<String, String> producer = producer()) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(TOPIC, 0, null, "{\"id\":\"bypass-" + i + "\"}")).get();
            }
            for (int i = 0; i < 2; i++) {
                ProducerRecord<String, String> ok = new ProducerRecord<>(TOPIC, 0, null, "{\"id\":\"ok-" + i + "\"}");
                ok.headers().add("x-schema-id", "1".getBytes());
                producer.send(ok).get();
            }
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> byStatus().get("SUCCESS").intValue() == HISTORY + 2);
        assertThat(byStatus().get("FAILED").intValue()).as("the bypassers are caught, not excused as legacy").isEqualTo(3);
        assertThat(metrics.counter("eventconsole.consume.legacy").count()).as("and were not counted as legacy").isEqualTo(HISTORY);
    }
}
