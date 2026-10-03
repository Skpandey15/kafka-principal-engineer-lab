package com.kafkalab.eventconsole.producer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.producer.model.PublishJob;

/**
 * The producer service against a real broker and a real MongoDB. Note what is NOT here: no
 * consumer, no read model. Whether the records arrived is proven where it matters -- by reading
 * the topic with a plain Kafka consumer. What the consumer service does with them is the
 * consumer project's own test suite.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ProducerIntegrationTest {

    private static final String TOPIC = "producer-test";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole_producer"));
        registry.add("app.topic", () -> TOPIC);
    }

    @LocalServerPort
    int port;

    @Autowired
    MongoTemplate mongo;

    private RestClient http() {
        return RestClient.create("http://localhost:" + port);
    }

    private Map<String, Object> post(Map<String, Object> body) {
        return http().post().uri("/api/publish/bulk").body(body).retrieve().body(new ParameterizedTypeReference<>() {
        });
    }

    private int statusOf(Map<String, Object> body) {
        return http().post().uri("/api/publish/bulk").body(body).exchange((req, res) -> res.getStatusCode()).value();
    }

    /** Reads the whole topic from the beginning with a brand-new consumer group. */
    private List<ConsumerRecord<String, String>> readTopic(int atLeast) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            // Read until we have at least `atLeast` records AND the topic has gone quiet (two empty
            // polls in a row), so records written by other tests cannot make us stop early.
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            int emptyPolls = 0;
            while (System.nanoTime() < deadline && (all.size() < atLeast || emptyPolls < 2)) {
                var batch = consumer.poll(Duration.ofMillis(500));
                emptyPolls = batch.isEmpty() ? emptyPolls + 1 : 0;
                batch.forEach(all::add);
            }
        }
        return all;
    }

    @Test
    void bulkPublishReallyLandsOnTheTopicWithPerKeyPartitionAffinityAndIsAudited() {
        Map<String, Object> job = post(Map.of(
                "mode", "GENERATE", "count", 100, "keyStrategy", "CYCLE", "keyCount", 5, "keyPrefix", "cust-"));

        // The callback-confirmed result, not "send() returned".
        assertThat(job.get("requested")).isEqualTo(100);
        assertThat(job.get("acked")).isEqualTo(100);
        assertThat(job.get("failed")).isEqualTo(0);

        List<ConsumerRecord<String, String>> onTopic = readTopic(100);
        List<ConsumerRecord<String, String>> mine = onTopic.stream()
                .filter(r -> r.key() != null && r.key().startsWith("cust-")).toList();
        assertThat(mine).hasSize(100);

        // Per-key partition affinity, observed on the broker itself.
        Map<String, Set<Integer>> partitionsByKey = new HashMap<>();
        for (ConsumerRecord<String, String> r : mine) {
            partitionsByKey.computeIfAbsent(r.key(), k -> new HashSet<>()).add(r.partition());
        }
        assertThat(partitionsByKey).hasSize(5);
        partitionsByKey.values().forEach(partitions -> assertThat(partitions).hasSize(1));

        // The partition split the job reports is exactly what the broker holds.
        Map<String, Long> actual = new HashMap<>();
        mine.forEach(r -> actual.merge(String.valueOf(r.partition()), 1L, Long::sum));
        @SuppressWarnings("unchecked")
        Map<String, Number> reported = (Map<String, Number>) job.get("partitionCounts");
        assertThat(reported).hasSameSizeAs(actual);
        actual.forEach((p, n) -> assertThat(reported.get(p).longValue()).isEqualTo(n));

        // ...and the audit record is in the producer's own database.
        assertThat(mongo.findById(job.get("id"), PublishJob.class)).isNotNull();
    }

    @Test
    void pastedEventsAreWrittenVerbatimIncludingAKeylessOne() {
        post(Map.of("mode", "PASTE", "events", List.of(
                Map.of("key", "inv-1", "value", "{\"sku\":\"A-100\"}"),
                Map.of("key", "inv-2", "value", "{\"sku\":\"B-200\"}"),
                Map.of("value", "keyless-event-marker"))));

        List<ConsumerRecord<String, String>> onTopic = readTopic(3);
        assertThat(onTopic).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("inv-1");
            assertThat(r.value()).isEqualTo("{\"sku\":\"A-100\"}");
        });
        assertThat(onTopic).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("inv-2");
            assertThat(r.value()).isEqualTo("{\"sku\":\"B-200\"}");
        });
        assertThat(onTopic).anySatisfy(r -> {
            assertThat(r.key()).isNull();
            assertThat(r.value()).isEqualTo("keyless-event-marker");
        });
    }

    @Test
    void theJobsEndpointListsAuditRecordsNewestFirst() {
        post(Map.of("mode", "GENERATE", "count", 3, "keyStrategy", "NONE", "valuePrefix", "first"));
        Map<String, Object> second = post(Map.of("mode", "GENERATE", "count", 4, "keyStrategy", "NONE", "valuePrefix", "second"));

        List<Map<String, Object>> jobs = http().get().uri("/api/jobs?limit=2").retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
        assertThat(jobs).hasSize(2);
        assertThat(jobs.get(0).get("id")).isEqualTo(second.get("id"));
        assertThat(jobs.get(0).get("requested")).isEqualTo(4);
    }

    @Test
    void theTopicIsCreatedByTheServiceWithTheConfiguredPartitionsAndDurabilitySettings() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            var description = admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC);
            assertThat(description.partitions()).hasSize(3);
            var config = admin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC)))
                    .all().get().values().iterator().next();
            assertThat(config.get("min.insync.replicas").value()).isEqualTo("1");
        }
    }

    @Test
    void theAuditCollectionExpiresOldRecords() {
        List<IndexInfo> indexes = mongo.indexOps(PublishJob.class).getIndexInfo();
        IndexInfo ttl = indexes.stream().filter(i -> i.getName().equals("ttl_createdAt")).findFirst().orElseThrow();
        assertThat(ttl.getExpireAfter()).contains(Duration.ofDays(30));
    }

    @Test
    void theConfigEndpointExposesWhatTheUiNeedsAndNoInfrastructureDetail() {
        Map<String, Object> config = http().get().uri("/api/config").retrieve().body(new ParameterizedTypeReference<>() {
        });
        assertThat(config).containsEntry("topic", TOPIC).containsEntry("partitions", 3).containsKey("maxBulkEvents");
        assertThat(config).doesNotContainKeys("bootstrapServers", "consumerGroup");
    }

    @Test
    void invalidRequestsAreRejectedWith400() {
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 0))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 60000))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE"))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "PASTE", "events", List.of()))).isEqualTo(400);
        assertThat(statusOf(Map.of("count", 5))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 1, "valuePrefix", "a\"},{\"x"))).isEqualTo(400);
        assertThat(statusOf(Map.of("mode", "GENERATE", "count", 1, "keyPrefix", "bad key!"))).isEqualTo(400);
    }

    @Test
    void frameworkErrorsKeepTheirRealStatusCodesInsteadOfBecoming500() {
        assertThat(http().get().uri("/api/nope").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(404);
        assertThat(http().put().uri("/api/publish/bulk").exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(405);
        assertThat(http().post().uri("/api/publish/bulk").header("Content-Type", "text/plain").body("x")
                .exchange((req, res) -> res.getStatusCode()).value()).isEqualTo(415);
    }

    @Test
    void oversizedBodiesAreRejectedBeforeBeingParsed() {
        String huge = "x".repeat(9 * 1024 * 1024);
        int status = http().post().uri("/api/publish/bulk").header("Content-Type", "application/json")
                .body("{\"mode\":\"PASTE\",\"pad\":\"" + huge + "\"}")
                .exchange((req, res) -> res.getStatusCode()).value();
        assertThat(status).isEqualTo(413);
    }
}
