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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.producer.contract.ContractUnavailableException;
import com.kafkalab.eventconsole.producer.contract.EventContract;
import com.kafkalab.eventconsole.producer.model.PublishJob;
import com.kafkalab.eventconsole.producer.publish.PublishJobReaper;
import com.kafkalab.eventconsole.producer.repo.PublishJobRepository;
import com.kafkalab.eventconsole.producer.testsupport.FakeSchemaRegistry;

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

    /** The contract registry, over real HTTP: schema id 7 is the latest version (3) of the subject. */
    static final FakeSchemaRegistry REGISTRY = new FakeSchemaRegistry(7, 3, FakeSchemaRegistry.V1);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole_producer"));
        registry.add("app.topic", () -> TOPIC);
        registry.add("app.schema.registry-url", REGISTRY::url);
    }

    @LocalServerPort
    int port;

    @Autowired
    MongoTemplate mongo;

    @MockitoSpyBean
    EventContract contract;

    @MockitoSpyBean
    PublishJobRepository jobRepository;

    @Autowired
    PublishJobReaper reaper;

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

    /**
     * How many records the topic holds, from the broker's end offsets. Exact and immediate -- unlike
     * polling, which can return "nothing" simply because the consumer has not been assigned yet.
     */
    private long topicSize() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var partitions = consumer.partitionsFor(TOPIC).stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(TOPIC, p.partition())).toList();
            return consumer.endOffsets(partitions).values().stream().mapToLong(Long::longValue).sum();
        }
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
                Map.of("key", "inv-1", "value", "{\"id\":\"i1\",\"batch\":\"A-100\"}"),
                Map.of("key", "inv-2", "value", "{\"id\":\"i2\",\"batch\":\"B-200\"}"),
                Map.of("value", "{\"id\":\"keyless-event-marker\"}"))));

        List<ConsumerRecord<String, String>> onTopic = readTopic(3);
        assertThat(onTopic).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("inv-1");
            assertThat(r.value()).isEqualTo("{\"id\":\"i1\",\"batch\":\"A-100\"}");
        });
        assertThat(onTopic).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("inv-2");
            assertThat(r.value()).isEqualTo("{\"id\":\"i2\",\"batch\":\"B-200\"}");
        });
        assertThat(onTopic).anySatisfy(r -> {
            assertThat(r.key()).isNull();
            assertThat(r.value()).isEqualTo("{\"id\":\"keyless-event-marker\"}");
        });
    }

    @Test
    void everyRecordOnTheTopicCarriesTheSchemaIdItWasCheckedAgainstAndTheAuditSaysWhich() {
        Map<String, Object> job = post(Map.of("mode", "GENERATE", "count", 20, "keyStrategy", "UNIQUE", "keyPrefix", "hdr-"));

        List<ConsumerRecord<String, String>> mine = readTopic(20).stream()
                .filter(r -> r.key() != null && r.key().startsWith("hdr-")).toList();
        assertThat(mine).hasSize(20).allSatisfy(r ->
                assertThat(new String(r.headers().lastHeader("x-schema-id").value())).isEqualTo("7"));
        assertThat(job).containsEntry("schemaId", 7).containsEntry("schemaVersion", 3);
    }

    @Test
    void oneEventThatBreaksTheContractGetsTheWholeRequestRejectedWith422AndNothingIsPublished() {
        long before = topicSize();

        Map<String, Object> rejection = http().post().uri("/api/publish/bulk").body(Map.of("mode", "PASTE", "events", List.of(
                        Map.of("key", "good-1", "value", "{\"id\":\"g1\"}"),
                        Map.of("key", "bad-1", "value", "{\"seq\":-3}"),
                        Map.of("key", "bad-2", "value", "plain text"))))
                .exchange((req, res) -> {
                    assertThat(res.getStatusCode().value()).isEqualTo(422);
                    return res.bodyTo(new ParameterizedTypeReference<Map<String, Object>>() {
                    });
                });

        assertThat((String) rejection.get("error")).contains("2 event(s)").contains("nothing was published");
        assertThat(rejection).containsEntry("total", 2);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> violations = (List<Map<String, Object>>) rejection.get("violations");
        assertThat(violations).extracting(v -> v.get("index")).containsExactly(1, 2);
        assertThat(violations.get(0)).containsEntry("key", "bad-1");
        assertThat((String) violations.get(0).get("reason")).contains("id");
        assertThat((String) violations.get(1).get("reason")).startsWith("value is not valid JSON");

        // Not even the good event went out.
        assertThat(topicSize()).isEqualTo(before);
    }

    @Test
    void whenNoContractIsAvailableThePublishIsRefusedWith503AndNothingGoesOut() {
        long before = topicSize();
        org.mockito.Mockito.doThrow(new ContractUnavailableException("registry down")).when(contract).current();

        int status = statusOf(Map.of("mode", "GENERATE", "count", 5));

        assertThat(status).isEqualTo(503);
        assertThat(topicSize()).isEqualTo(before);
    }

    // --- the audit record: written first, replaced by the outcome, honest when MongoDB fails -------------

    @Test
    void aPublishLeavesOneAuditRowThatEndsCompleted() {
        Map<String, Object> job = post(Map.of("mode", "GENERATE", "count", 6, "keyStrategy", "NONE"));

        assertThat(job).containsEntry("status", "COMPLETED");
        PublishJob stored = mongo.findById(job.get("id"), PublishJob.class);
        assertThat(stored.status()).isEqualTo(PublishJob.COMPLETED);
        assertThat(stored.acked()).isEqualTo(6);
        assertThat(mongo.count(org.springframework.data.mongodb.core.query.Query.query(
                org.springframework.data.mongodb.core.query.Criteria.where("_id").is(job.get("id"))), PublishJob.class))
                .as("one row, replaced - not two").isEqualTo(1);
    }

    @Test
    void ifTheAuditStoreIsDownBeforeThePublishNothingIsPublishedAndTheCallerIsToldToRetry() {
        long before = topicSize();
        org.mockito.Mockito.doThrow(new org.springframework.dao.DataAccessResourceFailureException("simulated: mongo down"))
                .when(jobRepository).save(org.mockito.ArgumentMatchers.any(PublishJob.class));

        Map<String, Object> refusal = http().post().uri("/api/publish/bulk").body(Map.of("mode", "GENERATE", "count", 5))
                .exchange((req, res) -> {
                    assertThat(res.getStatusCode().value()).isEqualTo(503);
                    return res.bodyTo(new ParameterizedTypeReference<Map<String, Object>>() {
                    });
                });

        assertThat((String) refusal.get("error")).contains("nothing was published").contains("safe to retry");
        assertThat(topicSize()).as("not a single event reached Kafka").isEqualTo(before);
    }

    @Test
    void ifTheOutcomeCannotBeRecordedTheEventsStandTheCallerIsToldTheTruthAndTheRowIsLaterLabelledInterrupted() {
        long before = topicSize();
        java.util.concurrent.atomic.AtomicInteger saves = new java.util.concurrent.atomic.AtomicInteger();
        org.mockito.Mockito.doAnswer(invocation -> {
            if (saves.incrementAndGet() >= 2) {
                throw new org.springframework.dao.DataAccessResourceFailureException("simulated: mongo went away mid-publish");
            }
            // A repository is an interface proxy, so there is no real method to call: write it ourselves.
            return mongo.save(invocation.<PublishJob>getArgument(0));
        }).when(jobRepository).save(org.mockito.ArgumentMatchers.any(PublishJob.class));

        Map<String, Object> job = post(Map.of("mode", "GENERATE", "count", 9, "keyStrategy", "NONE"));

        // 200, not an error: the events were written, and an error would invite a retry that writes them again.
        assertThat(job).containsEntry("status", "COMPLETED_UNRECORDED").containsEntry("acked", 9);
        assertThat(topicSize() - before).isEqualTo(9);
        // The database kept the write-ahead row: started, outcome unknown.
        PublishJob stored = mongo.findById(job.get("id"), PublishJob.class);
        assertThat(stored.status()).isEqualTo(PublishJob.STARTED);
        assertThat(stored.requested()).isEqualTo(9);

        // After the grace period nobody can mistake it for a publish still in progress.
        assertThat(reaper.sweep(java.time.Instant.now().plus(java.time.Duration.ofHours(1)))).isGreaterThanOrEqualTo(1);
        assertThat(mongo.findById(job.get("id"), PublishJob.class).status()).isEqualTo(PublishJob.INTERRUPTED);
    }

    @Test
    void theReaperOnlyLabelsOldUnfinishedJobsAndLeavesEverythingElseAlone() {
        java.time.Instant now = java.time.Instant.now();
        mongo.insert(job("reap-old-started", now.minus(java.time.Duration.ofHours(1)), PublishJob.STARTED));
        mongo.insert(job("reap-fresh-started", now.minusSeconds(5), PublishJob.STARTED));
        mongo.insert(job("reap-old-completed", now.minus(java.time.Duration.ofHours(1)), PublishJob.COMPLETED));

        long marked = reaper.sweep(now);

        assertThat(marked).isEqualTo(1);
        assertThat(mongo.findById("reap-old-started", PublishJob.class).status()).isEqualTo(PublishJob.INTERRUPTED);
        assertThat(mongo.findById("reap-fresh-started", PublishJob.class).status()).isEqualTo(PublishJob.STARTED);
        assertThat(mongo.findById("reap-old-completed", PublishJob.class).status()).isEqualTo(PublishJob.COMPLETED);
        assertThat(reaper.sweep(now)).as("idempotent").isZero();
    }

    private static PublishJob job(String id, java.time.Instant createdAt, String status) {
        return new PublishJob(id, createdAt, TOPIC, "GENERATE", "NONE", 1, 0, 0, 0, 1, 1, Map.of(), null, status);
    }

    @Test
    void thePrometheusEndpointExposesThePublishAndContractCounters() {
        post(Map.of("mode", "GENERATE", "count", 3, "keyStrategy", "NONE"));
        statusOf(Map.of("mode", "PASTE", "events", List.of(Map.of("value", "not json"))));

        String body = http().get().uri("/actuator/prometheus").retrieve().body(String.class);

        assertThat(body).containsPattern("eventconsole_publish_acked_total\\{[^}]*\\} [0-9.]+")
                .containsPattern("eventconsole_publish_rejected_total\\{[^}]*\\} [1-9][0-9.]*")
                .contains("application=\"event-console-producer\"");
    }

    @Test
    void theConfigEndpointShowsWhichContractIsInForce() {
        Map<String, Object> config = http().get().uri("/api/config").retrieve().body(new ParameterizedTypeReference<>() {
        });
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = (Map<String, Object>) config.get("contract");
        assertThat(schema).containsEntry("subject", "event-console-value").containsEntry("available", true)
                .containsEntry("version", 3).containsEntry("schemaId", 7);
        assertThat(config).doesNotContainKey("registryUrl");
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
