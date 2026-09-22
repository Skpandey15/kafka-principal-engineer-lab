package com.kafkalab.observability;

import com.kafkalab.observability.support.LabConfig;
import com.kafkalab.observability.support.LagInspector;
import com.kafkalab.observability.support.PrometheusQueryClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real integration tests against the ALREADY-RUNNING
 * {@code platform/kafka-cluster/} (with its JMX exporter agents) and
 * {@code platform/observability/} (Prometheus + kafka-exporter +
 * Grafana) environments -- see the README, "Prerequisites" and "Why
 * not Testcontainers here," for why this WP's tests deliberately
 * exercise the real, persistent stack rather than an ephemeral one.
 */
class ObservabilityIntegrationTest {

    private static Admin admin;
    private static KafkaProducer<String, String> producer;
    private static PrometheusQueryClient prometheus;

    @BeforeAll
    static void setUp() {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers()));
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
        prometheus = new PrometheusQueryClient(LabConfig.prometheusUrl());
    }

    @AfterAll
    static void tearDown() {
        producer.close();
        admin.close();
    }

    @Test
    void nativeAdminApiLagMatchesThePrometheusLagMetric() throws Exception {
        String id = uniqueId();
        String topic = "obs-lag-" + id;
        String groupId = "obs-lag-group-" + id;
        // A single partition, deliberately -- keeps "consumed exactly
        // 4 of 10" unambiguous. A multi-partition topic's first poll()
        // can return records from every partition at once, making "stop
        // after 4" a race against however many arrived in that one
        // batch, not a controlled position.
        createTopic(topic, 1, Map.of());
        TopicPartition partition = new TopicPartition(topic, 0);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)).get();
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long consumedOffset = -1;
            Instant deadline = Instant.now().plusSeconds(20);
            while (consumedOffset < 3 && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    consumedOffset = record.offset();
                    if (consumedOffset == 3) {
                        break;
                    }
                }
            }
            // Exactly offsets 0-3 (4 records) committed -- 6 remain.
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(consumedOffset + 1)));
        }

        long nativeLag = LagInspector.totalLag(admin, groupId, topic);
        assertEquals(6, nativeLag, "native Admin-API lag computation must show 6 unconsumed records");

        // kafka-exporter polls the Admin API itself on its own interval,
        // and Prometheus scrapes kafka-exporter on ITS OWN interval --
        // real, observable end-to-end latency through the actual
        // pipeline, not an instant reflection.
        waitUntil(() -> {
            try {
                double promLag = prometheus.queryScalarSum(
                        "kafka_consumergroup_lag{consumergroup=\"" + groupId + "\",topic=\"" + topic + "\"}");
                return promLag == 6.0;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(45));
    }

    @Test
    void sustainedConsumerLagGrowsUnboundedWithNoProducerBackpressure() throws Exception {
        String id = uniqueId();
        String topic = "obs-sustained-lag-" + id;
        String groupId = "obs-sustained-group-" + id;
        createTopic(topic, 1, Map.of());

        // A real consumer group that commits its starting position but
        // never actually consumes -- models a consumer that's
        // permanently stuck (a slow downstream dependency, e.g.).
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId))) {
            consumer.subscribe(List.of(topic));
            consumer.poll(Duration.ofMillis(500));
            consumer.commitSync();
        }

        for (int i = 0; i < 20; i++) {
            producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)).get();
        }
        long lagAfterFirstBatch = LagInspector.totalLag(admin, groupId, topic);

        // Kafka has no built-in backpressure toward producers based on
        // lag -- every one of these sends must succeed exactly like the
        // first batch did, with the SAME consumer group still never
        // having consumed anything.
        for (int i = 20; i < 60; i++) {
            producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)).get();
        }
        long lagAfterSecondBatch = LagInspector.totalLag(admin, groupId, topic);

        assertEquals(20, lagAfterFirstBatch);
        assertEquals(60, lagAfterSecondBatch, "lag must grow by exactly the second batch's size -- no record was throttled, rejected, or dropped by Kafka itself");
        assertTrue(lagAfterSecondBatch > lagAfterFirstBatch, "lag must keep growing, unbounded, exactly as the failure matrix names for this WP");
    }

    @Test
    void lagExceedingRetentionCausesRealDataLossForTheLaggingGroup() throws Exception {
        String id = uniqueId();
        String topic = "obs-retention-loss-" + id;
        String groupId = "obs-retention-group-" + id;
        // A deliberately tiny retention/segment window -- lab-speed
        // only (paired with the broker's own lowered
        // log.retention.check.interval.ms, see platform/kafka-cluster/docker-compose.yml)
        // so real segment deletion is observable within a bounded test.
        createTopic(topic, 1, Map.of(
                "retention.ms", "3000",
                "segment.ms", "3000"));

        for (int i = 0; i < 30; i++) {
            producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)).get();
        }

        // Commit offset 0 WITHOUT ever consuming -- models a consumer
        // group that fell behind from the very start and never caught
        // up before retention caught up to it.
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId))) {
            consumer.assign(List.of(partition));
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(0)));
        }

        // Real, bounded wait for the broker to actually delete the
        // rolled-over segment -- confirmed by the partition's earliest
        // available offset moving past 0.
        waitUntil(() -> {
            try {
                long earliest = admin.listOffsets(Map.of(partition, OffsetSpec.earliest()))
                        .all().get().get(partition).offset();
                return earliest > 0;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(40));

        // By now every one of the original 30 records may ALREADY be
        // gone (retention.ms=3000 deletes an entire rolled segment at
        // once, not record-by-record) -- produce a few fresh records,
        // still safely within the retention window, so the consumer has
        // real data to land on once it resets past the deleted ones.
        for (int i = 30; i < 35; i++) {
            producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)).get();
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId))) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, String> first = null;
            Instant deadline = Instant.now().plusSeconds(20);
            while (first == null && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                if (!records.isEmpty()) {
                    first = records.iterator().next();
                }
            }
            assertTrue(first != null && first.offset() > 0,
                    "the consumer's committed position (0) must no longer exist -- retention deleted it before the group ever consumed it, "
                            + "an ACTUAL data-loss path, not just a delay (the exact language this WP's failure-matrix row uses)");
        }
    }

    @Test
    void aHotPartitionShowsRealUnevenPerPartitionThroughput() throws Exception {
        String id = uniqueId();
        String topic = "obs-hot-partition-" + id;
        createTopic(topic, 3, Map.of());

        // Every record explicitly keyed to partition 0 -- a real,
        // deterministic hot-partition scenario (WP-04's own
        // partitioning concepts, viewed through an observability lens).
        for (int i = 0; i < 30; i++) {
            producer.send(new ProducerRecord<>(topic, 0, "hot-key", "v" + i)).get();
        }

        Map<TopicPartition, Long> endOffsets = admin.listOffsets(Map.of(
                        new TopicPartition(topic, 0), OffsetSpec.latest(),
                        new TopicPartition(topic, 1), OffsetSpec.latest(),
                        new TopicPartition(topic, 2), OffsetSpec.latest()))
                .all().get().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

        assertEquals(30L, endOffsets.get(new TopicPartition(topic, 0)));
        assertEquals(0L, endOffsets.get(new TopicPartition(topic, 1)));
        assertEquals(0L, endOffsets.get(new TopicPartition(topic, 2)));

        // The SAME skew, confirmed via the real observability pipeline,
        // not just the native Admin API -- kafka_topic_partition_current_offset
        // is scraped from kafka-exporter, exactly what a real Grafana
        // panel would show an operator diagnosing this.
        waitUntil(() -> {
            try {
                double p0 = prometheus.queryScalarSum("kafka_topic_partition_current_offset{topic=\"" + topic + "\",partition=\"0\"}");
                double p1 = prometheus.queryScalarSum("kafka_topic_partition_current_offset{topic=\"" + topic + "\",partition=\"1\"}");
                return p0 == 30.0 && p1 == 0.0;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(30));
    }

    @Test
    void brokerJmxMetricsReflectRealProducedThroughput() throws Exception {
        String id = uniqueId();
        String topic = "obs-throughput-" + id;
        createTopic(topic, 1, Map.of());

        double bytesInBefore = prometheus.queryScalarSum("kafka_server_brokertopicmetrics_bytesin_total{topic=\"" + topic + "\"}");

        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<>(topic, "k" + i, "a-reasonably-sized-value-" + i)).get();
        }

        waitUntil(() -> {
            try {
                double bytesInAfter = prometheus.queryScalarSum("kafka_server_brokertopicmetrics_bytesin_total{topic=\"" + topic + "\"}");
                return bytesInAfter > bytesInBefore;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(30));
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static void createTopic(String topic, int partitions, Map<String, String> configs) throws Exception {
        NewTopic newTopic = new NewTopic(topic, partitions, (short) 3).configs(configs);
        admin.createTopics(List.of(newTopic)).all().get();
    }

    private static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
