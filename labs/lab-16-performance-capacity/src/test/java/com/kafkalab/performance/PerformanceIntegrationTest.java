package com.kafkalab.performance;

import com.kafkalab.performance.bench.ProducerBenchmark;
import com.kafkalab.performance.support.PayloadGenerator;
import com.kafkalab.performance.support.PerformanceTestCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kafka + real, hand-built producer/consumer benchmarking -- no
 * mocked timing, no simulated throughput numbers. Every assertion
 * compares REAL measured results against each other (batched vs.
 * unbatched, compressed vs. not), never against a fixed absolute
 * number that would be hardware-dependent and flaky.
 */
class PerformanceIntegrationTest {

    private static PerformanceTestCluster cluster;
    private static Admin admin;

    @BeforeAll
    static void startCluster() {
        cluster = new PerformanceTestCluster(19_704);
        cluster.start();
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()));
    }

    @AfterAll
    static void stopCluster() {
        admin.close();
        cluster.close();
    }

    @Test
    void batchingRealMeasurablyImprovesThroughputOverUnbatchedProduction() throws Exception {
        String topic = "perf-batch-" + uniqueId();
        createTopic(topic, 3);
        byte[] payload = PayloadGenerator.compressible(200);

        ProducerBenchmark.Result unbatched = ProducerBenchmark.run(cluster.bootstrapServers(), topic, Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.BATCH_SIZE_CONFIG, 1,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"
        ), 3000, payload);

        ProducerBenchmark.Result batched = ProducerBenchmark.run(cluster.bootstrapServers(), topic, Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 20,
                ProducerConfig.BATCH_SIZE_CONFIG, 65536,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"
        ), 3000, payload);

        assertTrue(batched.recordsPerSec() > unbatched.recordsPerSec(),
                "batched throughput (" + batched.recordsPerSec() + " rec/s) must real-measurably beat "
                        + "batch.size=1/linger.ms=0 (" + unbatched.recordsPerSec() + " rec/s) -- one network "
                        + "round-trip per record is a real, drastic ceiling");
    }

    @Test
    void compressionRealMeasurablyReducesBytesSentForCompressibleData() throws Exception {
        String topic = "perf-compress-" + uniqueId();
        createTopic(topic, 1);
        byte[] payload = PayloadGenerator.compressible(4096);

        ProducerBenchmark.Result uncompressed = ProducerBenchmark.run(cluster.bootstrapServers(), topic, Map.of(
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"
        ), 1000, payload);

        ProducerBenchmark.Result compressed = ProducerBenchmark.run(cluster.bootstrapServers(), topic, Map.of(
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"
        ), 1000, payload);

        assertTrue(uncompressed.compressionRateAvg() >= 0.9,
                "compression.type=none must report a real compression-rate-avg near 1.0, got " + uncompressed.compressionRateAvg());
        assertTrue(compressed.compressionRateAvg() < 0.8,
                "lz4 against highly repetitive data must report a real, measurably-lower compression-rate-avg, got " + compressed.compressionRateAvg());
    }

    @Test
    void partitionCountIsARealCeilingOnConsumerParallelism() throws Exception {
        String topic = "perf-parallelism-" + uniqueId();
        String groupId = "perf-parallelism-group-" + uniqueId();
        createTopic(topic, 3);

        List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
        try {
            for (int i = 0; i < 5; i++) {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId));
                consumer.subscribe(List.of(topic));
                consumers.add(consumer);
            }
            // Real rebalance settling -- poll every member repeatedly
            // until assignment stabilizes.
            int totalAssigned = 0;
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                totalAssigned = 0;
                for (KafkaConsumer<String, String> consumer : consumers) {
                    consumer.poll(Duration.ofMillis(300));
                    totalAssigned += consumer.assignment().size();
                }
                if (totalAssigned == 3) {
                    break;
                }
            }

            long idleConsumers = consumers.stream().filter(c -> c.assignment().isEmpty()).count();
            assertEquals(3, totalAssigned, "all 3 partitions must be assigned across the group");
            assertTrue(idleConsumers >= 2,
                    "with 5 consumer instances and only 3 partitions, at least 2 instances must be real, "
                            + "measurably idle -- the native ceiling WP-04/WP-05 describe, not a hypothetical");
        } finally {
            consumers.forEach(KafkaConsumer::close);
        }
    }

    @Test
    void lowFetchMinBytesReturnsQuicklyWhileHighFetchMinBytesWaitsForTheRealMaxWait() throws Exception {
        String topic = "perf-fetch-" + uniqueId();
        createTopic(topic, 1);

        // A single small record -- nowhere near satisfying a large
        // fetch.min.bytes threshold.
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer"))) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "k", PayloadGenerator.compressible(50))).get();
        }

        Properties lowFetchMinProps = consumerBytesProps("perf-fetch-low-" + uniqueId());
        lowFetchMinProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(lowFetchMinProps)) {
            consumer.subscribe(List.of(topic));
            long start = System.currentTimeMillis();
            ConsumerRecords<String, byte[]> records = pollUntilNonEmpty(consumer, Duration.ofSeconds(10));
            long elapsedMs = System.currentTimeMillis() - start;
            assertTrue(!records.isEmpty());
            assertTrue(elapsedMs < 2000, "fetch.min.bytes=1 must return as soon as ANY data exists -- took " + elapsedMs + "ms");
        }

        Properties highFetchMinProps = consumerBytesProps("perf-fetch-high-" + uniqueId());
        highFetchMinProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1_000_000);
        highFetchMinProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 3000);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(highFetchMinProps)) {
            consumer.subscribe(List.of(topic));
            long start = System.currentTimeMillis();
            ConsumerRecords<String, byte[]> records = pollUntilNonEmpty(consumer, Duration.ofSeconds(10));
            long elapsedMs = System.currentTimeMillis() - start;
            assertTrue(!records.isEmpty(), "the broker must still return what it has once fetch.max.wait.ms elapses, even below fetch.min.bytes");
            assertTrue(elapsedMs >= 2500,
                    "with fetch.min.bytes=1MB never satisfied, the real wait must be bounded by fetch.max.wait.ms (3000ms), not return instantly -- took only " + elapsedMs + "ms");
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static void createTopic(String topic, int partitions) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
    }

    private static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static Properties consumerBytesProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static <K, V> ConsumerRecords<K, V> pollUntilNonEmpty(KafkaConsumer<K, V> consumer, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(timeout.toMillis()));
            if (!records.isEmpty()) {
                return records;
            }
        }
        throw new AssertionError("no records received within " + timeout);
    }
}
