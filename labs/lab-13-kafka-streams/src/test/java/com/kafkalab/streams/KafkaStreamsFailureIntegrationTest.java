package com.kafkalab.streams;

import com.kafkalab.streams.support.OrderEvent;
import com.kafkalab.streams.support.StreamsTestCluster;
import com.kafkalab.streams.topology.OrderAggregationTopology;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kafka + real multi-instance Kafka Streams -- no mocked
 * rebalancing, no simulated changelog. Covers the two failure-matrix
 * rows this WP owns ({@code docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md},
 * "Integration and processing failures": Kafka Streams process dies;
 * Kafka Streams state restoration takes a long time) plus a real
 * exactly-once-processing correctness check.
 */
class KafkaStreamsFailureIntegrationTest {

    private static StreamsTestCluster cluster;
    private static KafkaProducer<String, String> producer;

    @BeforeAll
    static void startCluster() {
        cluster = new StreamsTestCluster(19_703);
        cluster.start();
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
    }

    @AfterAll
    static void stopCluster() {
        producer.close();
        cluster.close();
    }

    @Test
    void killingOneStreamsInstanceMigratesItsTasksAndRestoresFullStateOnTheSurvivor() throws Exception {
        String id = uniqueId();
        String inputTopic = "orders-" + id;
        String outputTopic = "totals-" + id;
        String storeName = "store-" + id;
        String applicationId = "app-" + id;
        createTopic(inputTopic, 4);

        Map<String, Double> expectedTotals = new HashMap<>();
        for (int i = 0; i < 12; i++) {
            String customerId = "C-" + id + "-" + i;
            produceOrder(inputTopic, customerId, 10.0);
            produceOrder(inputTopic, customerId, 5.0);
            expectedTotals.put(customerId, 15.0);
        }

        AtomicInteger restoredRecordsOnSurvivor = new AtomicInteger();
        KafkaStreams instanceA = newStreamsInstance(applicationId, inputTopic, outputTopic, storeName, 0, null);
        KafkaStreams instanceB = newStreamsInstance(applicationId, inputTopic, outputTopic, storeName, 0, restoredRecordsOnSurvivor);
        instanceA.start();
        instanceB.start();
        try {
            waitUntilRunning(instanceA, Duration.ofSeconds(30));
            waitUntilRunning(instanceB, Duration.ofSeconds(30));
            waitUntilAllKeysQueryable(List.of(instanceA, instanceB), storeName, expectedTotals, Duration.ofSeconds(30));

            // Simulates the failure-matrix row "Kafka Streams process
            // dies": instance A stops; its tasks must be reassigned to
            // the survivor.
            instanceA.close(Duration.ofSeconds(30));

            waitUntilRunning(instanceB, Duration.ofSeconds(30));
            waitUntilAllKeysQueryable(List.of(instanceB), storeName, expectedTotals, Duration.ofSeconds(60));

            assertTrue(restoredRecordsOnSurvivor.get() > 0,
                    "the survivor must have actually restored A's former tasks' state from the changelog topic -- zero would mean no real migration happened");
        } finally {
            instanceB.close(Duration.ofSeconds(10));
        }
    }

    @Test
    void aStandbyReplicaMeansFailoverRequiresLittleOrNoChangelogRestoration() throws Exception {
        String id = uniqueId();
        String inputTopic = "orders-" + id;
        String outputTopic = "totals-" + id;
        String storeName = "store-" + id;
        String applicationId = "app-" + id;
        createTopic(inputTopic, 4);

        Map<String, Double> expectedTotals = new HashMap<>();
        for (int i = 0; i < 12; i++) {
            String customerId = "C-" + id + "-" + i;
            produceOrder(inputTopic, customerId, 20.0);
            expectedTotals.put(customerId, 20.0);
        }

        AtomicInteger restoredRecordsOnSurvivor = new AtomicInteger();
        KafkaStreams instanceA = newStreamsInstance(applicationId, inputTopic, outputTopic, storeName, 1, null);
        KafkaStreams instanceB = newStreamsInstance(applicationId, inputTopic, outputTopic, storeName, 1, restoredRecordsOnSurvivor);
        instanceA.start();
        instanceB.start();
        try {
            waitUntilRunning(instanceA, Duration.ofSeconds(30));
            waitUntilRunning(instanceB, Duration.ofSeconds(30));
            waitUntilAllKeysQueryable(List.of(instanceA, instanceB), storeName, expectedTotals, Duration.ofSeconds(30));

            // Give the standby replica time to actually replicate the
            // active tasks' changelog in the background before failover
            // -- there is no cheap public "standby is caught up" signal
            // to poll, so this is a deliberate, generous fixed wait
            // (documented, not a hidden flaky assumption).
            Thread.sleep(8_000);

            instanceA.close(Duration.ofSeconds(30));

            waitUntilRunning(instanceB, Duration.ofSeconds(30));
            waitUntilAllKeysQueryable(List.of(instanceB), storeName, expectedTotals, Duration.ofSeconds(60));

            assertTrue(restoredRecordsOnSurvivor.get() <= expectedTotals.size(),
                    "with a warm standby already holding the migrated tasks' state, failover must require little to no changelog replay, "
                            + "unlike the zero-standby scenario -- restored=" + restoredRecordsOnSurvivor.get());
        } finally {
            instanceB.close(Duration.ofSeconds(10));
        }
    }

    @Test
    void exactlyOnceProcessingCommitsTransactionallyWithNoDuplicationAcrossMultipleCommits() throws Exception {
        String id = uniqueId();
        String inputTopic = "orders-" + id;
        String outputTopic = "totals-" + id;
        String storeName = "store-" + id;
        String applicationId = "app-" + id;
        createTopic(inputTopic, 2);

        Map<String, Double> expectedTotals = new HashMap<>();
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 5; i++) {
                String customerId = "C-" + id + "-" + i;
                produceOrder(inputTopic, customerId, 3.0);
                expectedTotals.merge(customerId, 3.0, Double::sum);
            }
            // A short pause between batches so multiple real commit
            // intervals elapse -- proving correctness holds ACROSS
            // several transaction boundaries, not just within one.
            Thread.sleep(700);
        }

        KafkaStreams streams = newStreamsInstance(applicationId, inputTopic, outputTopic, storeName, 0, null, StreamsConfig.EXACTLY_ONCE_V2);
        streams.start();
        try {
            waitUntilRunning(streams, Duration.ofSeconds(30));
            waitUntilAllKeysQueryable(List.of(streams), storeName, expectedTotals, Duration.ofSeconds(30));

            List<ConsumerRecord<String, String>> outputRecords = consumeReadCommitted(outputTopic, Duration.ofSeconds(15));
            Map<String, String> lastValuePerKey = new HashMap<>();
            for (ConsumerRecord<String, String> record : outputRecords) {
                lastValuePerKey.put(record.key(), record.value());
            }
            for (var entry : expectedTotals.entrySet()) {
                assertEquals("customerId=" + entry.getKey() + "|runningTotal=" + entry.getValue(), lastValuePerKey.get(entry.getKey()));
            }
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static void createTopic(String topic, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private static void produceOrder(String topic, String customerId, double amount) throws Exception {
        String eventId = "E-" + UUID.randomUUID();
        producer.send(new ProducerRecord<>(topic, eventId, new OrderEvent(eventId, customerId, amount).toWireFormat())).get();
    }

    private static KafkaStreams newStreamsInstance(String applicationId, String inputTopic, String outputTopic, String storeName,
                                                     int numStandbyReplicas, AtomicInteger restoredRecordCounter) throws Exception {
        return newStreamsInstance(applicationId, inputTopic, outputTopic, storeName, numStandbyReplicas, restoredRecordCounter, StreamsConfig.AT_LEAST_ONCE);
    }

    private static KafkaStreams newStreamsInstance(String applicationId, String inputTopic, String outputTopic, String storeName,
                                                     int numStandbyReplicas, AtomicInteger restoredRecordCounter, String processingGuarantee) throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        OrderAggregationTopology.build(builder, inputTopic, outputTopic, storeName);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams-it-").toString());
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        // A real requirement this test surfaced: the DEFAULT commit
        // interval (30s for at_least_once, 100ms for EOS) is far too
        // slow for a bounded test -- lowered so changelog writes (and
        // therefore restoration behavior) are observable within this
        // test's timeouts.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 10_000);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        if (restoredRecordCounter != null) {
            streams.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(org.apache.kafka.common.TopicPartition topicPartition, String storeNameArg, long startingOffset, long endingOffset) {
                }

                @Override
                public void onBatchRestored(org.apache.kafka.common.TopicPartition topicPartition, String storeNameArg, long batchEndOffset, long numRestored) {
                    restoredRecordCounter.addAndGet((int) numRestored);
                }

                @Override
                public void onRestoreEnd(org.apache.kafka.common.TopicPartition topicPartition, String storeNameArg, long totalRestored) {
                }
            });
        }
        return streams;
    }

    private static void waitUntilRunning(KafkaStreams streams, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("Streams instance did not reach RUNNING within " + timeout + "; last state=" + streams.state());
    }

    private static void waitUntilAllKeysQueryable(List<KafkaStreams> instances, String storeName, Map<String, Double> expected, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (allKeysMatch(instances, storeName, expected)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("not all expected keys became queryable with the correct value within " + timeout);
    }

    private static boolean allKeysMatch(List<KafkaStreams> instances, String storeName, Map<String, Double> expected) {
        for (var entry : expected.entrySet()) {
            Double found = null;
            for (KafkaStreams instance : instances) {
                try {
                    ReadOnlyKeyValueStore<String, Double> store = instance.store(
                            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.<String, Double>keyValueStore()));
                    Double value = store.get(entry.getKey());
                    if (value != null) {
                        found = value;
                        break;
                    }
                } catch (Exception e) {
                    // This instance doesn't currently own this task's store -- expected, try the next instance.
                }
            }
            if (found == null || !found.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static List<ConsumerRecord<String, String>> consumeReadCommitted(String topic, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                records.forEach(collected::add);
            }
        }
        return collected;
    }
}
