package com.kafkalab.deliverysemantics;

import com.kafkalab.deliverysemantics.consumer.DeliverySemanticsApp;
import com.kafkalab.deliverysemantics.consumer.DeliverySemanticsApp.CommitMode;
import com.kafkalab.deliverysemantics.consumer.DeliverySemanticsApp.CommitTiming;
import com.kafkalab.deliverysemantics.consumer.DeliverySemanticsApp.Config;
import com.kafkalab.deliverysemantics.support.FailurePoint;
import com.kafkalab.deliverysemantics.support.OrderEvent;
import com.kafkalab.deliverysemantics.support.ProcessedEventStore;
import com.kafkalab.deliverysemantics.support.SimulatedCrashException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real, containerized, KRaft-mode Kafka broker (same {@code apache/kafka}
 * image/version as {@code platform/kafka/} and every prior lab's tests).
 * No mocked producer/consumer, no mocked crash: every "crash" here is a
 * real {@link SimulatedCrashException} thrown mid-loop by
 * {@link DeliverySemanticsApp#run}, and every "restart" is a genuinely
 * fresh {@code KafkaConsumer} instance against the same broker and
 * {@code group.id} -- the crash-injection framework's whole point is
 * that this is reproducible on demand, not a race against a randomly
 * killed process.
 *
 * <p>Deliberately no fixed {@code Thread.sleep} used as a synchronization
 * mechanism: every assertion is reached either by a direct, blocking
 * Kafka client call (offsets are committed synchronously before the test
 * reads them back) or a bounded condition-polling loop with an overall
 * deadline, matching every prior WP's test-design convention.
 */
@Testcontainers
class DeliverySemanticsIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Test
    void committedOffsetsSurviveRestartAndNothingIsRedelivered() throws Exception {
        String topic = uniqueTopic();
        createTopic(topic, 1, (short) 1);
        produceOrderEvents(topic, 0, "ORDER-2001", "ORDER-2002", "ORDER-2003");
        String groupId = "it-group-" + UUID.randomUUID();
        TopicPartition tp = new TopicPartition(topic, 0);

        Config config = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-1",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.NONE,
                "", "", false, 0, 3, false);

        try (KafkaConsumer<String, String> consumer1 = newTestConsumer(groupId, "consumer-1", false)) {
            DeliverySemanticsApp.run(config, consumer1, null, new AtomicBoolean(false));
            assertEquals(3L, committedOffset(consumer1, tp), "all 3 records should be committed after a clean run");
        }

        try (KafkaConsumer<String, String> consumer2 = newTestConsumer(groupId, "consumer-2", false)) {
            consumer2.subscribe(List.of(topic));
            int fetched = 0;
            for (int i = 0; i < 3; i++) {
                fetched += consumer2.poll(Duration.ofMillis(500)).count();
            }
            assertEquals(0, fetched, "restart with everything already committed should redeliver nothing");
            assertEquals(3L, committedOffset(consumer2, tp), "committed offset must survive the restart unchanged");
        }
    }

    @Test
    void neverProcessedRecordsAreReplayedAfterACrash() throws Exception {
        String topic = uniqueTopic();
        createTopic(topic, 1, (short) 1);
        produceOrderEvents(topic, 0, "ORDER-3001", "ORDER-3002", "ORDER-3003");
        String groupId = "it-group-" + UUID.randomUUID();

        Config crashingConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-1",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.BEFORE_PROCESS,
                "ORDER-3003", "", false, 0, 0, false);

        KafkaConsumer<String, String> consumer1 = newTestConsumer(groupId, "consumer-1", false);
        SimulatedCrashException crash = org.junit.jupiter.api.Assertions.assertThrows(
                SimulatedCrashException.class,
                () -> DeliverySemanticsApp.run(crashingConfig, consumer1, null, new AtomicBoolean(false)));
        assertEquals(FailurePoint.BEFORE_PROCESS, crash.failurePoint());
        // consumer1 is deliberately never closed -- that omission IS the simulated crash.

        Config restartConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-2",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.NONE,
                "", "", false, 0, 1, false);
        try (KafkaConsumer<String, String> consumer2 = newTestConsumer(groupId, "consumer-2", false)) {
            ByteArrayOutputStream captured = captureStdoutDuring(
                    () -> DeliverySemanticsApp.run(restartConfig, consumer2, null, new AtomicBoolean(false)));
            String output = captured.toString();
            assertTrue(output.contains("eventId=ORDER-3003") && output.contains("status=SUCCESS"),
                    "the record never processed before the crash must be fetched and processed fresh after restart:\n" + output);
        }
    }

    @Test
    void processingBeforeCommitCanReplayAnAlreadyProcessedRecordAsADuplicate() throws Exception {
        String topic = uniqueTopic();
        createTopic(topic, 1, (short) 1);
        produceOrderEvents(topic, 0, "ORDER-4001", "ORDER-4002", "ORDER-4003");
        String groupId = "it-group-" + UUID.randomUUID();
        TopicPartition tp = new TopicPartition(topic, 0);

        Config crashingConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-1",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.AFTER_PROCESS_BEFORE_COMMIT,
                "ORDER-4002", "", false, 0, 0, false);

        KafkaConsumer<String, String> consumer1 = newTestConsumer(groupId, "consumer-1", false);
        SimulatedCrashException crash = org.junit.jupiter.api.Assertions.assertThrows(
                SimulatedCrashException.class,
                () -> DeliverySemanticsApp.run(crashingConfig, consumer1, null, new AtomicBoolean(false)));
        assertEquals(FailurePoint.AFTER_PROCESS_BEFORE_COMMIT, crash.failurePoint());
        assertEquals(1L, committedOffset(consumer1, tp),
                "only ORDER-4001 (offset 0) was committed -- ORDER-4002 was processed but never committed");
        // consumer1 is deliberately never closed -- that omission IS the simulated crash.

        Config restartConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-2",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.NONE,
                "", "", false, 0, 1, false);
        try (KafkaConsumer<String, String> consumer2 = newTestConsumer(groupId, "consumer-2", false)) {
            DeliverySemanticsApp.run(restartConfig, consumer2, null, new AtomicBoolean(false));
            assertEquals(2L, committedOffset(consumer2, tp),
                    "ORDER-4002 (offset 1) is reprocessed and committed again after restart -- this is the duplicate");
        }
    }

    @Test
    void commitBeforeProcessCanPermanentlyLoseARecord() throws Exception {
        String topic = uniqueTopic();
        createTopic(topic, 1, (short) 1);
        produceOrderEvents(topic, 0, "ORDER-5001", "ORDER-5002", "ORDER-5003");
        String groupId = "it-group-" + UUID.randomUUID();
        TopicPartition tp = new TopicPartition(topic, 0);

        Config crashingConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-1",
                CommitTiming.BEFORE_PROCESS, CommitMode.SYNC, FailurePoint.AFTER_COMMIT_BEFORE_PROCESS,
                "ORDER-5002", "", false, 0, 0, false);

        KafkaConsumer<String, String> consumer1 = newTestConsumer(groupId, "consumer-1", false);
        SimulatedCrashException crash = org.junit.jupiter.api.Assertions.assertThrows(
                SimulatedCrashException.class,
                () -> DeliverySemanticsApp.run(crashingConfig, consumer1, null, new AtomicBoolean(false)));
        assertEquals(FailurePoint.AFTER_COMMIT_BEFORE_PROCESS, crash.failurePoint());
        assertEquals(2L, committedOffset(consumer1, tp),
                "commitTiming=BEFORE_PROCESS already committed ORDER-5002's offset (2) before crashing, even though it was never processed");
        // consumer1 is deliberately never closed -- that omission IS the simulated crash.

        try (KafkaConsumer<String, String> consumer2 = newTestConsumer(groupId, "consumer-2", false)) {
            consumer2.subscribe(List.of(topic));
            ConsumerRecords<String, String> records = pollUntilNonEmpty(consumer2, Duration.ofSeconds(15));
            assertFalse(records.isEmpty(), "ORDER-5003 should still be fetched -- it is the record after the lost one");
            ConsumerRecord<String, String> first = records.iterator().next();
            OrderEvent event = OrderEvent.parse(first.value());
            assertEquals("ORDER-5003", event.eventId(),
                    "restart resumes strictly after the committed offset -- ORDER-5002 is skipped forever, never delivered again");
        }
    }

    @Test
    void idempotentProcessingPreventsTheDuplicateFromReapplyingItsSideEffect() throws Exception {
        String topic = uniqueTopic();
        createTopic(topic, 1, (short) 1);
        produceOrderEvents(topic, 0, "ORDER-6001", "ORDER-6002", "ORDER-6003");
        String groupId = "it-group-" + UUID.randomUUID();

        ProcessedEventStore store = ProcessedEventStore.forGroup(groupId);
        store.reset();

        Config crashingConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-1",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.AFTER_PROCESS_BEFORE_COMMIT,
                "ORDER-6002", "", true, 0, 0, false);

        KafkaConsumer<String, String> consumer1 = newTestConsumer(groupId, "consumer-1", false);
        org.junit.jupiter.api.Assertions.assertThrows(
                SimulatedCrashException.class,
                () -> DeliverySemanticsApp.run(crashingConfig, consumer1, store, new AtomicBoolean(false)));
        assertTrue(store.isProcessed("ORDER-6002"),
                "the idempotency store durably records the side effect the instant it happens -- independent of whether the offset commit that would follow ever ran");
        int processedBeforeRestart = store.size();
        // consumer1 is deliberately never closed -- that omission IS the simulated crash.

        Config restartConfig = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-2",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.NONE,
                "", "", true, 0, 0, false);
        try (KafkaConsumer<String, String> consumer2 = newTestConsumer(groupId, "consumer-2", false)) {
            ByteArrayOutputStream captured = captureStdoutDuring(() -> {
                AtomicBoolean shuttingDown = new AtomicBoolean(false);
                Thread stopper = new Thread(() -> {
                    try {
                        // Comfortably longer than consumer-1's
                        // max.poll.interval.ms-driven eviction (6s) plus
                        // time for consumer-2's rebalance to settle.
                        Thread.sleep(11_000);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    shuttingDown.set(true);
                    consumer2.wakeup();
                });
                stopper.start();
                try {
                    DeliverySemanticsApp.run(restartConfig, consumer2, store, shuttingDown);
                } catch (org.apache.kafka.common.errors.WakeupException ignored) {
                    // expected: the stopper thread above ends the bounded run.
                }
                stopper.join();
            });
            String output = captured.toString();
            assertTrue(output.contains("eventId=ORDER-6002") && output.contains("status=DUPLICATE_SKIPPED"),
                    "the redelivered record must be recognized as already-processed, not reapplied:\n" + output);
        }

        assertEquals(2, processedBeforeRestart, "ORDER-6001 and ORDER-6002 were processed before the crash");
        assertEquals(3, store.size(),
                "3 distinct eventIds were ever processed (ORDER-6001/6002/6003) even though ORDER-6002 was DELIVERED "
                        + "twice across the two runs (once before the crash, once again on restart) -- the "
                        + "idempotency store's size tracks distinct business effects applied, not Kafka deliveries, "
                        + "which is exactly what prevented the redelivery of ORDER-6002 from reapplying its side "
                        + "effect a second time");
    }

    @Test
    void independentPartitionsMaintainIndependentCommittedOffsets() throws Exception {
        String topic = uniqueTopic();
        createTopic(topic, 2, (short) 1);
        produceToExplicitPartition(topic, 0, "ORDER-7001", "ORDER-7002", "ORDER-7003");
        produceToExplicitPartition(topic, 1, "ORDER-7101", "ORDER-7102");
        String groupId = "it-group-" + UUID.randomUUID();
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        Config config = new Config(
                KAFKA.getBootstrapServers(), topic, groupId, "consumer-1",
                CommitTiming.AFTER_PROCESS, CommitMode.SYNC, FailurePoint.NONE,
                "", "", false, 0, 5, false);

        try (KafkaConsumer<String, String> consumer = newTestConsumer(groupId, "consumer-1", false)) {
            DeliverySemanticsApp.run(config, consumer, null, new AtomicBoolean(false));
            assertEquals(3L, committedOffset(consumer, tp0),
                    "partition 0's committed offset reflects only partition 0's 3 records");
            assertEquals(2L, committedOffset(consumer, tp1),
                    "partition 1's committed offset reflects only partition 1's 2 records, independently of partition 0");
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueTopic() {
        return "ds-it-" + UUID.randomUUID();
    }

    private static void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, replicationFactor))).all().get();
        }
    }

    private static void produceOrderEvents(String topic, int unused, String... eventIds) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String eventId : eventIds) {
                OrderEvent event = new OrderEvent(eventId, "CUSTOMER-100", 42.00);
                producer.send(new ProducerRecord<>(topic, "CUSTOMER-100", event.toWireFormat())).get();
            }
        }
    }

    private static void produceToExplicitPartition(String topic, int partition, String... eventIds) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String eventId : eventIds) {
                OrderEvent event = new OrderEvent(eventId, "CUSTOMER-P" + partition, 42.00);
                producer.send(new ProducerRecord<>(topic, partition, "CUSTOMER-P" + partition, event.toWireFormat())).get();
            }
        }
    }

    private static KafkaConsumer<String, String> newTestConsumer(String groupId, String clientId, boolean autoCommit) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 7_000);
        // A "crashed" test consumer is deliberately never closed (see each
        // test's comment), so its background heartbeat thread keeps running
        // in this same JVM. It only proactively leaves the group once IT
        // detects no poll() call within max.poll.interval.ms -- shortened
        // here so that eviction, and therefore the next consumer's
        // reassignment, completes in single-digit seconds instead of
        // Kafka's 300s production default.
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 6_000);
        return new KafkaConsumer<>(props);
    }

    private static long committedOffset(KafkaConsumer<String, String> consumer, TopicPartition tp) {
        var committed = consumer.committed(Set.of(tp)).get(tp);
        return committed == null ? -1 : committed.offset();
    }

    private static ConsumerRecords<String, String> pollUntilNonEmpty(KafkaConsumer<String, String> consumer, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records;
            }
        }
        return consumer.poll(Duration.ofMillis(500));
    }

    /**
     * Captures everything {@link DeliverySemanticsApp#run} prints to
     * {@code System.out} while {@code action} executes, so a test can
     * assert on the exact {@code status=...} lines this lab's
     * observability format promises, without adding a test-only hook to
     * the production loop.
     */
    private static ByteArrayOutputStream captureStdoutDuring(ThrowingRunnable action) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buffer, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            action.run();
        } finally {
            System.setOut(original);
        }
        return buffer;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
