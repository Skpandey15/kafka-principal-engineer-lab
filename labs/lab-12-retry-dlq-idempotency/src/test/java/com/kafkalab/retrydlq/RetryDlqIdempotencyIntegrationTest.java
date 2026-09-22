package com.kafkalab.retrydlq;

import com.kafkalab.retrydlq.consumer.RetryingRecordProcessor;
import com.kafkalab.retrydlq.support.BusinessLogicSimulator;
import com.kafkalab.retrydlq.support.IdempotencyStore;
import com.kafkalab.retrydlq.support.OrderEvent;
import com.kafkalab.retrydlq.support.RetryDlqTestCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kafka + real PostgreSQL -- no mocked idempotency store, no
 * mocked producer, no simulated Kafka Connect. Business logic FAILURES
 * are the one thing simulated, and always deterministically (see
 * {@link BusinessLogicSimulator}), matching WP-06's own established
 * convention.
 */
class RetryDlqIdempotencyIntegrationTest {

    private static RetryDlqTestCluster cluster;
    private static KafkaProducer<String, String> producer;

    @BeforeAll
    static void startCluster() {
        cluster = new RetryDlqTestCluster(19_702);
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
    void duplicateEventIsProcessedOnlyOnceEvenAfterRedeliveryAfterACrashBeforeOffsetCommit() throws Exception {
        String id = uniqueId();
        String groupId = "grp-dup-" + id;
        String eventId = "ORDER-" + id;
        BusinessLogicSimulator sim = new BusinessLogicSimulator();
        ConsumerRecord<String, String> record = orderRecord("topic-dup-" + id, eventId, "C-1", "10.00");

        RetryingRecordProcessor.Outcome first = newProcessor(groupId).process(record, sim::process);
        assertEquals(RetryingRecordProcessor.Outcome.PROCESSED, first);

        // A brand-new IdempotencyStore/processor backed by a brand-new
        // DB connection -- models a consumer restart after a crash that
        // happened AFTER the DB commit but BEFORE the Kafka offset
        // commit, so the same record is redelivered.
        RetryingRecordProcessor.Outcome redelivered = newProcessor(groupId).process(record, sim::process);
        assertEquals(RetryingRecordProcessor.Outcome.DUPLICATE_SKIPPED, redelivered);
        assertEquals(1, sim.attemptsFor(eventId), "business logic must be invoked exactly once despite redelivery");
    }

    @Test
    void concurrentDuplicateClaimsAreSafelySerializedByTheUniqueConstraint() throws Exception {
        String id = uniqueId();
        String groupId = "grp-race-" + id;
        String eventId = "ORDER-" + id;
        BusinessLogicSimulator sim = new BusinessLogicSimulator();
        ConsumerRecord<String, String> record = orderRecord("topic-race-" + id, eventId, "C-1", "10.00");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch startGate = new CountDownLatch(1);
        List<java.util.concurrent.Future<RetryingRecordProcessor.Outcome>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(pool.submit(() -> {
                startGate.await();
                return newProcessor(groupId).process(record, sim::process);
            }));
        }
        startGate.countDown();
        List<RetryingRecordProcessor.Outcome> outcomes = new ArrayList<>();
        for (var f : futures) {
            outcomes.add(f.get());
        }
        pool.shutdown();

        long processedCount = outcomes.stream().filter(o -> o == RetryingRecordProcessor.Outcome.PROCESSED).count();
        assertEquals(1, processedCount, "exactly one of the two racing claims must win: " + outcomes);
        assertEquals(1, sim.attemptsFor(eventId), "business logic must never run twice for the same eventId, even under a real race");
    }

    @Test
    void aTransientlyFailingMessageIsRetriedWithBackoffThenSucceeds() throws Exception {
        String id = uniqueId();
        String eventId = "ORDER-" + id;
        BusinessLogicSimulator sim = new BusinessLogicSimulator();
        sim.failNTimesThenSucceed(eventId, 2);
        ConsumerRecord<String, String> record = orderRecord("topic-retry-" + id, eventId, "C-1", "10.00");

        long start = System.currentTimeMillis();
        RetryingRecordProcessor.Outcome outcome = newProcessor("grp-retry-" + id, 3, Duration.ofMillis(150)).process(record, sim::process);
        long elapsedMs = System.currentTimeMillis() - start;

        assertEquals(RetryingRecordProcessor.Outcome.PROCESSED, outcome);
        assertEquals(3, sim.attemptsFor(eventId), "must have failed twice then succeeded on the 3rd attempt");
        assertTrue(elapsedMs >= 150 + 300, "backoff must actually have elapsed (150ms + 300ms minimum), got " + elapsedMs + "ms");
    }

    @Test
    void aPoisonMessageIsRoutedToTheDlqImmediatelyWithOriginalHeadersAndPayload() throws Exception {
        String id = uniqueId();
        String sourceTopic = "topic-poison-" + id;
        String dlqTopic = "dlq-poison-" + id;
        String malformed = "this-is-not-a-valid-order-event-wire-format";
        ConsumerRecord<String, String> record = new ConsumerRecord<>(sourceTopic, 3, 77L, "some-key", malformed);

        RetryingRecordProcessor.Outcome outcome = newProcessor("grp-poison-" + id, dlqTopic, 3, Duration.ofMillis(10))
                .process(record, event -> { throw new IllegalStateException("business logic must never be invoked for a poison message"); });
        assertEquals(RetryingRecordProcessor.Outcome.POISON_SENT_TO_DLQ, outcome);

        List<ConsumerRecord<String, String>> dlqRecords = consumeRecords(dlqTopic, 1, Duration.ofSeconds(15));
        ConsumerRecord<String, String> dlqRecord = dlqRecords.get(0);
        assertEquals("some-key", dlqRecord.key());
        assertEquals(malformed, dlqRecord.value(), "the DLQ message must preserve the original payload unmodified");
        assertEquals(sourceTopic, headerValue(dlqRecord, "kafka_dlt-original-topic"));
        assertEquals("3", headerValue(dlqRecord, "kafka_dlt-original-partition"));
        assertEquals("77", headerValue(dlqRecord, "kafka_dlt-original-offset"));
        assertTrue(headerValue(dlqRecord, "kafka_dlt-exception-fqcn").contains("IllegalArgumentException"));
    }

    @Test
    void anAlwaysFailingMessageExhaustsRetriesIsRoutedToTheDlqAndItsClaimIsReleased() throws Exception {
        String id = uniqueId();
        String groupId = "grp-exhaust-" + id;
        String eventId = "ORDER-" + id;
        String dlqTopic = "dlq-exhaust-" + id;
        BusinessLogicSimulator sim = new BusinessLogicSimulator();
        sim.alwaysFail(eventId);
        ConsumerRecord<String, String> record = orderRecord("topic-exhaust-" + id, eventId, "C-1", "10.00");

        RetryingRecordProcessor.Outcome outcome = newProcessor(groupId, dlqTopic, 2, Duration.ofMillis(10)).process(record, sim::process);
        assertEquals(RetryingRecordProcessor.Outcome.SENT_TO_DLQ, outcome);
        assertEquals(2, sim.attemptsFor(eventId), "exactly maxAttempts attempts must be made before giving up");

        try (Connection connection = newJdbcConnection()) {
            assertNull(new IdempotencyStore(connection, groupId).statusOf(eventId),
                    "the claim must be RELEASED (deleted), not left DONE-less forever, so a later fixed redelivery isn't permanently blocked");
        }
        consumeRecords(dlqTopic, 1, Duration.ofSeconds(15));
    }

    @Test
    void anOrphanedInProgressClaimOlderThanTheLeaseWindowCanBeReclaimed() throws Exception {
        String id = uniqueId();
        String groupId = "grp-orphan-" + id;
        String eventId = "ORDER-" + id;

        try (Connection connection = newJdbcConnection()) {
            IdempotencyStore store = new IdempotencyStore(connection, groupId);
            // Simulates a process that claimed the event then crashed --
            // never calls complete() or release().
            assertEquals(IdempotencyStore.ClaimResult.CLAIMED, store.tryClaim(eventId, Duration.ofSeconds(30)));
            assertEquals(IdempotencyStore.ClaimResult.IN_PROGRESS_ELSEWHERE, store.tryClaim(eventId, Duration.ofSeconds(30)),
                    "not yet stale -- must not be reclaimable within the lease window");
        }

        Thread.sleep(600);

        try (Connection connection = newJdbcConnection()) {
            IdempotencyStore store = new IdempotencyStore(connection, groupId);
            assertEquals(IdempotencyStore.ClaimResult.CLAIMED, store.tryClaim(eventId, Duration.ofMillis(500)),
                    "a claim older than the lease window must be reclaimable -- otherwise a crash mid-processing blocks this eventId forever");
        }
    }

    @Test
    void consumptionContinuesPastAPoisonMessageToTheNextRecordOnTheSamePartition() throws Exception {
        String id = uniqueId();
        String topic = "topic-continue-" + id;
        String dlqTopic = "dlq-continue-" + id;
        String groupId = "grp-continue-" + id;
        String eventId = "ORDER-" + id;
        BusinessLogicSimulator sim = new BusinessLogicSimulator();

        producer.send(new ProducerRecord<>(topic, "poison-key", "not-a-valid-wire-format")).get();
        producer.send(new ProducerRecord<>(topic, eventId, new OrderEvent(eventId, "C-1", 5.00).toWireFormat())).get();

        RetryingRecordProcessor processor = newProcessor(groupId, dlqTopic, 2, Duration.ofMillis(10));
        List<RetryingRecordProcessor.Outcome> outcomes = consumeAndProcessAll(topic, groupId, processor, sim, 2, Duration.ofSeconds(25));

        assertEquals(RetryingRecordProcessor.Outcome.POISON_SENT_TO_DLQ, outcomes.get(0));
        assertEquals(RetryingRecordProcessor.Outcome.PROCESSED, outcomes.get(1));
        assertEquals(1, sim.attemptsFor(eventId), "the record AFTER the poison one must still have been processed -- the partition must not stall");
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static Connection newJdbcConnection() throws Exception {
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        return DriverManager.getConnection(cluster.jdbcUrl(), props);
    }

    private static RetryingRecordProcessor newProcessor(String groupId) throws Exception {
        return newProcessor(groupId, "unused-dlq-" + UUID.randomUUID(), 3, Duration.ofMillis(50));
    }

    private static RetryingRecordProcessor newProcessor(String groupId, int maxAttempts, Duration backoff) throws Exception {
        return newProcessor(groupId, "unused-dlq-" + UUID.randomUUID(), maxAttempts, backoff);
    }

    private static RetryingRecordProcessor newProcessor(String groupId, String dlqTopic, int maxAttempts, Duration backoff) throws Exception {
        IdempotencyStore store = new IdempotencyStore(newJdbcConnection(), groupId);
        return new RetryingRecordProcessor(store, producer, dlqTopic, maxAttempts, backoff, Duration.ofSeconds(30));
    }

    private static ConsumerRecord<String, String> orderRecord(String topic, String eventId, String customerId, String amount) {
        return new ConsumerRecord<>(topic, 0, 0L, eventId, new OrderEvent(eventId, customerId, Double.parseDouble(amount)).toWireFormat());
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static List<ConsumerRecord<String, String>> consumeRecords(String topic, int minCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consume-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                records.forEach(collected::add);
            }
        }
        if (collected.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " records on " + topic + ", got " + collected.size());
        }
        return collected;
    }

    private static List<RetryingRecordProcessor.Outcome> consumeAndProcessAll(String topic, String groupId, RetryingRecordProcessor processor,
                                                                                BusinessLogicSimulator sim, int minCount, Duration timeout) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        List<RetryingRecordProcessor.Outcome> outcomes = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (outcomes.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    outcomes.add(processor.process(record, sim::process));
                }
                consumer.commitSync();
            }
        }
        if (outcomes.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " outcomes on " + topic + ", got " + outcomes.size());
        }
        return outcomes;
    }
}
