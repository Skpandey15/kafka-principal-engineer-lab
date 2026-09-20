package com.kafkalab.transactions;

import com.kafkalab.transactions.eos.NonTransactionalConsumeTransformProduceApp;
import com.kafkalab.transactions.eos.TransactionalConsumeTransformProduceApp;
import com.kafkalab.transactions.support.FailurePoint;
import com.kafkalab.transactions.support.OrderEvent;
import com.kafkalab.transactions.support.SimulatedCrashException;
import com.kafkalab.transactions.support.TransactionsKafkaCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Real Kafka transactions against a real, single-node KRaft cluster (see
 * {@link TransactionsKafkaCluster} for why single-node is the right choice
 * for THESE tests specifically) -- no mocked producer, no mocked
 * coordinator, no simulated commit-marker bookkeeping.
 *
 * <p>Every assertion is reached through a bounded condition-polling loop
 * with an overall deadline, per this repository's established test
 * convention. Where a test needs to assert an ABSENCE (nothing arrives),
 * it polls for a bounded window and then asserts nothing arrived -- this is
 * an absence check with a timeout, not a sleep used as a synchronization
 * mechanism.
 */
class TransactionsExactlyOnceIntegrationTest {

    private static TransactionsKafkaCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new TransactionsKafkaCluster(19_500);
        cluster.start();
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Test
    void committedTransactionalRecordsAreVisibleToReadCommitted() throws Exception {
        String topic = uniqueTopic("commit-visible");
        createTopic(topic, 1);

        try (KafkaProducer<String, String> producer = transactionalProducer("txn-" + UUID.randomUUID())) {
            producer.initTransactions();
            producer.beginTransaction();
            send(producer, topic, "Order-1");
            send(producer, topic, "Order-2");
            send(producer, topic, "Order-3");
            producer.commitTransaction();
        }

        List<String> seen = pollAtLeast(topic, "read_committed", 3, Duration.ofSeconds(20));
        assertEquals(3, seen.size(), "all 3 records from the committed transaction must be visible to read_committed");
    }

    @Test
    void abortedRecordsAreHiddenFromReadCommittedButVisibleToReadUncommitted() throws Exception {
        String topic = uniqueTopic("abort-hidden");
        createTopic(topic, 1);

        try (KafkaProducer<String, String> producer = transactionalProducer("txn-" + UUID.randomUUID())) {
            producer.initTransactions();
            producer.beginTransaction();
            send(producer, topic, "Order-D");
            send(producer, topic, "Order-E");
            producer.abortTransaction();

            // A marker record from a SEPARATE, committed transaction on the
            // same producer/partition -- read_committed consumers must skip
            // straight past the aborted offsets to this one; its arrival is
            // what lets the "nothing before it" assertion be bounded rather
            // than an indefinite wait for an absence.
            producer.beginTransaction();
            send(producer, topic, "Order-MARKER");
            producer.commitTransaction();
        }

        List<String> committedView = pollAtLeast(topic, "read_committed", 1, Duration.ofSeconds(20));
        assertEquals(List.of("Order-MARKER"), committedView,
                "read_committed must skip the aborted records entirely and see only the committed marker");

        List<String> uncommittedView = pollAtLeast(topic, "read_uncommitted", 3, Duration.ofSeconds(20));
        assertEquals(3, uncommittedView.size(),
                "read_uncommitted must observe the aborted records' actual content, plus the marker");
        assertTrue(uncommittedView.contains("Order-D") && uncommittedView.contains("Order-E"),
                "read_uncommitted's view must include the records from the transaction that later aborted");
    }

    @Test
    void oneTransactionCommitsAtomicallyAcrossMultipleTopics() throws Exception {
        String ordersTopic = uniqueTopic("multi-orders");
        String paymentsTopic = uniqueTopic("multi-payments");
        String auditTopic = uniqueTopic("multi-audit");
        createTopic(ordersTopic, 1);
        createTopic(paymentsTopic, 1);
        createTopic(auditTopic, 1);

        try (KafkaProducer<String, String> producer = transactionalProducer("txn-" + UUID.randomUUID())) {
            producer.initTransactions();
            producer.beginTransaction();
            send(producer, ordersTopic, "Order-500");
            send(producer, paymentsTopic, "Order-500-PAYMENT");
            send(producer, auditTopic, "Order-500-AUDIT");
            producer.commitTransaction();
        }

        assertEquals(List.of("Order-500"), pollAtLeast(ordersTopic, "read_committed", 1, Duration.ofSeconds(20)));
        assertEquals(List.of("Order-500-PAYMENT"), pollAtLeast(paymentsTopic, "read_committed", 1, Duration.ofSeconds(20)));
        assertEquals(List.of("Order-500-AUDIT"), pollAtLeast(auditTopic, "read_committed", 1, Duration.ofSeconds(20)));
    }

    @Test
    void oneTransactionAbortDiscardsAllInvolvedTopicsTogether() throws Exception {
        String ordersTopic = uniqueTopic("multi-abort-orders");
        String paymentsTopic = uniqueTopic("multi-abort-payments");
        createTopic(ordersTopic, 1);
        createTopic(paymentsTopic, 1);

        try (KafkaProducer<String, String> producer = transactionalProducer("txn-" + UUID.randomUUID())) {
            producer.initTransactions();
            producer.beginTransaction();
            send(producer, ordersTopic, "Order-999");
            send(producer, paymentsTopic, "Order-999-PAYMENT");
            producer.abortTransaction();

            // Same marker trick as the single-topic abort test, applied to
            // BOTH topics, so the "nothing was committed" assertion is
            // bounded by a real, subsequent, committed arrival rather than
            // an indefinite wait.
            producer.beginTransaction();
            send(producer, ordersTopic, "MARKER");
            send(producer, paymentsTopic, "MARKER");
            producer.commitTransaction();
        }

        assertEquals(List.of("MARKER"), pollAtLeast(ordersTopic, "read_committed", 1, Duration.ofSeconds(20)),
                "the aborted orders record must not appear to read_committed on either topic");
        assertEquals(List.of("MARKER"), pollAtLeast(paymentsTopic, "read_committed", 1, Duration.ofSeconds(20)),
                "the aborted payments record must not appear to read_committed on either topic");
    }

    @Test
    void secondProducerInstanceFencesTheFirstUnderTheSameTransactionalId() throws Exception {
        String topic = uniqueTopic("fencing");
        createTopic(topic, 1);
        String transactionalId = "txn-fence-" + UUID.randomUUID();

        KafkaProducer<String, String> first = transactionalProducer(transactionalId);
        first.initTransactions();
        first.beginTransaction();
        send(first, topic, "first-record");

        try (KafkaProducer<String, String> second = transactionalProducer(transactionalId)) {
            // initTransactions() for the SAME transactional.id ALWAYS bumps
            // the coordinator's producer epoch and fences whatever epoch
            // `first` was using -- deterministic, not timing-dependent.
            second.initTransactions();
            second.beginTransaction();
            send(second, topic, "second-record");
            second.commitTransaction();
        }

        Throwable fencingCause = null;
        try {
            send(first, topic, "first-record-after-fencing");
            fail("expected the fenced producer's send to fail");
        } catch (ExecutionException e) {
            fencingCause = e.getCause();
        } catch (InvalidProducerEpochException | ProducerFencedException e) {
            fencingCause = e;
        } finally {
            first.close(Duration.ofSeconds(2));
        }

        assertTrue(fencingCause instanceof InvalidProducerEpochException || fencingCause instanceof ProducerFencedException,
                "expected a fencing-related exception (InvalidProducerEpochException in kafka-clients 4.3.1, or ProducerFencedException), got: " + fencingCause);
    }

    @Test
    void consumeTransformProduceIsExactlyOnceWithNoFailure() throws Exception {
        String inputTopic = uniqueTopic("eos-input");
        String outputTopic = uniqueTopic("eos-output");
        createTopic(inputTopic, 1);
        createTopic(outputTopic, 1);
        produceUntransactional(inputTopic, "Order-A1", "Order-A2");

        var consumer = plainConsumer("eos-group-" + UUID.randomUUID(), "read_committed");
        var producer = transactionalProducer("txn-eos-" + UUID.randomUUID());
        try {
            producer.initTransactions();
            var config = new TransactionalConsumeTransformProduceApp.Config(
                    cluster.bootstrapServers(), inputTopic, outputTopic, "unused", "unused", FailurePoint.NONE);
            int handled = TransactionalConsumeTransformProduceApp.run(config, consumer, producer, new AtomicBoolean(false), 2);
            assertEquals(2, handled);
        } finally {
            consumer.close();
            producer.close();
        }

        List<String> output = pollAtLeast(outputTopic, "read_committed", 2, Duration.ofSeconds(20));
        assertEquals(2, output.size());
        assertTrue(output.contains("PROCESSED-Order-A1") && output.contains("PROCESSED-Order-A2"));
    }

    @Test
    void crashBeforeCommitLeavesNoVisibleOutputAndInputIsReprocessedOnRestart() throws Exception {
        String inputTopic = uniqueTopic("crash-before-input");
        String outputTopic = uniqueTopic("crash-before-output");
        String groupId = "crash-before-group-" + UUID.randomUUID();
        String transactionalId = "txn-crash-before-" + UUID.randomUUID();
        createTopic(inputTopic, 1);
        createTopic(outputTopic, 1);
        produceUntransactional(inputTopic, "Order-B1");

        // Attempt 1: crash BEFORE commitTransaction() returns.
        var consumer1 = plainConsumer(groupId, "read_committed");
        var producer1 = transactionalProducer(transactionalId);
        try {
            producer1.initTransactions();
            var config = new TransactionalConsumeTransformProduceApp.Config(
                    cluster.bootstrapServers(), inputTopic, outputTopic, "unused", "unused", FailurePoint.BEFORE_COMMIT);
            SimulatedCrashException crash = assertThrows(SimulatedCrashException.class,
                    () -> TransactionalConsumeTransformProduceApp.run(config, consumer1, producer1, new AtomicBoolean(false), 1));
            assertEquals(FailurePoint.BEFORE_COMMIT, crash.failurePoint());
        } finally {
            // A real crash never calls close() -- this producer is left
            // exactly as abruptly as System.exit(1) would leave it. Its
            // transaction is now hanging at the coordinator.
        }

        // Nothing must be visible yet: the transaction was never committed.
        assertNoRecordsArrive(outputTopic, "read_committed", Duration.ofSeconds(5));

        // Attempt 2: a fresh producer/consumer pair, same transactional.id
        // and group.id, simulating a restart. initTransactions() aborts the
        // hanging transaction left by attempt 1 automatically.
        var consumer2 = plainConsumer(groupId, "read_committed");
        var producer2 = transactionalProducer(transactionalId);
        try {
            producer2.initTransactions();
            var config = new TransactionalConsumeTransformProduceApp.Config(
                    cluster.bootstrapServers(), inputTopic, outputTopic, "unused", "unused", FailurePoint.NONE);
            int handled = TransactionalConsumeTransformProduceApp.run(config, consumer2, producer2, new AtomicBoolean(false), 1);
            assertEquals(1, handled, "the input record must be reprocessed on restart, since its offset was never committed");
        } finally {
            consumer2.close();
            producer2.close();
        }

        List<String> output = pollAtLeast(outputTopic, "read_committed", 1, Duration.ofSeconds(20));
        assertEquals(List.of("PROCESSED-Order-B1"), output,
                "exactly ONE committed output record must exist -- the aborted attempt's output must never surface, and the retry must not duplicate it");
    }

    @Test
    void crashAfterCommitDoesNotReprocessInputOnRestart() throws Exception {
        String inputTopic = uniqueTopic("crash-after-input");
        String outputTopic = uniqueTopic("crash-after-output");
        String groupId = "crash-after-group-" + UUID.randomUUID();
        String transactionalId = "txn-crash-after-" + UUID.randomUUID();
        createTopic(inputTopic, 1);
        createTopic(outputTopic, 1);
        produceUntransactional(inputTopic, "Order-C1");

        var consumer1 = plainConsumer(groupId, "read_committed");
        var producer1 = transactionalProducer(transactionalId);
        try {
            producer1.initTransactions();
            var config = new TransactionalConsumeTransformProduceApp.Config(
                    cluster.bootstrapServers(), inputTopic, outputTopic, "unused", "unused", FailurePoint.AFTER_COMMIT);
            SimulatedCrashException crash = assertThrows(SimulatedCrashException.class,
                    () -> TransactionalConsumeTransformProduceApp.run(config, consumer1, producer1, new AtomicBoolean(false), 1));
            assertEquals(FailurePoint.AFTER_COMMIT, crash.failurePoint());
        } finally {
            // Same as the BEFORE_COMMIT test: no close(), a real crash
            // wouldn't get to call it either. The commit already succeeded.
        }

        // The commit happened before the simulated crash -- output must
        // already be visible.
        List<String> output = pollAtLeast(outputTopic, "read_committed", 1, Duration.ofSeconds(20));
        assertEquals(List.of("PROCESSED-Order-C1"), output);

        // Restart: a fresh consumer in the SAME group must find nothing left
        // to process -- the offset was committed atomically with the output
        // in the SAME transaction, before the crash. run() with no new
        // input to fetch loops (correctly, for a real app) waiting for the
        // next batch, so this is bounded from the outside instead of by a
        // record count.
        var consumer2 = plainConsumer(groupId, "read_committed");
        var producer2 = transactionalProducer(transactionalId);
        try {
            producer2.initTransactions();
            var config = new TransactionalConsumeTransformProduceApp.Config(
                    cluster.bootstrapServers(), inputTopic, outputTopic, "unused", "unused", FailurePoint.NONE);
            int handled = runBounded(
                    shuttingDown -> TransactionalConsumeTransformProduceApp.run(config, consumer2, producer2, shuttingDown, 0),
                    Duration.ofSeconds(8));
            assertEquals(0, handled, "no new input should be found -- the previous transaction's offset commit already covered it");
        } finally {
            consumer2.close();
            producer2.close();
        }
    }

    @Test
    void offsetsAndOutputAreCommittedAtomicallyInOneTransaction() throws Exception {
        String inputTopic = uniqueTopic("atomic-offset-input");
        String outputTopic = uniqueTopic("atomic-offset-output");
        String groupId = "atomic-offset-group-" + UUID.randomUUID();
        String transactionalId = "txn-atomic-offset-" + UUID.randomUUID();
        createTopic(inputTopic, 1);
        createTopic(outputTopic, 1);
        produceUntransactional(inputTopic, "Order-Z1");

        try (Admin admin = newAdmin();
             KafkaConsumer<String, String> inputConsumer = plainConsumer(groupId, "read_committed");
             KafkaProducer<String, String> producer = transactionalProducer(transactionalId)) {

            producer.initTransactions();
            inputConsumer.subscribe(List.of(inputTopic));
            ConsumerRecord<String, String> record = pollOneRecord(inputConsumer, Duration.ofSeconds(20));
            OrderEvent input = OrderEvent.parse(record.value());
            TopicPartition inputTp = new TopicPartition(record.topic(), record.partition());

            producer.beginTransaction();
            send(producer, outputTopic, "PROCESSED-" + input.eventId());
            producer.sendOffsetsToTransaction(
                    Map.of(inputTp, new OffsetAndMetadata(record.offset() + 1)),
                    inputConsumer.groupMetadata());

            // BEFORE commit: neither the output nor the committed group
            // offset should be visible/updated yet.
            assertNoRecordsArrive(outputTopic, "read_committed", Duration.ofSeconds(3));
            Long offsetBeforeCommit = committedOffsetOrNull(admin, groupId, inputTp);
            assertTrue(offsetBeforeCommit == null || offsetBeforeCommit == 0,
                    "the input offset must not be committed before commitTransaction() -- got " + offsetBeforeCommit);

            producer.commitTransaction();

            // AFTER commit: both become visible/updated together.
            List<String> output = pollAtLeast(outputTopic, "read_committed", 1, Duration.ofSeconds(20));
            assertEquals(List.of("PROCESSED-" + input.eventId()), output);
            waitUntil(() -> {
                Long offset = committedOffsetOrNull(admin, groupId, inputTp);
                return offset != null && offset == record.offset() + 1;
            }, Duration.ofSeconds(20));
        }
    }

    @Test
    void nonTransactionalPipelineDuplicatesOutputWhenCrashingBeforeOffsetCommit() throws Exception {
        String inputTopic = uniqueTopic("non-txn-input");
        String outputTopic = uniqueTopic("non-txn-output");
        String groupId = "non-txn-group-" + UUID.randomUUID();
        createTopic(inputTopic, 1);
        createTopic(outputTopic, 1);
        produceUntransactional(inputTopic, "Order-N1");

        var config = new NonTransactionalConsumeTransformProduceApp.Config(
                cluster.bootstrapServers(), inputTopic, outputTopic, groupId, true, "Order-N1");

        // Attempt 1: crashes right after producing output, before committing
        // the input offset.
        try (KafkaConsumer<String, String> consumer1 = plainConsumer(groupId, "read_uncommitted");
             KafkaProducer<String, String> producer1 = plainProducer()) {
            assertThrows(NonTransactionalConsumeTransformProduceApp.CrashPoint.class,
                    () -> NonTransactionalConsumeTransformProduceApp.run(config, consumer1, producer1, new AtomicBoolean(false), 1));
        }

        // Attempt 2 (restart): the offset was never committed, so the same
        // input is reprocessed and produces a SECOND output record.
        var restartConfig = new NonTransactionalConsumeTransformProduceApp.Config(
                cluster.bootstrapServers(), inputTopic, outputTopic, groupId, false, "");
        try (KafkaConsumer<String, String> consumer2 = plainConsumer(groupId, "read_uncommitted");
             KafkaProducer<String, String> producer2 = plainProducer()) {
            int handled = NonTransactionalConsumeTransformProduceApp.run(restartConfig, consumer2, producer2, new AtomicBoolean(false), 1);
            assertEquals(1, handled);
        }

        List<String> output = pollAtLeast(outputTopic, "read_uncommitted", 2, Duration.ofSeconds(20));
        assertEquals(2, output.size(),
                "without transactions, a crash between producing output and committing the input offset causes the input to be reprocessed and the output to be duplicated");
        assertTrue(output.stream().allMatch("PROCESSED-Order-N1"::equals));
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private static Admin newAdmin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        return Admin.create(props);
    }

    private static void createTopic(String topic, int partitions) throws Exception {
        try (Admin admin = newAdmin()) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private static KafkaProducer<String, String> transactionalProducer(String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return new KafkaProducer<>(props);
    }

    private static KafkaProducer<String, String> plainProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void produceUntransactional(String topic, String... eventIds) throws Exception {
        try (KafkaProducer<String, String> producer = plainProducer()) {
            for (String eventId : eventIds) {
                send(producer, topic, eventId);
            }
        }
    }

    private static RecordMetadata send(KafkaProducer<String, String> producer, String topic, String eventId) throws Exception {
        OrderEvent event = new OrderEvent(eventId, "CUSTOMER-IT", 1.0);
        return producer.send(new ProducerRecord<>(topic, eventId, event.toWireFormat())).get();
    }

    private static KafkaConsumer<String, String> plainConsumer(String groupId, String isolationLevel) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        return new KafkaConsumer<>(props);
    }

    private static ConsumerRecord<String, String> pollOneRecord(KafkaConsumer<String, String> consumer, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var records = consumer.poll(Duration.ofMillis(300));
            for (ConsumerRecord<String, String> record : records) {
                return record;
            }
        }
        throw new AssertionError("no record arrived within " + timeout);
    }

    private static List<String> pollAtLeast(String topic, String isolationLevel, int minCount, Duration timeout) {
        try (KafkaConsumer<String, String> consumer = plainConsumer("it-read-" + UUID.randomUUID(), isolationLevel)) {
            consumer.subscribe(List.of(topic));
            List<String> collected = new ArrayList<>();
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                var records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    collected.add(OrderEvent.parse(record.value()).eventId());
                }
            }
            if (collected.size() < minCount) {
                throw new AssertionError("expected at least " + minCount + " records on " + topic + " (isolation=" + isolationLevel + "), got " + collected);
            }
            return collected;
        }
    }

    /** An absence check with a bounded window -- see the class Javadoc. */
    private static void assertNoRecordsArrive(String topic, String isolationLevel, Duration window) {
        try (KafkaConsumer<String, String> consumer = plainConsumer("it-absence-" + UUID.randomUUID(), isolationLevel)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(window);
            List<String> unexpected = new ArrayList<>();
            while (Instant.now().isBefore(deadline)) {
                var records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    unexpected.add(record.value());
                }
            }
            assertTrue(unexpected.isEmpty(), "expected no records on " + topic + " within " + window + ", but got: " + unexpected);
        }
    }

    private static Long committedOffsetOrNull(Admin admin, String groupId, TopicPartition tp) {
        try {
            var offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            OffsetAndMetadata oam = offsets.get(tp);
            return oam == null ? null : oam.offset();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Runs {@code task} on a background thread with a live
     * {@code AtomicBoolean shuttingDown}, waits up to {@code timeout}, then
     * signals shutdown and joins. Used where "nothing new happens" must be
     * bounded by a wall-clock window rather than a record count, since the
     * pipeline's own loop otherwise waits indefinitely for the next batch --
     * exactly as a real long-running processor should.
     */
    private static int runBounded(java.util.function.Function<AtomicBoolean, Integer> task, Duration timeout) throws InterruptedException {
        AtomicBoolean shuttingDown = new AtomicBoolean(false);
        int[] result = {0};
        Thread worker = new Thread(() -> result[0] = task.apply(shuttingDown), "runBounded-worker");
        worker.start();
        worker.join(timeout.toMillis());
        shuttingDown.set(true);
        worker.join(Duration.ofSeconds(5).toMillis());
        return result[0];
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
