package com.kafkalab.nativeclient;

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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This is the WP-02A testing progression's second rung for this lab:
 * a real, containerized, KRaft-mode Kafka broker — not a mocked
 * {@code KafkaProducer}/{@code KafkaConsumer} — proving the actual
 * end-to-end behavior this lab teaches: a record produced with the native
 * client is later visible to the native consumer, addressable by
 * (topic, partition, offset).
 *
 * <p>The container image is pinned to the exact same {@code apache/kafka}
 * version this repository's Docker Compose environment uses
 * (platform/kafka/docker-compose.yml), so this test exercises the same
 * broker behavior a learner sees by hand in the manual lab experiments.
 *
 * <p>Testcontainers manages this container's lifecycle deterministically:
 * {@code @Testcontainers} + {@code @Container} start it before this class's
 * tests run and stop it afterward, with no manual start/stop calls and
 * nothing left running afterward. The one JUnit assertion this test relies
 * on is reached through a bounded polling loop with an overall deadline —
 * never a single blind sleep-then-check.
 */
@Testcontainers
class ProducerConsumerIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Test
    void producedRecordIsVisibleToNativeConsumer() throws Exception {
        String topic = "orders-java-it-" + UUID.randomUUID();
        createTopic(topic, 1, (short) 1);

        String key = "order-9001";
        String value = "CREATED-" + Instant.now();

        RecordMetadata sentMetadata = produceOneRecord(topic, key, value);
        assertNotNull(sentMetadata, "producer.send() should have completed with metadata, not a failure");

        ConsumerRecord<String, String> received = consumeUntil(topic, Duration.ofSeconds(30));

        assertEquals(key, received.key(), "the key the consumer saw should be exactly what was produced");
        assertEquals(value, received.value(), "the value the consumer saw should be exactly what was produced");
        assertEquals(
                sentMetadata.partition(), received.partition(),
                "the record should be readable from the same partition the broker reported at send time"
        );
        assertEquals(
                sentMetadata.offset(), received.offset(),
                "the record's offset should match what the broker assigned at send time"
        );
    }

    /**
     * Proves the exact mechanism {@code ConsumerApp}'s graceful-shutdown
     * path relies on: calling {@code wakeup()} from another thread
     * interrupts a blocking {@code poll()} call by throwing
     * {@link WakeupException}, rather than requiring the poll's own
     * timeout to elapse first.
     *
     * <p>This is tested directly at the Kafka-client level, rather than by
     * sending an OS signal to a running process, because reliably
     * delivering an interactive Ctrl+C-equivalent signal to a child JVM is
     * an OS/shell concern, not a Kafka one — what this lab actually needs
     * proven is that {@code consumer.wakeup()} does what {@code ConsumerApp}
     * assumes it does. The test's deadline (well under {@code poll}'s own
     * 30-second duration) is the deterministic assertion: if wakeup() did
     * nothing, this test would time out waiting for the latch instead of
     * passing quickly.
     */
    @Test
    void consumerWakeupInterruptsBlockingPoll() throws Exception {
        String topic = "orders-java-it-wakeup-" + UUID.randomUUID();
        createTopic(topic, 1, (short) 1);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "it-wakeup-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        AtomicBoolean caughtWakeup = new AtomicBoolean(false);
        CountDownLatch pollLoopExited = new CountDownLatch(1);

        Thread pollThread = new Thread(() -> {
            try {
                consumer.subscribe(List.of(topic));
                // A long poll duration on purpose: this thread should be
                // interrupted by wakeup() long before this elapses. If
                // wakeup() were a no-op, this test would time out waiting
                // on pollLoopExited below instead of passing in ~1 second.
                consumer.poll(Duration.ofSeconds(30));
            } catch (WakeupException e) {
                caughtWakeup.set(true);
            } finally {
                consumer.close();
                pollLoopExited.countDown();
            }
        });

        pollThread.start();
        // Give the poll thread a moment to actually enter poll() and join
        // the group before waking it up -- this sleep is a lead-in delay
        // before the real, bounded synchronization (the latch await
        // below), not the mechanism the test's pass/fail depends on.
        Thread.sleep(1_000);

        long wakeupCalledAt = System.currentTimeMillis();
        consumer.wakeup();

        boolean exitedInTime = pollLoopExited.await(10, TimeUnit.SECONDS);
        long elapsedMs = System.currentTimeMillis() - wakeupCalledAt;

        assertTrue(exitedInTime, "poll thread should have exited promptly after wakeup(), not after poll()'s own 30s timeout");
        assertTrue(caughtWakeup.get(), "the poll thread should have observed a WakeupException, not a normal poll() return");
        assertTrue(elapsedMs < 10_000, "wakeup() should interrupt poll() almost immediately, not after ~30s; took " + elapsedMs + "ms");
    }

    private static void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, replicationFactor)))
                    .all()
                    .get();
        }
    }

    private static RecordMetadata produceOneRecord(String topic, String key, String value)
            throws ExecutionException, InterruptedException {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            return producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
    }

    /**
     * Polls in a bounded loop until a record is found or {@code timeout}
     * elapses. This is the deterministic-timeout, no-fixed-sleep
     * synchronization this test's class comment refers to: each
     * {@code poll()} call already has its own short duration, and the
     * surrounding loop simply keeps calling it until the overall deadline
     * — there is no separate "wait N seconds, then check once" step.
     */
    private static ConsumerRecord<String, String> consumeUntil(String topic, Duration timeout) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "it-test-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));

            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }

        fail("no record was consumed from topic " + topic + " within " + timeout);
        throw new AssertionError("unreachable");
    }
}
