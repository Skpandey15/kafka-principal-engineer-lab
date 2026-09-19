package com.kafkalab.consumergroups;

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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real, containerized, KRaft-mode Kafka broker (same {@code apache/kafka}
 * image/version as {@code platform/kafka/}, lab-02, and lab-03) proving
 * the architectural properties WP-05 is built around. No mocked
 * producer/consumer.
 *
 * <p>Every assertion below is reached through a bounded condition-polling
 * loop with an overall deadline (never a fixed sleep used as the
 * synchronization mechanism), because a consumer's assignment only
 * updates as a side effect of its own thread calling {@code poll()} --
 * see {@code ConsumerRunner} below, and
 * docs/consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md for why that is
 * a real Kafka client constraint, not a test-design choice.
 */
@Testcontainers
class ConsumerGroupsRebalancingIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Test
    void oneConsumerInAGroupReceivesEveryProducedRecord() throws Exception {
        String topic = "cg-it-" + UUID.randomUUID();
        createTopic(topic, 3, (short) 1);
        int recordCount = 30;
        produceRecords(topic, recordCount);

        ConsumerRunner runner = new ConsumerRunner("it-group-" + UUID.randomUUID(), "consumer-1", topic);
        Thread thread = new Thread(runner);
        thread.start();
        try {
            waitUntil(() -> runner.consumedCount.get() >= recordCount, Duration.ofSeconds(30));
            assertEquals(recordCount, runner.consumedCount.get());
            assertEquals(Set.of(0, 1, 2), partitionNumbers(runner.lastAssignment.get()));
        } finally {
            runner.stop();
            thread.join(10_000);
        }
    }

    @Test
    void twoIndependentConsumerGroupsEachReceiveEveryRecord() throws Exception {
        String topic = "cg-it-" + UUID.randomUUID();
        createTopic(topic, 3, (short) 1);
        int recordCount = 30;
        produceRecords(topic, recordCount);

        ConsumerRunner orderProcessing = new ConsumerRunner("order-processing-service-" + UUID.randomUUID(), "consumer-1", topic);
        ConsumerRunner orderAnalytics = new ConsumerRunner("order-analytics-service-" + UUID.randomUUID(), "consumer-1", topic);
        Thread t1 = new Thread(orderProcessing);
        Thread t2 = new Thread(orderAnalytics);
        t1.start();
        t2.start();
        try {
            waitUntil(() -> orderProcessing.consumedCount.get() >= recordCount
                    && orderAnalytics.consumedCount.get() >= recordCount, Duration.ofSeconds(30));

            assertEquals(recordCount, orderProcessing.consumedCount.get(),
                    "a partition can be assigned to only one consumer within a group, but two DIFFERENT groups each get their own full copy");
            assertEquals(recordCount, orderAnalytics.consumedCount.get());
        } finally {
            orderProcessing.stop();
            orderAnalytics.stop();
            t1.join(10_000);
            t2.join(10_000);
        }
    }

    @Test
    void twoConsumersInSameGroupSplitPartitionsWithNoOverlap() throws Exception {
        String topic = "cg-it-" + UUID.randomUUID();
        createTopic(topic, 3, (short) 1);
        String groupId = "it-group-" + UUID.randomUUID();

        ConsumerRunner a = new ConsumerRunner(groupId, "consumer-a", topic);
        ConsumerRunner b = new ConsumerRunner(groupId, "consumer-b", topic);
        Thread threadA = new Thread(a);
        Thread threadB = new Thread(b);
        threadA.start();
        threadB.start();

        try {
            waitUntil(() -> {
                Set<TopicPartition> assignedA = a.lastAssignment.get();
                Set<TopicPartition> assignedB = b.lastAssignment.get();
                return assignedA != null && assignedB != null
                        && assignedA.size() + assignedB.size() == 3
                        && Collections.disjoint(assignedA, assignedB);
            }, Duration.ofSeconds(30));

            Set<TopicPartition> assignedA = a.lastAssignment.get();
            Set<TopicPartition> assignedB = b.lastAssignment.get();
            assertEquals(3, assignedA.size() + assignedB.size(),
                    "both members of one group together should own every partition exactly once");
            assertTrue(Collections.disjoint(assignedA, assignedB),
                    "a partition must never be owned by two members of the SAME group at the same time");
        } finally {
            a.stop();
            b.stop();
            threadA.join(10_000);
            threadB.join(10_000);
        }
    }

    @Test
    void consumerLeavingCausesItsPartitionsToBeReassigned() throws Exception {
        String topic = "cg-it-" + UUID.randomUUID();
        createTopic(topic, 3, (short) 1);
        String groupId = "it-group-" + UUID.randomUUID();

        ConsumerRunner a = new ConsumerRunner(groupId, "consumer-a", topic);
        ConsumerRunner b = new ConsumerRunner(groupId, "consumer-b", topic);
        Thread threadA = new Thread(a);
        Thread threadB = new Thread(b);
        threadA.start();
        threadB.start();

        waitUntil(() -> a.lastAssignment.get() != null && b.lastAssignment.get() != null
                && a.lastAssignment.get().size() + b.lastAssignment.get().size() == 3, Duration.ofSeconds(30));

        // Consumer A departs gracefully (the same wakeup()-based shutdown
        // ConsumerGroupMemberApp uses) -- its partitions must end up
        // entirely with B.
        a.stop();
        threadA.join(10_000);

        try {
            waitUntil(() -> b.lastAssignment.get() != null && b.lastAssignment.get().size() == 3,
                    Duration.ofSeconds(30));
            assertEquals(3, b.lastAssignment.get().size(),
                    "after the only other member leaves gracefully, the sole remaining member should own every partition");
        } finally {
            b.stop();
            threadB.join(10_000);
        }
    }

    // --- test infrastructure --------------------------------------------

    private static Set<Integer> partitionNumbers(Set<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).collect(java.util.stream.Collectors.toSet());
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private static void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, replicationFactor))).all().get();
        }
    }

    private static void produceRecords(String topic, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "CUSTOMER-" + (100 + i % 5), "EVENT-" + i)).get();
            }
        }
    }

    /**
     * A single consumer-group member driven entirely on its own thread --
     * mirroring {@code ConsumerGroupMemberApp}'s real poll loop and
     * {@code wakeup()}-based shutdown. {@code lastAssignment} and
     * {@code consumedCount} are the only state read from other threads;
     * every {@code KafkaConsumer} method call happens exclusively on this
     * runner's own thread, per the Kafka client's single-threaded-access
     * contract.
     */
    private static final class ConsumerRunner implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
        final AtomicReference<Set<TopicPartition>> lastAssignment = new AtomicReference<>();
        final AtomicInteger consumedCount = new AtomicInteger(0);

        ConsumerRunner(String groupId, String clientId, String topic) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(List.of(topic));
        }

        void stop() {
            shuttingDown.set(true);
            consumer.wakeup();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    lastAssignment.set(Set.copyOf(consumer.assignment()));
                    for (ConsumerRecord<String, String> ignored : records) {
                        consumedCount.incrementAndGet();
                    }
                }
            } catch (WakeupException e) {
                if (!shuttingDown.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
        }
    }
}
