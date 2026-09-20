package com.kafkalab.replication;

import com.kafkalab.replication.support.ThreeBrokerKafkaCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A real, 3-node, combined broker+controller KRaft cluster (see
 * {@link ThreeBrokerKafkaCluster}), mechanically identical to
 * {@code platform/kafka-cluster/docker-compose.yml} -- no mocked broker,
 * no mocked producer/consumer, no simulated ISR bookkeeping.
 *
 * <p>Every assertion below is reached through a bounded condition-polling
 * loop with an overall deadline (never a fixed sleep used as the
 * synchronization mechanism), per this repository's established test
 * convention.
 *
 * <p>This class deliberately does NOT attempt an automated,
 * fully-deterministic reproduction of every manual experiment in the lab
 * README -- specifically, forcing ISR below {@code min.insync.replicas}
 * on the RF=3 topic by killing two of these three combined broker+
 * controller nodes also breaks this cluster's own controller-quorum
 * majority (a real, structural coupling this lab's own manual
 * experiments discovered and documented -- see the README's Experiment
 * 8 "what actually happened"). The {@code minIsrRejectsWriteBelowThreshold}
 * test below sidesteps that coupling on purpose, using an RF=2 topic and
 * killing only one broker, specifically so the ISR-enforcement behavior
 * itself is deterministic in CI without also depending on how this
 * specific 3-voter quorum happens to react to losing a majority.
 */
class ReplicationIsrBrokerFailureIntegrationTest {

    private static ThreeBrokerKafkaCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new ThreeBrokerKafkaCluster(19_400);
        cluster.start();
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Test
    void clusterFormsWithThreeRegisteredBrokers() throws Exception {
        try (Admin admin = newAdmin()) {
            waitUntil(() -> {
                try {
                    return admin.describeCluster().nodes().get().size() == 3;
                } catch (Exception e) {
                    return false;
                }
            }, Duration.ofSeconds(30));
            assertEquals(3, admin.describeCluster().nodes().get().size());
        }
    }

    @Test
    void replicatedTopicHasCorrectReplicationFactorAndFullIsr() throws Exception {
        String topic = uniqueTopic("rf3");
        try (Admin admin = newAdmin()) {
            createTopic(admin, topic, 3, (short) 3, null);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 3), Duration.ofSeconds(30));

            TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();
            assertEquals(3, description.partitions().size());
            for (TopicPartitionInfo partition : description.partitions()) {
                assertEquals(3, partition.replicas().size(),
                        "partition " + partition.partition() + " should have 3 replicas (replication factor 3)");
                assertEquals(3, partition.isr().size(),
                        "partition " + partition.partition() + " should start with a full ISR");
                assertNotNull(partition.leader(), "every partition should have an elected leader");
            }
        }
    }

    @Test
    void producedRecordsAreConsumedSuccessfully() throws Exception {
        String topic = uniqueTopic("produce-consume");
        try (Admin admin = newAdmin()) {
            createTopic(admin, topic, 1, (short) 3, null);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 3), Duration.ofSeconds(30));
        }

        int recordCount = 20;
        produceRecords(topic, recordCount);

        List<String> consumed = consumeAtLeast(topic, recordCount, Duration.ofSeconds(30));
        assertEquals(recordCount, consumed.size());
    }

    @Test
    void leaderFailureElectsNewLeaderAndPreservesCommittedRecords() throws Exception {
        String topic = uniqueTopic("leader-failure");
        int recordCount = 10;
        int originalLeader;
        try (Admin admin = newAdmin()) {
            createTopic(admin, topic, 1, (short) 3, null);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 3), Duration.ofSeconds(30));
            originalLeader = leaderOf(admin, topic, 0);
        }

        produceRecords(topic, recordCount);

        cluster.killBroker(originalLeader);
        try (Admin admin = newAdmin()) {
            waitUntil(() -> {
                Integer leader = leaderOfOrNull(admin, topic, 0);
                return leader != null && leader != originalLeader;
            }, Duration.ofSeconds(60));

            int newLeader = leaderOf(admin, topic, 0);
            assertTrue(newLeader != originalLeader, "a different broker must have taken over as leader");
        }

        // Durability check: every record committed BEFORE the leader died
        // must still be readable from a fresh consumer, through the NEW
        // leader, with no broker restart involved.
        List<String> consumed = consumeAtLeast(topic, recordCount, Duration.ofSeconds(30));
        assertEquals(recordCount, consumed.size(),
                "all records committed before the leader failure must remain readable after failover");

        cluster.reviveBroker(originalLeader);
        try (Admin admin = newAdmin()) {
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 3), Duration.ofSeconds(60));
        }
    }

    @Test
    void revivedBrokerRejoinsIsrAfterCatchingUp() throws Exception {
        String topic = uniqueTopic("broker-recovery");
        try (Admin admin = newAdmin()) {
            createTopic(admin, topic, 1, (short) 3, null);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 3), Duration.ofSeconds(30));

            int follower = followerOf(admin, topic, 0);
            cluster.killBroker(follower);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 2), Duration.ofSeconds(30));

            produceRecords(topic, 15);

            cluster.reviveBroker(follower);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 3), Duration.ofSeconds(60));

            TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();
            Set<Integer> isrIds = new HashSet<>();
            description.partitions().get(0).isr().forEach(n -> isrIds.add(n.id()));
            assertTrue(isrIds.contains(follower),
                    "the revived broker (node " + follower + ") must have caught up and rejoined the ISR");
        }
    }

    @Test
    void minIsrRejectsWriteBelowThreshold() throws Exception {
        String topic = uniqueTopic("min-isr");
        try (Admin admin = newAdmin()) {
            createTopic(admin, topic, 1, (short) 2, java.util.Map.of("min.insync.replicas", "2"));
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 2), Duration.ofSeconds(30));

            // Below full strength (2 replicas up), acks=all must still succeed.
            produceOne(topic, true, Duration.ofSeconds(10));

            int aReplica = replicasOf(admin, topic, 0).get(0);
            cluster.killBroker(aReplica);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 1), Duration.ofSeconds(30));

            // Now ISR (1) is below min.insync.replicas (2) -- acks=all must be rejected.
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> produceOne(topic, true, Duration.ofSeconds(8)));
            assertTrue(
                    failure.getCause() instanceof org.apache.kafka.common.errors.NotEnoughReplicasException
                            || failure.getCause() instanceof org.apache.kafka.common.errors.TimeoutException,
                    "expected a not-enough-replicas or timeout failure, got: " + failure.getCause());

            cluster.reviveBroker(aReplica);
            waitUntil(() -> allPartitionsHaveIsrSize(admin, topic, 2), Duration.ofSeconds(60));
        }
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

    private static void createTopic(Admin admin, String topic, int partitions, short rf, java.util.Map<String, String> configs) throws Exception {
        NewTopic newTopic = new NewTopic(topic, partitions, rf);
        if (configs != null) {
            newTopic.configs(configs);
        }
        admin.createTopics(List.of(newTopic)).all().get();
    }

    private static boolean allPartitionsHaveIsrSize(Admin admin, String topic, int expectedIsrSize) {
        try {
            TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();
            for (TopicPartitionInfo partition : description.partitions()) {
                if (partition.isr().size() != expectedIsrSize) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static int leaderOf(Admin admin, String topic, int partition) throws Exception {
        Integer leader = leaderOfOrNull(admin, topic, partition);
        assertNotNull(leader, "partition " + partition + " has no leader");
        return leader;
    }

    private static Integer leaderOfOrNull(Admin admin, String topic, int partition) {
        try {
            TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();
            for (TopicPartitionInfo p : description.partitions()) {
                if (p.partition() == partition) {
                    return p.leader() == null ? null : p.leader().id();
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static int followerOf(Admin admin, String topic, int partition) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();
        for (TopicPartitionInfo p : description.partitions()) {
            if (p.partition() == partition) {
                int leaderId = p.leader().id();
                return p.replicas().stream().map(n -> n.id()).filter(id -> id != leaderId).findFirst()
                        .orElseThrow(() -> new IllegalStateException("no follower found"));
            }
        }
        throw new IllegalArgumentException("no such partition");
    }

    private static List<Integer> replicasOf(Admin admin, String topic, int partition) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();
        for (TopicPartitionInfo p : description.partitions()) {
            if (p.partition() == partition) {
                return p.replicas().stream().map(n -> n.id()).toList();
            }
        }
        throw new IllegalArgumentException("no such partition");
    }

    private static void produceRecords(String topic, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)));
            }
            for (var future : futures) {
                future.get();
            }
        }
    }

    private static void produceOne(String topic, boolean idempotenceOff, Duration timeout) throws ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        if (idempotenceOff) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        }
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis() - 1000);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            try {
                producer.send(new ProducerRecord<>(topic, "key", "value")).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static List<String> consumeAtLeast(String topic, int minCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<String> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    collected.add(record.value());
                }
            }
        }
        return collected;
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
