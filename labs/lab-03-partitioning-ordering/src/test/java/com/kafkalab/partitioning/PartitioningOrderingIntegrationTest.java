package com.kafkalab.partitioning;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real, containerized, KRaft-mode Kafka broker (same {@code apache/kafka}
 * image/version as {@code platform/kafka/} and lab-02) proving the three
 * architectural properties WP-04 is built around. No mocked
 * producer/consumer, per this repository's testing strategy.
 */
@Testcontainers
class PartitioningOrderingIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Test
    void sameKeyRecordsShareOnePartitionUnderStableTopology() throws Exception {
        String topic = "affinity-it-" + UUID.randomUUID();
        createTopic(topic, 6, (short) 1);

        String key = "order-1001";
        Set<Integer> observedPartitions = new HashSet<>();

        Properties producerProps = producerProps();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 20; i++) {
                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, "EVENT-" + i)).get();
                observedPartitions.add(metadata.partition());
            }
        }

        assertEquals(
                1, observedPartitions.size(),
                "20 sends with the same key, same unchanged topic topology, should all land on one partition; observed: " + observedPartitions
        );
    }

    @Test
    void perPartitionOrderingIsPreservedAcrossProduceAndConsume() throws Exception {
        String topic = "ordering-it-" + UUID.randomUUID();
        createTopic(topic, 3, (short) 1);

        String key = "order-2002";
        List<String> sentValues = List.of("CREATED", "PAID", "PACKED", "SHIPPED");

        Properties producerProps = producerProps();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (String value : sentValues) {
                producer.send(new ProducerRecord<>(topic, key, value)).get();
            }
        }

        List<ConsumerRecord<String, String>> consumed = consumeAll(topic, sentValues.size());
        List<String> consumedValues = consumed.stream()
                .sorted((a, b) -> Long.compare(a.offset(), b.offset()))
                .map(ConsumerRecord::value)
                .toList();

        assertEquals(sentValues, consumedValues, "events for one key on one partition must be read back in send order");

        long distinctPartitions = consumed.stream().map(ConsumerRecord::partition).distinct().count();
        assertEquals(1, distinctPartitions, "all four events for this single key should share one partition");

        long previousOffset = -1;
        for (ConsumerRecord<String, String> record : consumed.stream().sorted((a, b) -> Long.compare(a.offset(), b.offset())).toList()) {
            assertTrue(record.offset() > previousOffset, "offsets within a partition must be strictly increasing");
            previousOffset = record.offset();
        }
    }

    @Test
    void increasingPartitionCountRemapsAtLeastSomeExistingKeys() throws Exception {
        String topic = "expansion-it-" + UUID.randomUUID();
        int initialPartitions = 3;
        int expandedPartitions = 12;
        createTopic(topic, initialPartitions, (short) 1);

        // A large-enough deterministic key set that asserting ">= 1 remap"
        // is a real architectural check, not a coin flip -- with this many
        // keys and a 3->12 partition change, expecting zero remaps would
        // itself be the statistically implausible outcome.
        List<String> keys = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            keys.add(String.format("order-%06d", i));
        }

        Map<String, Integer> before = produceAndCaptureMapping(topic, keys);

        try (Admin admin = Admin.create(adminProps())) {
            admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(expandedPartitions))).all().get();
        }
        waitUntilPartitionCountIs(topic, expandedPartitions);

        Map<String, Integer> after = produceAndCaptureMapping(topic, keys);

        int remapped = 0;
        for (String key : keys) {
            if (!before.get(key).equals(after.get(key))) {
                remapped++;
            }
        }

        assertTrue(
                remapped > 0,
                "expected at least one of " + keys.size() + " keys to map to a different partition after "
                        + initialPartitions + " -> " + expandedPartitions + " partitions; observed 0 remaps"
        );

        // Existing behavior this test does NOT claim: it never re-reads
        // "before" data after expansion, because expansion does not move
        // it -- there is nothing to re-read differently. This test only
        // checks where FUTURE records for the same keys land.
    }

    private static Map<String, Integer> produceAndCaptureMapping(String topic, List<String> keys) throws Exception {
        Map<String, Integer> mapping = new HashMap<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (String key : keys) {
                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, "CREATED@" + Instant.now())).get();
                mapping.put(key, metadata.partition());
            }
        }
        return mapping;
    }

    private static void waitUntilPartitionCountIs(String topic, int expected) throws Exception {
        try (Admin admin = Admin.create(adminProps())) {
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                int actual = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().size();
                if (actual >= expected) {
                    return;
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("topic " + topic + " did not reach " + expected + " partitions in time");
    }

    private static void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        try (Admin admin = Admin.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, replicationFactor))).all().get();
        }
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static List<ConsumerRecord<String, String>> consumeAll(String topic, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(30);
            while (collected.size() < expectedCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
        }
        return collected;
    }
}
