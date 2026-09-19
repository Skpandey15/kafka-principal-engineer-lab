package com.kafkalab.partitioning.producer;

import com.kafkalab.partitioning.support.LabConfig;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Experiment: business ordering, and the difference between "Kafka orders
 * this" and "Kafka happens to preserve the order I need."
 *
 * <p>Kafka does not know what an "order" is. It sees a topic, a partition,
 * a key's bytes, and a value's bytes. The only ordering guarantee it makes
 * is: records appended to the SAME partition are appended, and later
 * fetched, in that append order. Business ordering -- "these four events
 * for order-1001 must be seen in this sequence" -- exists only because
 * this producer chose a partition key (the order id) that keeps all of
 * one order's events on the same partition, for as long as this topic's
 * partition count stays unchanged. See
 * docs/partitioning/PARTITIONING_AND_ORDERING.md for the full mental model
 * this class demonstrates.
 *
 * <p>This class both produces and consumes in one run, specifically so the
 * proof is self-contained: produce a realistic order lifecycle for several
 * orders, then read it back and show (a) each order's events are in order
 * within their partition, and (b) the overall consumption order across
 * orders is NOT a single global sequence -- it's an interleaving that
 * depends on partition assignment, not on when events were produced.
 */
public final class BusinessOrderingApp {

    private static final List<String> ORDER_IDS = List.of("order-1001", "order-1002", "order-1003");
    private static final List<String> LIFECYCLE = List.of("CREATED", "PAID", "PACKED", "SHIPPED");

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        String bootstrapServers = LabConfig.bootstrapServers();

        System.out.println("=== Phase 1: producing order-lifecycle events, keyed by orderId ===");
        List<SentRecord> sent = produceOrderLifecycles(bootstrapServers, topic);

        System.out.println();
        System.out.println("=== Phase 2: consuming everything back, from a fresh group ===");
        List<ConsumerRecord<String, String>> consumed = consumeAll(bootstrapServers, topic, sent.size());

        printRawConsumptionOrder(consumed);
        printPerPartitionSequences(consumed);
        printPerOrderSequences(consumed);
    }

    private record SentRecord(String key, String event, int partition, long offset) {
    }

    private static List<SentRecord> produceOrderLifecycles(String bootstrapServers, String topic) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        List<SentRecord> results = new ArrayList<>();
        // Blocking per send (producer.send(record).get()) is deliberate here:
        // this is a 12-record illustrative experiment, not a throughput
        // measurement -- see the distribution experiments
        // (GoodCardinalityDistributionApp etc.) for where async, non-blocking
        // sends are used instead because volume actually matters there.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String orderId : ORDER_IDS) {
                for (String event : LIFECYCLE) {
                    String value = event + "@" + Instant.now();
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, orderId, value);
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf(
                            "key=%s event=%-7s -> partition=%d offset=%d%n",
                            orderId, event, metadata.partition(), metadata.offset()
                    );
                    results.add(new SentRecord(orderId, event, metadata.partition(), metadata.offset()));
                }
            }
        }
        return results;
    }

    private static List<ConsumerRecord<String, String>> consumeAll(String bootstrapServers, String topic, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "business-ordering-" + UUID.randomUUID());
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

    private static void printRawConsumptionOrder(List<ConsumerRecord<String, String>> consumed) {
        System.out.println();
        System.out.println("--- Raw consumption order (as poll() returned it) ---");
        System.out.println("This is an interleaving across partitions, NOT a single global");
        System.out.println("business sequence -- do not read chronological meaning into it.");
        for (ConsumerRecord<String, String> record : consumed) {
            System.out.printf(
                    "partition=%d offset=%d key=%s value=%s%n",
                    record.partition(), record.offset(), record.key(), record.value()
            );
        }
    }

    private static void printPerPartitionSequences(List<ConsumerRecord<String, String>> consumed) {
        System.out.println();
        System.out.println("--- Per-partition sequence (sorted by offset) ---");
        Map<Integer, List<ConsumerRecord<String, String>>> byPartition = new LinkedHashMap<>();
        consumed.stream()
                .sorted((a, b) -> Integer.compare(a.partition(), b.partition()))
                .forEach(r -> byPartition.computeIfAbsent(r.partition(), p -> new ArrayList<>()).add(r));

        byPartition.forEach((partition, records) -> {
            records.sort((a, b) -> Long.compare(a.offset(), b.offset()));
            System.out.println("partition " + partition + ":");
            long previousOffset = -1;
            boolean monotonic = true;
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("  offset=%d key=%s value=%s%n", record.offset(), record.key(), record.value());
                if (record.offset() <= previousOffset) {
                    monotonic = false;
                }
                previousOffset = record.offset();
            }
            System.out.println("  monotonically increasing offsets: " + monotonic);
        });
    }

    private static void printPerOrderSequences(List<ConsumerRecord<String, String>> consumed) {
        System.out.println();
        System.out.println("--- Per-order (per-key) event sequence, as consumed ---");
        for (String orderId : ORDER_IDS) {
            List<ConsumerRecord<String, String>> forOrder = consumed.stream()
                    .filter(r -> orderId.equals(r.key()))
                    .sorted((a, b) -> Long.compare(a.offset(), b.offset()))
                    .toList();
            long distinctPartitions = forOrder.stream().map(ConsumerRecord::partition).distinct().count();
            System.out.printf("%s (all on partition %s -- %d distinct partition(s) observed):%n",
                    orderId,
                    distinctPartitions == 1 ? String.valueOf(forOrder.get(0).partition()) : "MULTIPLE",
                    distinctPartitions);
            for (ConsumerRecord<String, String> record : forOrder) {
                System.out.printf("  offset=%d value=%s%n", record.offset(), record.value());
            }
        }
        System.out.println();
        System.out.println("Under this topic's current (unchanged) partition count, every order's");
        System.out.println("events should share one partition -- that affinity is what lets the");
        System.out.println("per-partition ordering guarantee above double as business ordering here.");
    }
}
