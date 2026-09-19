package com.kafkalab.partitioning.producer;

import com.kafkalab.partitioning.support.LabConfig;
import com.kafkalab.partitioning.support.PartitionDistributionReport;
import com.kafkalab.partitioning.support.TopicInspector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Experiment: a deliberately poor partition key.
 *
 * <p>Keys are drawn only from {@code IN}, {@code US}, {@code UK} --
 * three distinct values, sent against a topic with many more partitions
 * than that (see the README's Setup for the exact partition count this
 * experiment expects; intentionally more than 3). Because a keyed
 * record's partition is {@code murmur2(keyBytes) % partitionCount}
 * (verified in docs/partitioning/PARTITIONING_AND_ORDERING.md), only as
 * many distinct partitions as there are distinct key values can ever
 * receive traffic here -- at most 3, no matter how many partitions the
 * topic has.
 *
 * <p>The lesson this measures directly:
 * <pre>
 * many partitions + low-cardinality key != parallelism
 * </pre>
 */
public final class LowCardinalityDistributionApp {

    private static final List<String> COUNTRIES = List.of("IN", "US", "UK");

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        String bootstrapServers = LabConfig.bootstrapServers();
        int count = Integer.parseInt(LabConfig.get("count", "10000"));

        int partitionCount = TopicInspector.partitionCount(bootstrapServers, topic);
        if (partitionCount <= COUNTRIES.size()) {
            System.out.printf(
                    "WARNING: topic %s has only %d partition(s) -- this experiment is most"
                            + " instructive against a topic with meaningfully more partitions than"
                            + " the %d distinct key values it sends. See the README's Setup.%n",
                    topic, partitionCount, COUNTRIES.size()
            );
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Map<Integer, LongAdder> countsByPartition = new ConcurrentHashMap<>();
        Map<String, Integer> partitionByCountry = new ConcurrentHashMap<>();
        CountDownLatch pending = new CountDownLatch(count);

        long startNanos = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String country = COUNTRIES.get(ThreadLocalRandom.current().nextInt(COUNTRIES.size()));
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, country, "PAYMENT-" + i);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        countsByPartition.computeIfAbsent(metadata.partition(), p -> new LongAdder()).increment();
                        partitionByCountry.putIfAbsent(country, metadata.partition());
                    }
                    pending.countDown();
                });
            }
            producer.flush();
            boolean completed = pending.await(60, TimeUnit.SECONDS);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            System.out.printf(
                    "Sent %d records across %d distinct key values in %dms (all callbacks completed: %s)%n",
                    count, COUNTRIES.size(), elapsedMs, completed
            );
        }

        System.out.println();
        System.out.println("Key -> partition (each of the 3 keys is internally consistent, per");
        System.out.println("the same affinity BusinessOrderingApp demonstrated):");
        partitionByCountry.forEach((country, partition) -> System.out.printf("  %s -> partition %d%n", country, partition));

        Map<Integer, Long> finalCounts = new HashMap<>();
        countsByPartition.forEach((partition, adder) -> finalCounts.put(partition, adder.sum()));
        PartitionDistributionReport.print(
                "Partition Distribution -- low-cardinality key (country: IN/US/UK) across "
                        + partitionCount + " partitions",
                finalCounts, partitionCount
        );

        long activePartitions = finalCounts.values().stream().filter(c -> c > 0).count();
        System.out.println();
        System.out.printf(
                "%d of %d partitions received any traffic at all -- more partitions did not"
                        + " create more parallelism here, because the key only had %d distinct values.%n",
                activePartitions, partitionCount, COUNTRIES.size()
        );
    }
}
