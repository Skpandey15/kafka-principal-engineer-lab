package com.kafkalab.partitioning.producer;

import com.kafkalab.partitioning.support.LabConfig;
import com.kafkalab.partitioning.support.PartitionDistributionReport;
import com.kafkalab.partitioning.support.TopicInspector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Experiment: null-key distribution, extended from WP-03's small
 * qualitative demo to a measurable scale.
 *
 * <p>Verified against the real {@code kafka-clients:4.3.1} source
 * (see docs/partitioning/PARTITIONING_AND_ORDERING.md's source-reading
 * section): a null-key record does NOT hash to a partition and does NOT
 * round-robin. It gets {@code RecordMetadata.UNKNOWN_PARTITION} from
 * {@code KafkaProducer.partition(...)}, and the actual partition is chosen
 * later, inside {@code RecordAccumulator}, by {@code BuiltInPartitioner} --
 * KIP-794's "adaptive sticky partitioning": batches are stuck to one
 * partition at a time (to build efficient batches) and, once load
 * statistics exist, new partition choices are weighted toward
 * less-loaded partitions rather than picked uniformly at random.
 *
 * <p>This class does not assert what distribution you will see -- it
 * measures it, at {@code count} records, and prints the real result.
 */
public final class NullKeyDistributionApp {

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        String bootstrapServers = LabConfig.bootstrapServers();
        int count = Integer.parseInt(LabConfig.get("count", "10000"));

        int partitionCount = TopicInspector.partitionCount(bootstrapServers, topic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Map<Integer, LongAdder> countsByPartition = new ConcurrentHashMap<>();
        CountDownLatch pending = new CountDownLatch(count);

        long startNanos = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, "NULL-KEY-" + i);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        countsByPartition.computeIfAbsent(metadata.partition(), p -> new LongAdder()).increment();
                    }
                    pending.countDown();
                });
            }
            producer.flush();
            boolean completed = pending.await(60, TimeUnit.SECONDS);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            System.out.printf(
                    "Sent %d null-key records in %dms (all callbacks completed: %s)%n",
                    count, elapsedMs, completed
            );
        }

        Map<Integer, Long> finalCounts = new HashMap<>();
        countsByPartition.forEach((partition, adder) -> finalCounts.put(partition, adder.sum()));
        PartitionDistributionReport.print(
                "Partition Distribution -- null-key records (" + count + " records)",
                finalCounts, partitionCount
        );

        System.out.println();
        System.out.println("Do not generalize this distribution into \"null keys are round-robin\"");
        System.out.println("or into any other fixed rule -- it reflects this run, this client");
        System.out.println("version's adaptive/sticky batching behavior, and current partition");
        System.out.println("load, not a documented API guarantee.");
    }
}
