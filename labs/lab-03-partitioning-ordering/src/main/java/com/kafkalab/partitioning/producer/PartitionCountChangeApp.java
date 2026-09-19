package com.kafkalab.partitioning.producer;

import com.kafkalab.partitioning.support.LabConfig;
import com.kafkalab.partitioning.support.TopicInspector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * The mandatory partition-count-change experiment (WP-04 section 20-23).
 *
 * <p>Run in two phases against the SAME topic and the SAME deterministic
 * key set:
 * <pre>
 * ./gradlew runPartitionMappingBefore              (topic has its initial partition count)
 * ... expand the topic's partition count via the CLI ...
 * ./gradlew runPartitionMappingAfter                (same keys, same topic, new partition count)
 * </pre>
 *
 * <p>The "before" phase writes its observed key-&gt;partition mapping to
 * {@code partition-mapping-before.local.properties} in this project
 * directory (gitignored; local experiment state, not source) so the
 * "after" phase can automatically compare against it rather than relying
 * on a human to remember or retype numbers. Every mapping printed by this
 * class is a real, observed {@code RecordMetadata.partition()} value from
 * this run -- nothing here is invented.
 *
 * <p>Verified against {@code kafka-clients:4.3.1} (see
 * docs/partitioning/PARTITIONING_AND_ORDERING.md): a keyed record's
 * partition is {@code murmur2(serializedKeyBytes) % currentPartitionCount},
 * recomputed against whatever the CURRENT partition count is at send time.
 * That single fact is why partition-count changes can remap a key: the
 * modulus itself changed. It also produces a precise, checkable
 * consequence when the partition count is exactly doubled (e.g. 3 -&gt; 6):
 * for any key, {@code newPartition} must be either {@code oldPartition} or
 * {@code oldPartition + oldPartitionCount} -- never anything else. This
 * class checks that claim against its own real output rather than just
 * asserting it in prose.
 */
public final class PartitionCountChangeApp {

    private static final int KEY_COUNT = 30;
    private static final Path MAPPING_FILE = Path.of("partition-mapping-before.local.properties");

    public static void main(String[] args) throws Exception {
        String phase = LabConfig.get("phase", "before");
        String topic = LabConfig.topic();
        String bootstrapServers = LabConfig.bootstrapServers();

        int currentPartitionCount = TopicInspector.partitionCount(bootstrapServers, topic);
        Map<String, Integer> mapping = produceAndCaptureMapping(bootstrapServers, topic);

        System.out.printf("Phase: %s -- topic %s currently has %d partition(s)%n", phase, topic, currentPartitionCount);
        mapping.forEach((key, partition) -> System.out.printf("  %s -> partition %d%n", key, partition));

        if ("before".equalsIgnoreCase(phase)) {
            saveMapping(mapping, currentPartitionCount);
            System.out.println();
            System.out.println("Saved this mapping to " + MAPPING_FILE.toAbsolutePath());
            System.out.println("Now increase this topic's partition count via the Kafka CLI (see the");
            System.out.println("README), then run ./gradlew runPartitionMappingAfter.");
        } else {
            compareToSavedMapping(mapping, currentPartitionCount);
        }
    }

    private static Map<String, Integer> produceAndCaptureMapping(String bootstrapServers, String topic) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Map<String, Integer> mapping = new LinkedHashMap<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= KEY_COUNT; i++) {
                String key = String.format("order-%06d", i);
                var metadata = producer.send(new ProducerRecord<>(topic, key, "CREATED")).get();
                mapping.put(key, metadata.partition());
            }
        }
        return mapping;
    }

    private static void saveMapping(Map<String, Integer> mapping, int partitionCount) throws IOException {
        Properties props = new Properties();
        props.setProperty("_partitionCount", String.valueOf(partitionCount));
        mapping.forEach((key, partition) -> props.setProperty(key, String.valueOf(partition)));
        try (OutputStream out = Files.newOutputStream(MAPPING_FILE)) {
            props.store(out, "lab-03 partition-count-change experiment: key -> partition, before expansion");
        }
    }

    private static void compareToSavedMapping(Map<String, Integer> afterMapping, int afterPartitionCount) throws IOException {
        if (!Files.exists(MAPPING_FILE)) {
            System.out.println();
            System.out.println("No " + MAPPING_FILE + " found -- run ./gradlew runPartitionMappingBefore first.");
            return;
        }

        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(MAPPING_FILE)) {
            props.load(in);
        }
        int beforePartitionCount = Integer.parseInt(props.getProperty("_partitionCount"));

        System.out.println();
        System.out.printf("Comparing against the mapping saved before expansion (then %d partitions, now %d):%n",
                beforePartitionCount, afterPartitionCount);
        System.out.printf("%-14s %-10s %-10s %-10s%n", "Key", "Before", "After", "Remapped?");

        Map<String, Integer> sortedAfter = new TreeMap<>(afterMapping);
        int remappedCount = 0;
        int checkedCount = 0;
        boolean doubling = afterPartitionCount == beforePartitionCount * 2;
        boolean allRemapsExplainedByDoubling = true;

        for (Map.Entry<String, Integer> entry : sortedAfter.entrySet()) {
            String key = entry.getKey();
            int after = entry.getValue();
            String beforeStr = props.getProperty(key);
            if (beforeStr == null) {
                continue;
            }
            int before = Integer.parseInt(beforeStr);
            boolean remapped = before != after;
            System.out.printf("%-14s %-10d %-10d %-10s%n", key, before, after, remapped ? "YES" : "no");
            checkedCount++;
            if (remapped) {
                remappedCount++;
                if (doubling && after != before && after != before + beforePartitionCount) {
                    allRemapsExplainedByDoubling = false;
                }
            }
        }

        System.out.println();
        System.out.printf("%d of %d keys remapped to a different partition after the topology change.%n",
                remappedCount, checkedCount);
        System.out.println("Existing, already-written records did NOT move -- only where FUTURE");
        System.out.println("records for these keys land has changed. Kafka did not \"rebalance\"");
        System.out.println("old data across the new partitions; nothing in this client does that.");

        if (doubling) {
            System.out.printf(
                    "Partition count exactly doubled (%d -> %d): every remapped key's new partition"
                            + " matched old-partition or old-partition+%d, as murmur2(key) %% N predicts: %s%n",
                    beforePartitionCount, afterPartitionCount, beforePartitionCount, allRemapsExplainedByDoubling
            );
        }
    }
}
