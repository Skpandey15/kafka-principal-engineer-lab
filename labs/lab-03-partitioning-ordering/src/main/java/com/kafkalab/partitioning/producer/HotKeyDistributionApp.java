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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Experiment: a skewed key workload -- a single hot key dominating
 * traffic, even though the overall key space has good cardinality.
 *
 * <p>90% of records use the single key {@code customer-VIP}; the
 * remaining 10% are spread across many distinct {@code customer-normal-*}
 * keys. This is deliberately NOT the same failure as the low-cardinality
 * experiment: the key space here is large (thousands of distinct normal
 * customers are possible), but traffic is not even close to uniform
 * across it -- which is exactly {@link #main} teaches: cardinality alone
 * does not predict skew.
 *
 * <p>The architectural point this measures (see
 * docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md's hot-partition row
 * and docs/partitioning/PARTITIONING_AND_ORDERING.md): a hot key maps,
 * by design, to exactly one partition -- so a hot key becomes a hot
 * partition, and a hot partition is a real capacity problem even on a
 * completely healthy cluster.
 */
public final class HotKeyDistributionApp {

    private static final String HOT_KEY = "customer-VIP";
    private static final double HOT_KEY_FRACTION = 0.90;

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
        Map<String, Integer> countsByKeyKind = new ConcurrentHashMap<>();
        CountDownLatch pending = new CountDownLatch(count);

        long startNanos = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                boolean isHot = ThreadLocalRandom.current().nextDouble() < HOT_KEY_FRACTION;
                String key = isHot
                        ? HOT_KEY
                        : "customer-normal-" + ThreadLocalRandom.current().nextInt(1, 5000);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, "ORDER-" + i);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        countsByPartition.computeIfAbsent(metadata.partition(), p -> new LongAdder()).increment();
                    }
                    countsByKeyKind.merge(isHot ? "hot (customer-VIP)" : "normal (customer-normal-*)", 1, Integer::sum);
                    pending.countDown();
                });
            }
            producer.flush();
            boolean completed = pending.await(60, TimeUnit.SECONDS);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            System.out.printf(
                    "Sent %d records in %dms (all callbacks completed: %s)%n",
                    count, elapsedMs, completed
            );
        }

        System.out.println();
        System.out.println("Records by key kind:");
        countsByKeyKind.forEach((kind, n) -> System.out.printf("  %-30s %d (%.1f%%)%n", kind, n, 100.0 * n / count));

        Map<Integer, Long> finalCounts = new HashMap<>();
        countsByPartition.forEach((partition, adder) -> finalCounts.put(partition, adder.sum()));
        PartitionDistributionReport.print(
                "Partition Distribution -- hot key (customer-VIP = " + (int) (HOT_KEY_FRACTION * 100) + "% of traffic)",
                finalCounts, partitionCount
        );

        System.out.println();
        System.out.println("Diagnostic mindset this maps to in production: high consumer lag ->");
        System.out.println("check partition-level lag -> check throughput by partition -> check");
        System.out.println("key distribution -> identify the hot key behind the hot partition.");
        System.out.println("The cluster can be completely healthy while this happens -- it is not");
        System.out.println("necessarily \"Kafka is slow,\" it can be \"this key distribution is skewed.\"");
    }
}
