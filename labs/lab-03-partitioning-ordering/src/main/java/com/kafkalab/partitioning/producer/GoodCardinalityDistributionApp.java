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
 * Experiment: high-cardinality keys, measured at scale.
 *
 * <p>Sends {@code count} records (default 10,000), each with a distinct
 * key ({@code order-000001}, {@code order-000002}, ...) so there are as
 * many distinct key values as there are records -- the highest cardinality
 * this experiment can practically demonstrate. This is a DISTRIBUTION
 * experiment, not a throughput benchmark: sends are async with a counting
 * callback, never {@code send(record).get()} per record, so the measured
 * elapsed time reflects how fast this batch could be produced, not how
 * fast a synchronous round trip is -- see the lab README's "Measure,
 * don't guess" and "Performance experiment discipline" notes.
 *
 * <p>The durable lesson (see docs/partitioning/PARTITIONING_AND_ORDERING.md):
 * good cardinality gives the partitioner enough distinct key values to
 * distribute load across every partition -- it does not promise
 * mathematically perfect balance, and this report's own numbers are the
 * proof either way.
 */
public final class GoodCardinalityDistributionApp {

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
            for (int i = 1; i <= count; i++) {
                String key = String.format("order-%06d", i);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, "CREATED");
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        countsByPartition.computeIfAbsent(metadata.partition(), p -> new LongAdder()).increment();
                    } else {
                        System.out.println("send failed for key=" + key + ": " + exception);
                    }
                    pending.countDown();
                });
            }
            producer.flush();
            boolean completed = pending.await(60, TimeUnit.SECONDS);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            System.out.printf(
                    "Sent %d high-cardinality records in %dms (all callbacks completed: %s)%n",
                    count, elapsedMs, completed
            );
        }

        Map<Integer, Long> finalCounts = new HashMap<>();
        countsByPartition.forEach((partition, adder) -> finalCounts.put(partition, adder.sum()));
        PartitionDistributionReport.print(
                "Partition Distribution -- high-cardinality keys (" + count + " distinct orders)",
                finalCounts, partitionCount
        );
    }
}
