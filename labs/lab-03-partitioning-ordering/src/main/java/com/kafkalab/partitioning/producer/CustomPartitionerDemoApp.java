package com.kafkalab.partitioning.producer;

import com.kafkalab.partitioning.support.FirstLetterPartitioner;
import com.kafkalab.partitioning.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * Experiment: a custom {@code partitioner.class} in action -- and why
 * this repository does not recommend reaching for one by default.
 *
 * <p>This is not "look, we can write our own partitioner." It exists to
 * make the real cost concrete: the moment {@link FirstLetterPartitioner}'s
 * routing rule is live, it is a long-lived compatibility contract --
 * <ul>
 *   <li><b>Producer rollout consistency</b> -- every producer application
 *       writing to this topic must use this exact class (or something
 *       API-compatible with identical routing decisions), or different
 *       producers will route the same logical key differently.</li>
 *   <li><b>Multiple producer applications</b> -- a change to this class
 *       has to roll out to every one of them, coordinated, or they
 *       disagree mid-rollout.</li>
 *   <li><b>Algorithm versioning</b> -- there is no built-in mechanism to
 *       version this routing rule the way Kafka versions its own wire
 *       protocol; a routing-logic change is an application-level
 *       migration you own entirely.</li>
 *   <li><b>Partition-count changes</b> -- like the built-in partitioner,
 *       this one recomputes against whatever {@code numPartitions} is at
 *       send time, so it has the exact same remapping behavior
 *       {@code PartitionCountChangeApp} demonstrates -- a custom
 *       partitioner does not exempt you from that.</li>
 *   <li><b>Skew</b> -- a hand-written rule can just as easily create a hot
 *       partition as a bad key choice can (a two-way alphabet split is
 *       already a plausible source of imbalance if key first-letters
 *       aren't themselves uniform).</li>
 *   <li><b>Testing and migration</b> -- this logic now needs its own
 *       tests, and migrating away from it later means reasoning about
 *       every key that was ever routed under it.</li>
 * </ul>
 *
 * <p>Reach for a custom partitioner only when a specific business
 * requirement -- not convenience -- justifies owning this contract.
 */
public final class CustomPartitionerDemoApp {

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        String bootstrapServers = LabConfig.bootstrapServers();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, FirstLetterPartitioner.class.getName());

        List<String> keys = List.of("apple-1", "banana-2", "cherry-3", "walnut-4", "yam-5", "zebra-6");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String key : keys) {
                var metadata = producer.send(new ProducerRecord<>(topic, key, "EVENT")).get();
                System.out.printf("key=%-10s -> partition=%d (custom FirstLetterPartitioner)%n", key, metadata.partition());
            }
        }

        System.out.println();
        System.out.println("Keys starting A-M and N-Z landed in two different halves of this");
        System.out.println("topic's partitions -- a routing rule murmur2(key) % partitions has no");
        System.out.println("way to express, because it has no idea what a key's first letter means.");
        System.out.println("That expressiveness is the entire benefit; the class-level Javadoc above");
        System.out.println("is the cost.");
    }
}
