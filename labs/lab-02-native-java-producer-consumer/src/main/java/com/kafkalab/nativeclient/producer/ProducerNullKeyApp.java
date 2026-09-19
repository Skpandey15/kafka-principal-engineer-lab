package com.kafkalab.nativeclient.producer;

import com.kafkalab.nativeclient.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Experiment: null-key records — observe the actual distribution, and stop
 * there.
 *
 * <p>This class deliberately does not print a conclusion like "records were
 * round-robin distributed" or any other named strategy. Run it against the
 * pinned Kafka client and look at the raw per-partition counts it prints;
 * that observed result — for this client version, this run, this batch
 * timing — is the only thing this experiment entitles you to claim. Do not
 * generalize it into a rule for "how Kafka partitions null-key records" in
 * general — see the lab README and
 * docs/architecture/KAFKA_MENTAL_MODEL.md's partitioning section for why:
 * null-key partitioning behavior is client-version-dependent and can be
 * influenced by batching/sticky-partition behavior, not a fixed, documented
 * round-robin contract.
 *
 * <p>The durable lesson this experiment supports is narrower than a
 * specific distribution pattern:
 *
 * <pre>
 * keyed records      -> partition affinity (see ProducerKeyAffinityApp)
 * null-key records   -> no business-key partition affinity guarantee
 * </pre>
 */
public final class ProducerNullKeyApp {

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        int count = Integer.parseInt(LabConfig.get("count", "6"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Map<Integer, Integer> countsByPartition = new LinkedHashMap<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                // No key at all -- this is not "an empty string key," it is
                // a genuinely null key, which is what removes any
                // key-based partition affinity.
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, "NULL-KEY-EVENT-" + i);
                var metadata = producer.send(record).get();
                System.out.printf("null-key record %d -> partition=%d%n", i, metadata.partition());
                countsByPartition.merge(metadata.partition(), 1, Integer::sum);
            }
        }

        System.out.println();
        System.out.println("Observed distribution (this run only, this client version):");
        countsByPartition.forEach((partition, sends) ->
                System.out.printf("  partition %d: %d record(s)%n", partition, sends));
        System.out.println();
        System.out.println("Do not generalize this into a universal null-key partitioning rule --");
        System.out.println("see the lab README for why.");
    }
}
