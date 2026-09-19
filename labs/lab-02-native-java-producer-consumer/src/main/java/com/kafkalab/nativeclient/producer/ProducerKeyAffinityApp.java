package com.kafkalab.nativeclient.producer;

import com.kafkalab.nativeclient.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Experiment: same key, sent repeatedly — prove partition affinity for
 * yourself instead of taking it on faith.
 *
 * <p>This class sends the same key several times in a row and prints the
 * partition each one landed on. Run it, and every line should show the
 * same partition number, for as long as the topic's partition count does
 * not change between sends. That is the actual, provable claim this
 * experiment supports:
 *
 * <pre>
 * Records with the same key are routed consistently according to the
 * active partitioning behavior, while the topic's partition topology
 * remains unchanged.
 * </pre>
 *
 * <p>This class deliberately does not claim, or depend on, a specific
 * hashing algorithm — see docs/architecture/KAFKA_MENTAL_MODEL.md's
 * partitioning section and the lab README for why the exact algorithm is
 * version-dependent and is a topic for the future partitioning lab
 * (WP-04), not this one. What matters here is the observed *consistency*,
 * not the mechanism producing it.
 */
public final class ProducerKeyAffinityApp {

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        String key = LabConfig.get("key", "order-1001");
        int count = Integer.parseInt(LabConfig.get("count", "5"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, "EVENT-" + i);
                // Blocking on get() here (instead of a callback) is
                // deliberate for this one experiment: we want each send's
                // partition printed in send order, not interleaved
                // arbitrarily as callbacks complete on the background
                // sender thread. This is a demo-only simplification -- see
                // the lab README's note on why blocking on every send()
                // this way is the wrong pattern for a real producer.
                var metadata = producer.send(record).get();
                System.out.printf(
                        "key=%s -> topic=%s partition=%d offset=%d%n",
                        key, metadata.topic(), metadata.partition(), metadata.offset()
                );
            }
        }

        System.out.println();
        System.out.println("Every line above should show the same partition number.");
        System.out.println("That consistency, not the specific number, is what this experiment proves.");
    }
}
