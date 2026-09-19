package com.kafkalab.nativeclient.producer;

import com.kafkalab.nativeclient.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Experiment: bypass the partitioner entirely by naming a partition
 * explicitly on the {@link ProducerRecord}.
 *
 * <p>{@code ProducerRecord} has a constructor that takes a partition number
 * directly, alongside (or instead of) a key. This class exists to show that
 * the capability is real and simple to use — and then to make the case, in
 * the README, for why ordinary business code almost never should:
 *
 * <ul>
 *   <li>It couples your application directly to this topic's current
 *       partition count and layout, rather than letting the partitioner
 *       (and, for keyed records, the key) decide.</li>
 *   <li>If the topic's partition count changes later, a hard-coded
 *       partition number does not adapt — it just keeps writing to
 *       whatever partition that number now refers to, silently.</li>
 *   <li>It creates an easy path to an accidental hot partition: every
 *       caller of this code path writes to the same fixed partition
 *       regardless of key or volume.</li>
 *   <li>It gives up the flexibility a key-based partitioner gives you for
 *       free: consistent per-key routing without your code needing to know
 *       or care how many partitions the topic has.</li>
 * </ul>
 */
public final class ProducerExplicitPartitionApp {

    public static void main(String[] args) throws Exception {
        String topic = LabConfig.topic();
        int partition = Integer.parseInt(LabConfig.get("partition", "0"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // (topic, partition, key, value) -- the partitioner is never
            // consulted for this record. Whatever key is supplied here is
            // still stored with the record, but it plays no role in
            // choosing where the record goes.
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, partition, "order-9999", "EXPLICIT-PARTITION-EVENT");

            var metadata = producer.send(record).get();
            System.out.printf(
                    "Sent with explicit partition request: requested=%d actual=%d offset=%d%n",
                    partition, metadata.partition(), metadata.offset()
            );
            System.out.println();
            System.out.println("requested == actual here because the partition you named exists.");
            System.out.println("If it did not (e.g., you asked for a partition beyond the topic's");
            System.out.println("current partition count), send() would fail instead of falling back");
            System.out.println("to the partitioner -- explicit means explicit.");
        }
    }
}
