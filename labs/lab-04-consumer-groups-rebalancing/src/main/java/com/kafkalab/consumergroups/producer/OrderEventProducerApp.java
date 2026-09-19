package com.kafkalab.consumergroups.producer;

import com.kafkalab.consumergroups.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

/**
 * Seeds {@code orders.consumer-group.lab} with keyed order events. This
 * class is deliberately not the subject of this lab -- WP-04's
 * {@code BusinessOrderingApp} and {@code GoodCardinalityDistributionApp}
 * already cover partition selection and keyed-record affinity in depth.
 * Here, producing is only the means to have something for consumer-group
 * experiments to consume.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@code burst} (default) -- sends {@code count} records once and
 *       exits. Used for the ownership/scaling/independent-group
 *       experiments, where a finite, known amount of data is easier to
 *       reason about.</li>
 *   <li>{@code continuous} -- sends one record every {@code delayMs}
 *       indefinitely, until interrupted. Used for the rebalance-impact
 *       experiment (Experiment D in the README), where a live workload is
 *       needed while consumers are added or removed.</li>
 * </ul>
 */
public final class OrderEventProducerApp {

    private static final int DISTINCT_CUSTOMERS = 20;

    public static void main(String[] args) throws InterruptedException {
        String topic = LabConfig.topic();
        String mode = LabConfig.get("mode", "burst");
        int count = Integer.parseInt(LabConfig.get("count", "30"));
        long delayMs = Long.parseLong(LabConfig.get("delayMs", "500"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            if ("continuous".equalsIgnoreCase(mode)) {
                System.out.println("Producing continuously, one record every " + delayMs + "ms. Press Ctrl+C to stop.");
                long i = 0;
                while (true) {
                    sendOne(producer, topic, i++);
                    Thread.sleep(delayMs);
                }
            } else {
                System.out.println("Producing " + count + " records (burst mode).");
                for (long i = 0; i < count; i++) {
                    sendOne(producer, topic, i);
                }
                producer.flush();
                System.out.println("Done.");
            }
        }
    }

    private static void sendOne(KafkaProducer<String, String> producer, String topic, long i) {
        String key = "CUSTOMER-" + (100 + (i % DISTINCT_CUSTOMERS));
        String value = "ORDER-EVENT-" + i + "@" + Instant.now();
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                System.out.println("send failed for key=" + key + ": " + exception);
            } else {
                System.out.printf("sent key=%-14s -> partition=%d offset=%d%n", key, metadata.partition(), metadata.offset());
            }
        });
    }
}
