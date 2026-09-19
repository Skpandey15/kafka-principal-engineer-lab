package com.kafkalab.nativeclient.producer;

import com.kafkalab.nativeclient.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

/**
 * Failure experiment: run this continuously, then stop and restart the
 * broker (`docker compose -f platform/kafka/docker-compose.yml stop` / `...
 * start`) while it's running, per the lab README's broker-unavailable
 * experiment.
 *
 * <p>Two configuration values are deliberately shortened from their
 * defaults, purely so this experiment produces an observable failure within
 * a reasonable lab-session timeframe instead of the default two minutes:
 *
 * <ul>
 *   <li>{@code request.timeout.ms} (default 30000) -&gt; 5000</li>
 *   <li>{@code delivery.timeout.ms} (default 120000) -&gt; 15000</li>
 * </ul>
 *
 * <p>This is a controlled adjustment to make one specific experiment
 * observable, not a production recommendation -- see the lab README's "Do
 * not over-tune configuration" section. Retries and idempotence are left at
 * their client defaults; this experiment is not about tuning those yet.
 */
public final class ProducerBrokerDownApp {

    public static void main(String[] args) throws InterruptedException {
        String topic = LabConfig.topic();
        int iterations = Integer.parseInt(LabConfig.get("iterations", "60"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);

        System.out.println("Sending one record per second. Stop the broker mid-run, then restart it.");
        System.out.println("Press Ctrl+C to stop early.");
        System.out.println();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < iterations; i++) {
                String key = "heartbeat-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, Instant.now().toString());

                long sentAt = System.currentTimeMillis();
                producer.send(record, (metadata, exception) -> {
                    long elapsedMs = System.currentTimeMillis() - sentAt;
                    if (exception != null) {
                        // This is the callback observing a failed send --
                        // note that send() itself already returned long
                        // before this runs. The Future/callback completing
                        // with an exception is how you find out a record
                        // was never durably stored; a returned Future by
                        // itself proves nothing about broker state.
                        System.out.printf(
                                "[%dms] key=%s FAILED: %s: %s%n",
                                elapsedMs, key, exception.getClass().getSimpleName(), exception.getMessage()
                        );
                    } else {
                        System.out.printf(
                                "[%dms] key=%s OK: partition=%d offset=%d%n",
                                elapsedMs, key, metadata.partition(), metadata.offset()
                        );
                    }
                });

                Thread.sleep(1_000);
            }
        }

        System.out.println();
        System.out.println("Done. Compare the FAILED window's timing against when you stopped/restarted");
        System.out.println("the broker, and against the request.timeout.ms/delivery.timeout.ms values");
        System.out.println("this class sets above.");
    }
}
