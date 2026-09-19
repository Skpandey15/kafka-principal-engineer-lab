package com.kafkalab.nativeclient.producer;

import com.kafkalab.nativeclient.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Experiment: Producer basics — send(), the callback, and flush()/close().
 *
 * <p>This is the smallest possible native Kafka producer that still lets you
 * observe the thing the lab exists to teach: {@code send()} does not return
 * a broker-confirmed result. It returns a {@link java.util.concurrent.Future}
 * (here observed via a callback instead) that completes later, once the
 * record has actually been appended and acknowledged according to {@code
 * acks}. Read the mental model
 * (docs/architecture/KAFKA_MENTAL_MODEL.md) before this class if you haven't
 * already — this code is the Java-shaped version of that document's
 * end-to-end path.
 */
public final class ProducerBasicApp {

    public static void main(String[] args) throws InterruptedException {
        String topic = LabConfig.topic();

        Properties props = new Properties();
        // The three properties every KafkaProducer needs. Nothing else is
        // set here on purpose — see the lab README's "Do not over-tune
        // configuration" section for why a longer property list belongs to
        // a later, more advanced lab, not this one.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // try-with-resources calls close() for us — see "Flush and close"
        // in the README for what close() actually does and why simply
        // letting the JVM exit instead is dangerous for a real producer.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String[] keys = {"order-1001", "order-1002", "order-1003"};
            String[] values = {"CREATED", "CREATED", "CREATED"};

            // A latch only so this short-lived demo program can wait for
            // every callback before exiting — it is not part of the Kafka
            // API and is not something a long-running application needs.
            CountDownLatch pending = new CountDownLatch(keys.length);

            for (int i = 0; i < keys.length; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, keys[i], values[i]);

                // send() returns immediately; the application thread is
                // free to move on to the next line before this record has
                // necessarily even left the process. Everything below
                // happens later, on Kafka's own network/I/O thread, and
                // reports back through this callback.
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.out.printf(
                                "Send FAILED: key=%s error=%s%n",
                                record.key(), exception
                        );
                    } else {
                        System.out.printf(
                                "Sent: key=%s topic=%s partition=%d offset=%d timestamp=%d%n",
                                record.key(), metadata.topic(), metadata.partition(),
                                metadata.offset(), metadata.timestamp()
                        );
                    }
                    pending.countDown();
                });

                System.out.printf("send() returned for key=%s -- broker has not necessarily stored it yet%n", keys[i]);
            }

            // flush() blocks until every previously-sent record has
            // completed (successfully or not) -- it exists precisely
            // because send() does not wait. Without it (or without close(),
            // which flushes internally), this program could exit before
            // the background sender thread has actually written these
            // records to the broker.
            producer.flush();

            // Belt-and-suspenders for this demo: wait for every callback to
            // have actually run and printed its result before main() (and
            // therefore the try-with-resources close()) proceeds.
            pending.await(30, TimeUnit.SECONDS);
        }
        // close() has now run (via try-with-resources): any buffered
        // records are flushed, the background sender thread is stopped,
        // and network connections are released. See the README for why
        // skipping this (e.g., killing the JVM instead) risks silently
        // dropping records that send() had accepted but not yet delivered.
    }
}
