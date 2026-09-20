package com.kafkalab.controllerquorum.producer;

import com.kafkalab.controllerquorum.support.LabConfig;
import com.kafkalab.controllerquorum.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Produces to an already-created, already-replicated topic indefinitely,
 * one record every {@code delayMs} -- this lab's tool for observing
 * whether ordinary DATA-PLANE traffic continues while the CONTROL PLANE
 * (the controller quorum) is degraded or unavailable (README Experiments
 * 6, 10, 11). This app never creates a topic or does anything else that
 * requires a metadata mutation -- it only produces to an
 * already-existing partition, which is the entire point of the
 * distinction this lab is built around.
 */
public final class ContinuousProducerApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        long delayMs = LabConfig.getLong("delayMs", 500);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        System.out.printf("Starting continuous producer | topic=%s delayMs=%d%n", topic, delayMs);
        System.out.println("Press Ctrl+C to stop. This app never creates a topic or mutates metadata --");
        System.out.println("it only produces to the already-existing partitions, on purpose.");

        try {
            long i = 0;
            while (running.get()) {
                String eventId = "ORDER-CP-" + i;
                OrderEvent event = new OrderEvent(eventId, "CUSTOMER-100", 10.0 + (i % 25));
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "CUSTOMER-100", event.toWireFormat());

                long start = System.nanoTime();
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    RecordMetadata metadata = future.get(6, TimeUnit.SECONDS);
                    long latencyMs = (System.nanoTime() - start) / 1_000_000;
                    System.out.printf("%s | eventId=%-14s partition=%d offset=%d result=SUCCESS latencyMs=%d%n",
                            Instant.now(), eventId, metadata.partition(), metadata.offset(), latencyMs);
                } catch (Exception e) {
                    long latencyMs = (System.nanoTime() - start) / 1_000_000;
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    System.out.printf("%s | eventId=%-14s result=FAILED latencyMs=%d exception=%s message=%s%n",
                            Instant.now(), eventId, latencyMs, cause.getClass().getName(), cause.getMessage());
                }

                i++;
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running.set(false);
                }
            }
        } finally {
            producer.close();
            System.out.println("Continuous producer stopped.");
        }
    }
}
