package com.kafkalab.controllerquorum.producer;

import com.kafkalab.controllerquorum.support.LabConfig;
import com.kafkalab.controllerquorum.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Seeds a topic with {@code eventId}-tagged order events (burst mode),
 * synchronously by default (one send, wait for its result, log it) so
 * this lab's control-plane-vs-data-plane experiments (6, 10) can see a
 * clean pass/fail for ordinary produce traffic without a separate
 * retry-attempt framework -- this WP is about the control plane, not
 * producer retry semantics (that is WP-07's lab).
 */
public final class OrderEventProducerApp {

    private static final int DISTINCT_CUSTOMERS = 4;

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        int count = LabConfig.getInt("count", 10);
        int startEventNumber = LabConfig.getInt("startEventNumber", 1);
        boolean sync = LabConfig.getBoolean("sync", true);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("Producing %d order events to %s (sync=%s) starting at ORDER-%d%n",
                    count, topic, sync, 1000 + startEventNumber);
            int succeeded = 0;
            int failed = 0;
            for (int i = 0; i < count; i++) {
                int eventNumber = startEventNumber + i;
                String eventId = "ORDER-" + (1000 + eventNumber);
                String customerId = "CUSTOMER-" + (100 + (eventNumber % DISTINCT_CUSTOMERS));
                double amount = 10.00 + (eventNumber % 25) * 3.5;
                OrderEvent event = new OrderEvent(eventId, customerId, amount);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, customerId, event.toWireFormat());

                if (sync) {
                    long start = System.nanoTime();
                    try {
                        Future<RecordMetadata> future = producer.send(record);
                        RecordMetadata metadata = future.get();
                        long latencyMs = (System.nanoTime() - start) / 1_000_000;
                        System.out.printf("eventId=%-12s partition=%d offset=%d latencyMs=%d status=SUCCESS%n",
                                eventId, metadata.partition(), metadata.offset(), latencyMs);
                        succeeded++;
                    } catch (ExecutionException | InterruptedException e) {
                        long latencyMs = (System.nanoTime() - start) / 1_000_000;
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        System.out.printf("eventId=%-12s latencyMs=%d status=FAILED exception=%s message=%s%n",
                                eventId, latencyMs, cause.getClass().getName(), cause.getMessage());
                        failed++;
                    }
                } else {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.out.printf("eventId=%-12s status=FAILED exception=%s message=%s%n",
                                    eventId, exception.getClass().getName(), exception.getMessage());
                        } else {
                            System.out.printf("eventId=%-12s -> partition=%d offset=%d status=SUCCESS%n",
                                    eventId, metadata.partition(), metadata.offset());
                        }
                    });
                }
            }
            producer.flush();
            System.out.printf("Done. succeeded=%d failed=%d%n", succeeded, failed);
        }
    }
}
