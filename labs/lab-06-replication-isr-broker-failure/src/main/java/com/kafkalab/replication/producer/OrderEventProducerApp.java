package com.kafkalab.replication.producer;

import com.kafkalab.replication.support.LabConfig;
import com.kafkalab.replication.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Seeds a topic with {@code eventId}-tagged order events (burst mode) --
 * this lab's general-purpose producer, used both to populate
 * {@code replicated-orders} for the leader/follower and ISR experiments,
 * and, with {@code -Psync=true}, as the exact tool the {@code min.insync.replicas}
 * experiment (README Experiment 8) uses to force each record's
 * {@code acks=all} acknowledgment (or rejection) to be observed
 * synchronously, one at a time, instead of buried in an async callback.
 *
 * <p>{@code -Packs} is passed straight through to the producer's own
 * {@code acks} config -- this is also the tool Experiment 7 (acks=0/1/all)
 * runs with each of the three values to observe the real difference in
 * behavior and latency.
 */
public final class OrderEventProducerApp {

    private static final int DISTINCT_CUSTOMERS = 4;

    public static void main(String[] args) throws InterruptedException {
        String topic = LabConfig.topic();
        int count = LabConfig.getInt("count", 10);
        int startEventNumber = LabConfig.getInt("startEventNumber", 1);
        String acks = LabConfig.get("acks", "all");
        boolean sync = LabConfig.getBoolean("sync", false);
        boolean idempotence = LabConfig.getBoolean("idempotence", true);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        if ("0".equals(acks) || !idempotence) {
            // acks=0 is incompatible with idempotence (which the client
            // otherwise enables by default) -- disabling it here is what
            // makes acks=0 actually take effect rather than the producer
            // refusing to start. -Pidempotence=false is also used
            // deliberately by the min.insync.replicas experiment (README
            // Experiment 8): an idempotent producer's startup handshake
            // (InitProducerId) needs its own transaction/producer-id
            // coordinator broker, which is a genuinely different
            // requirement from "can this partition's leader accept an
            // acks=all write" -- with most of the cluster down, that
            // handshake can block for this client's entire max.block.ms
            // before send() even returns a Future, which would obscure
            // the ISR-rejection behavior this experiment exists to show.
            // This was discovered for real while building this lab, not
            // assumed -- see the README's Experiment 8 "what actually
            // happened" note.
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        }
        // Fail fast rather than retrying for a long time, so a rejected
        // send (Experiment 8, ISR below min.insync.replicas) surfaces
        // quickly and predictably instead of after the client's default
        // multi-minute retry budget.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("Producing %d order events to %s (acks=%s, sync=%s) starting at ORDER-%d%n",
                    count, topic, acks, sync, 1000 + startEventNumber);
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
                    } catch (ExecutionException e) {
                        long latencyMs = (System.nanoTime() - start) / 1_000_000;
                        System.out.printf("eventId=%-12s latencyMs=%d status=FAILED exception=%s message=%s%n",
                                eventId, latencyMs, e.getCause().getClass().getName(), e.getCause().getMessage());
                        failed++;
                    }
                } else {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.out.printf("eventId=%-12s status=FAILED exception=%s message=%s%n",
                                    eventId, exception.getClass().getName(), exception.getMessage());
                        } else {
                            System.out.printf("eventId=%-12s customerId=%-12s -> partition=%d offset=%d status=SUCCESS%n",
                                    eventId, customerId, metadata.partition(), metadata.offset());
                        }
                    });
                }
            }
            producer.flush();
            if (sync) {
                System.out.printf("Done. succeeded=%d failed=%d%n", succeeded, failed);
            } else {
                System.out.println("Done.");
            }
        }
    }
}
