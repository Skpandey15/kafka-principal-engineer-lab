package com.kafkalab.replication.producer;

import com.kafkalab.replication.support.LabConfig;
import com.kafkalab.replication.support.OrderEvent;
import com.kafkalab.replication.support.PartitionMetadataLookup;
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
 * Produces to a single fixed partition (partition 0 of the configured
 * topic) indefinitely, one record every {@code delayMs}, so that killing
 * and recovering that partition's leader broker mid-run (README
 * Experiment 4/13) produces a real, observable sequence of successful
 * sends, temporary failures, and resumed sends -- not a simulation.
 *
 * <h2>What "attempt" means here, precisely</h2>
 * The Kafka producer client does not expose how many times it internally
 * retried a send before the send callback fires -- {@code RecordMetadata}
 * and the callback's {@code Exception} report only the final outcome.
 * Rather than fabricate a retry count the client doesn't actually provide,
 * this app implements its OWN outer retry loop: each "attempt" logged
 * below is one call to {@code producer.send(record).get(attemptTimeoutMs)}
 * made by this app, not a count of Kafka's own internal
 * {@code retries}/{@code delivery.timeout.ms}-governed retry machinery
 * underneath a single attempt. This distinction is spelled out here
 * because conflating the two would be exactly the kind of misleading log
 * this lab's own instructions warn against.
 *
 * <h2>What "leader" means in this log, precisely</h2>
 * Neither a producer's send callback nor {@code RecordMetadata} reports
 * which broker actually served a given request. Every {@code ADMIN_QUERY}
 * line below is a deliberately separate, explicitly-labeled
 * {@link PartitionMetadataLookup} call, taken periodically (not per
 * record), reporting the leader as of that separate query -- never
 * attributed to any specific record's own send path.
 */
public final class ContinuousProducerApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        String acks = LabConfig.get("acks", "all");
        long delayMs = LabConfig.getLong("delayMs", 500);
        int leaderCheckEveryN = LabConfig.getInt("leaderCheckEveryN", 5);
        int maxAttempts = LabConfig.getInt("maxAttempts", 20);
        long attemptTimeoutMs = LabConfig.getLong("attemptTimeoutMs", 3000);
        long attemptBackoffMs = LabConfig.getLong("attemptBackoffMs", 300);
        int targetPartition = LabConfig.getInt("partition", 0);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        // Long enough that Kafka's OWN internal retry/metadata-refresh
        // machinery gets a real chance to recover from a leader failure
        // within a single attempt, before this app's outer loop gives up
        // on that attempt and logs it as failed.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) attemptTimeoutMs - 200);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) attemptTimeoutMs - 500);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        PartitionMetadataLookup metadataLookup = new PartitionMetadataLookup(LabConfig.bootstrapServers());
        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        System.out.printf("Starting continuous producer | topic=%s partition=%d acks=%s delayMs=%d%n",
                topic, targetPartition, acks, delayMs);
        System.out.println("Press Ctrl+C to stop. Kill/restart a broker in another terminal while this runs.");

        try {
            long i = 0;
            while (running.get()) {
                String eventId = "ORDER-CP-" + i;
                String customerId = "CUSTOMER-100";
                OrderEvent event = new OrderEvent(eventId, customerId, 10.0 + (i % 25));
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, targetPartition, customerId, event.toWireFormat());

                boolean succeeded = false;
                for (int attempt = 1; attempt <= maxAttempts && !succeeded && running.get(); attempt++) {
                    long start = System.nanoTime();
                    try {
                        Future<RecordMetadata> future = producer.send(record);
                        RecordMetadata metadata = future.get(attemptTimeoutMs, TimeUnit.MILLISECONDS);
                        long latencyMs = (System.nanoTime() - start) / 1_000_000;
                        System.out.printf(
                                "%s | eventId=%-14s topic=%s partition=%d offset=%d attempt=%d result=SUCCESS latencyMs=%d%n",
                                Instant.now(), eventId, topic, metadata.partition(), metadata.offset(), attempt, latencyMs);
                        succeeded = true;
                    } catch (Exception e) {
                        long latencyMs = (System.nanoTime() - start) / 1_000_000;
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        System.out.printf(
                                "%s | eventId=%-14s topic=%s partition=%d attempt=%d result=FAILED latencyMs=%d exception=%s message=%s%n",
                                Instant.now(), eventId, topic, targetPartition, attempt, latencyMs,
                                cause.getClass().getName(), cause.getMessage());
                        if (attempt < maxAttempts) {
                            try {
                                Thread.sleep(attemptBackoffMs);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                running.set(false);
                            }
                        }
                    }
                }
                if (!succeeded && running.get()) {
                    System.out.printf("%s | eventId=%-14s GAVE_UP after %d attempts%n", Instant.now(), eventId, maxAttempts);
                }

                if (i % leaderCheckEveryN == 0) {
                    try {
                        String description = metadataLookup.describePartition(topic, targetPartition);
                        System.out.printf("%s | ADMIN_QUERY (separate from the send path above) partition=%d %s%n",
                                Instant.now(), targetPartition, description);
                    } catch (Exception e) {
                        System.out.printf("%s | ADMIN_QUERY failed: %s%n", Instant.now(), e.getMessage());
                    }
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
            metadataLookup.close();
            producer.close();
            System.out.println("Continuous producer stopped.");
        }
    }
}
