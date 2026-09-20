package com.kafkalab.transactions.producer;

import com.kafkalab.transactions.support.LabConfig;
import com.kafkalab.transactions.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Section 2's experiment: a deliberately NON-idempotent producer, sending
 * ONE logical business event ({@code eventId}), with an application-level
 * retry loop standing in for exactly the reliability problem real producer
 * code faces: {@code producer.send(record).get()} can throw without telling
 * the caller whether the broker actually persisted the record before the
 * ack was lost.
 *
 * <h2>How to actually see a duplicate (manual, real, timing-dependent)</h2>
 * Run this against the reused WP-07 cluster, then, WHILE it is blocked
 * inside the first {@code get()} call, kill the current leader of this
 * topic-partition in another terminal:
 * <pre>
 *   docker stop kafka-broker-&lt;leaderId&gt;
 * </pre>
 * Depending on exactly when the leader dies relative to this producer's
 * in-flight request, one of two real outcomes happens -- both are honest,
 * both are worth seeing:
 * <ul>
 *   <li>The broker had already appended the record and replicated it to the
 *       ISR before dying, but the acknowledgement never made it back to
 *       this producer before the connection dropped. This app cannot tell
 *       the difference between that and "never received," so it retries --
 *       and the retried send is accepted as a SECOND, distinct record. The
 *       topic now contains {@code Order-101} twice.</li>
 *   <li>The broker died before ever appending the record. The retry is the
 *       first successful write. No duplicate.</li>
 * </ul>
 * This is precisely why the outcome cannot be forced deterministically --
 * see the lab README's Experiment 1 for real captured output from an actual
 * run, whichever of the two outcomes it happened to produce, plus the
 * automated tests' honest account of why this specific experiment is
 * documented rather than asserted on in CI.
 */
public final class DuplicateRiskProducerApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "txn-lab-duplicate-risk");
        String eventId = LabConfig.get("eventId", "Order-101");
        int requestTimeoutMs = LabConfig.getInt("requestTimeoutMs", 3000);
        int maxAttempts = LabConfig.getInt("maxAttempts", 5);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // The two settings that make this scenario possible, deliberately:
        // idempotence OFF means the broker has no PID/sequence number to
        // deduplicate a resend with, and acks=1 (rather than the "all"
        // default) narrows the ack-loss window to "the leader wrote it but
        // died before the response reached us" rather than requiring a
        // full-ISR replication failure too.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // Kafka's internal retry mechanism is disabled so that every retry
        // seen in this app's output is OUR retry loop, not the client
        // quietly resending under the hood -- the whole point is to make
        // the "I don't know if this succeeded" decision visible.
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, requestTimeoutMs + 1000);

        System.out.printf("DuplicateRiskProducerApp | topic=%s | eventId=%s | enable.idempotence=false | acks=1 | retries=0 (app-level retry only)%n",
                topic, eventId);
        System.out.println("Kill this topic-partition's leader broker now (docker stop kafka-broker-<id>) to try to land in the ack-loss window.");

        OrderEvent event = new OrderEvent(eventId, "CUSTOMER-1", 42.50);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, eventId, event.toWireFormat()));
                System.out.printf("%s | attempt=%d | eventId=%s | send() issued, waiting up to %dms...%n",
                        Instant.now(), attempt, eventId, requestTimeoutMs + 2000);
                try {
                    RecordMetadata metadata = future.get(requestTimeoutMs + 2000L, TimeUnit.MILLISECONDS);
                    System.out.printf("%s | attempt=%d | eventId=%s | status=SUCCESS | partition=%d | offset=%d%n",
                            Instant.now(), attempt, eventId, metadata.partition(), metadata.offset());
                    System.out.println("Send acknowledged -- this attempt is done. If an earlier attempt's ack was lost but its write actually landed, the topic now holds a duplicate. Verify with runIsolationConsumer.");
                    return;
                } catch (TimeoutException | ExecutionException e) {
                    System.out.printf("%s | attempt=%d | eventId=%s | status=OUTCOME_UNKNOWN | exception=%s%n",
                            Instant.now(), attempt, eventId, e.getClass().getName() + ": " + rootMessage(e));
                    System.out.println("This producer cannot tell whether the broker already persisted that write before the ack was lost. Retrying is the only reliability-preserving choice -- and is exactly what creates duplicate risk without idempotence.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            System.out.printf("Gave up after %d attempts without a confirmed ack.%n", maxAttempts);
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
