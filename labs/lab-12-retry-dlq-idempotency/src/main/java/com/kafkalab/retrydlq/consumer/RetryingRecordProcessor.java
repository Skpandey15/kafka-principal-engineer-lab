package com.kafkalab.retrydlq.consumer;

import com.kafkalab.retrydlq.support.IdempotencyStore;
import com.kafkalab.retrydlq.support.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * The core retry / DLQ / idempotency orchestration, factored out of any
 * one runnable app so both {@link RetryDlqConsumerApp} and this lab's
 * test suite exercise the EXACT same logic -- no test-only
 * reimplementation to drift out of sync with what actually runs.
 *
 * <p>Per record: (1) parse -- a parse failure is a POISON message,
 * routed straight to the DLQ with no idempotency check at all (there is
 * no reliable eventId to claim); (2) claim the eventId via
 * {@link IdempotencyStore#tryClaim} -- a duplicate is skipped entirely,
 * no business logic invoked; (3) run business logic with bounded
 * retries and exponential backoff; (4) on success, mark the claim DONE;
 * on exhausted retries, RELEASE the claim (so a later, fixed redelivery
 * isn't blocked forever, see {@link IdempotencyStore}'s Javadoc) and
 * publish to the DLQ with headers mirroring Spring Kafka's
 * {@code DeadLetterPublishingRecoverer} convention
 * ({@code kafka_dlt-*}), a real, recognizable production shape.
 */
public final class RetryingRecordProcessor {

    public enum Outcome {
        PROCESSED, DUPLICATE_SKIPPED, SENT_TO_DLQ, POISON_SENT_TO_DLQ
    }

    public interface EventProcessor {
        void process(OrderEvent event) throws Exception;
    }

    private final IdempotencyStore idempotencyStore;
    private final KafkaProducer<String, String> producer;
    private final String dlqTopic;
    private final int maxAttempts;
    private final Duration baseBackoff;
    private final Duration claimLeaseTimeout;

    public RetryingRecordProcessor(IdempotencyStore idempotencyStore, KafkaProducer<String, String> producer,
                                    String dlqTopic, int maxAttempts, Duration baseBackoff, Duration claimLeaseTimeout) {
        this.idempotencyStore = idempotencyStore;
        this.producer = producer;
        this.dlqTopic = dlqTopic;
        this.maxAttempts = maxAttempts;
        this.baseBackoff = baseBackoff;
        this.claimLeaseTimeout = claimLeaseTimeout;
    }

    public Outcome process(ConsumerRecord<String, String> record, EventProcessor businessLogic) throws Exception {
        OrderEvent event;
        try {
            event = OrderEvent.parse(record.value());
        } catch (IllegalArgumentException poison) {
            sendToDlq(record, poison);
            return Outcome.POISON_SENT_TO_DLQ;
        }

        IdempotencyStore.ClaimResult claim = idempotencyStore.tryClaim(event.eventId(), claimLeaseTimeout);
        if (claim != IdempotencyStore.ClaimResult.CLAIMED) {
            return Outcome.DUPLICATE_SKIPPED;
        }

        Exception lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                businessLogic.process(event);
                idempotencyStore.complete(event.eventId());
                return Outcome.PROCESSED;
            } catch (Exception e) {
                lastFailure = e;
                if (attempt < maxAttempts) {
                    Thread.sleep(baseBackoff.toMillis() * attempt);
                }
            }
        }

        idempotencyStore.release(event.eventId());
        sendToDlq(record, lastFailure);
        return Outcome.SENT_TO_DLQ;
    }

    private void sendToDlq(ConsumerRecord<String, String> record, Exception cause) throws Exception {
        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, record.key(), record.value());
        dlqRecord.headers()
                .add(new RecordHeader("kafka_dlt-exception-fqcn", cause.getClass().getName().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("kafka_dlt-exception-message", String.valueOf(cause.getMessage()).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("kafka_dlt-original-topic", record.topic().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("kafka_dlt-original-partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("kafka_dlt-original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8)));
        producer.send(dlqRecord).get();
    }
}
