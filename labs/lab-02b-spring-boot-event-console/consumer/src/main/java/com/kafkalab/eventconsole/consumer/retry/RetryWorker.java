package com.kafkalab.eventconsole.consumer.retry;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.consume.EventConsumer;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.process.ConsumedEvent;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Owns every event that failed processing: retries it with exponential backoff, and when the
 * allowed retries are used up parks it as DEAD and publishes it to the dead-letter topic.
 *
 * <p>It lives inside the consumer service on purpose. The events and their state belong to the
 * consumer's database, and a separate retry application would need credentials for it, undoing
 * the database-per-service isolation. Any number of consumer instances may run a worker: they
 * share the work through leases (see {@link EventRetryRepository}).
 *
 * <p>What it guarantees, and what it does not:
 * <ul>
 *   <li>An event is never lost: its state is in MongoDB before its Kafka offset is committed, and
 *       it only leaves FAILED by succeeding or becoming DEAD.</li>
 *   <li>A database outage does not burn retries: when MongoDB is unreachable the worker cannot
 *       record an attempt, so none is counted; the lease expires and the event is simply tried
 *       again later.</li>
 *   <li>Processing is at-least-once: a worker that dies after processing but before recording the
 *       result causes one more attempt, so {@link EventProcessor} must be idempotent.</li>
 *   <li>The dead-letter topic is at-least-once as well, and keyed by the event id so a consumer of
 *       it can de-duplicate.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "app.retry.enabled", havingValue = "true", matchIfMissing = true)
public class RetryWorker {

    private static final Logger log = LoggerFactory.getLogger(RetryWorker.class);

    private final EventRetryRepository repository;
    private final EventProcessor processor;
    private final KafkaTemplate<String, String> kafka;
    private final AppProperties props;
    private final MeterRegistry metrics;
    private final Clock clock;

    public RetryWorker(EventRetryRepository repository, EventProcessor processor, KafkaTemplate<String, String> kafka,
            AppProperties props, MeterRegistry metrics, Clock clock) {
        this.repository = repository;
        this.processor = processor;
        this.kafka = kafka;
        this.props = props;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${app.retry.poll-interval-ms}", initialDelayString = "${app.retry.poll-interval-ms}")
    void tick() {
        try {
            runOnce();
        } catch (DataAccessException e) {
            // MongoDB is unreachable. Nothing was recorded, so nothing was lost or counted; the next tick tries again.
            log.warn("Retry worker could not reach MongoDB, will try again: {}", e.getMessage());
        }
    }

    /** One pass: retry what is due, then confirm dead letters. Public so tests can drive it deterministically. */
    public void runOnce() {
        retryDueEvents();
        publishPendingDeadLetters();
    }

    private void retryDueEvents() {
        Duration lease = Duration.ofMillis(props.retry().leaseMs());
        for (int i = 0; i < props.retry().batchSize(); i++) {
            Optional<EventDocument> claimed = repository.claimDueForRetry(clock.instant(), lease);
            if (claimed.isEmpty()) {
                return;
            }
            try {
                retry(claimed.get());
            } catch (InfrastructureUnavailableException outage) {
                // A dependency (the Schema Registry) is down. That says nothing about this event: no
                // attempt is recorded or counted, its lease simply runs out and it is tried again. Stop
                // this pass (every other event would hit the same wall) but still do the dead-letter sweep.
                metrics.counter("eventconsole.contract.unavailable").increment();
                log.warn("Retry pass stopped, a dependency is unavailable: {}", outage.getMessage());
                return;
            }
        }
    }

    private void retry(EventDocument event) {
        String error = null;
        try {
            processor.process(new ConsumedEvent(event.key(), event.value(), event.schemaId()));
        } catch (InfrastructureUnavailableException outage) {
            throw outage;
        } catch (RuntimeException e) {
            error = EventConsumer.describe(e);
        }

        Instant now = clock.instant();
        int attemptNumber = event.retries() + 1;
        if (error == null) {
            if (repository.markSucceeded(event.id(), event.retries(), now)) {
                metrics.counter("eventconsole.retry.succeeded").increment();
                log.info("Event {} succeeded on retry {}", event.id(), attemptNumber);
            } else {
                log.info("Event {} was already handled by another worker; ignoring this result", event.id());
            }
        } else if (props.retry().isExhausted(attemptNumber)) {
            if (repository.markDead(event.id(), event.retries(), error, now)) {
                metrics.counter("eventconsole.retry.dead").increment();
                log.warn("Event {} is DEAD after {} failed retries: {}", event.id(), attemptNumber, error);
            }
        } else {
            Instant next = now.plus(props.retry().delayAfter(attemptNumber));
            if (repository.markRetryFailed(event.id(), event.retries(), error, next)) {
                metrics.counter("eventconsole.retry.failed").increment();
                log.info("Event {} failed retry {}/{}, next attempt at {}: {}", event.id(), attemptNumber,
                        props.retry().maxRetries(), next, error);
            }
        }
    }

    /**
     * DEAD is recorded in MongoDB first (atomic, the source of truth), the dead-letter topic is
     * written second and only then confirmed. If Kafka is down, the event stays DEAD and
     * unconfirmed and is picked up again; the cost is at-least-once delivery to the topic.
     */
    private void publishPendingDeadLetters() {
        Duration lease = Duration.ofMillis(props.retry().leaseMs());
        for (int i = 0; i < props.retry().batchSize(); i++) {
            Optional<EventDocument> claimed = repository.claimDeadForDlt(clock.instant(), lease);
            if (claimed.isEmpty()) {
                return;
            }
            EventDocument dead = claimed.get();
            try {
                kafka.send(deadLetterRecord(dead)).get(props.retry().dltSendTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Leave it unconfirmed; the lease expires and a later pass retries the write.
                log.warn("Could not write dead event {} to {}, will retry: {}", dead.id(), props.deadLetterTopic(),
                        e.getMessage());
                continue;
            }
            repository.markDltPublished(dead.id());
            metrics.counter("eventconsole.retry.dlt.published").increment();
        }
    }

    private ProducerRecord<String, String> deadLetterRecord(EventDocument dead) {
        // Same partition number as the source record, like the topic's partitions were sized for.
        ProducerRecord<String, String> record = new ProducerRecord<>(props.deadLetterTopic(), dead.partition(),
                dead.key(), dead.value());
        record.headers()
                .add("x-event-id", bytes(dead.id()))
                .add("x-original-topic", bytes(dead.topic()))
                .add("x-original-partition", bytes(String.valueOf(dead.partition())))
                .add("x-original-offset", bytes(String.valueOf(dead.offset())))
                .add("x-retries", bytes(String.valueOf(dead.retries())))
                .add("x-last-error", bytes(dead.lastError() == null ? "" : dead.lastError()));
        return record;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
