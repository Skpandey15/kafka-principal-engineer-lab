package com.kafkalab.eventconsole.consumer.consume;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.process.ConsumedEvent;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    /** Keeps the error text bounded: it is stored, returned by the API and rendered in the UI. */
    static final int MAX_ERROR_LENGTH = 500;

    private final EventQueryRepository events;
    private final EventProcessor processor;
    private final AppProperties props;
    private final MeterRegistry metrics;
    private final Clock clock;

    public EventConsumer(EventQueryRepository events, EventProcessor processor, AppProperties props,
            MeterRegistry metrics, Clock clock) {
        this.events = events;
        this.processor = processor;
        this.props = props;
        this.metrics = metrics;
        this.clock = clock;
    }

    // Batch listener: one poll() worth of records becomes one bulk write, which is
    // what makes a 10,000-event publish land in seconds instead of 10,000 round trips.
    //
    // EVERY record in the batch is stored, whatever happens to it: an event that fails processing
    // is stored as FAILED (with the reason and a retry time) instead of throwing. So a bad event
    // never blocks its partition, never forces a batch to be replayed, and is never lost -- the
    // offset can always be committed, because the event now lives in MongoDB, where the retry
    // worker owns it.
    //
    // The only way out of this method by exception is MongoDB being unreachable. That is not the
    // event's fault, so it is left to the error handler, which waits and retries the same batch
    // until the database is back (see ConsumerErrorHandlingConfig).
    //
    // Delivery is at-least-once: offsets are committed only after this method returns. A crash
    // between the Mongo write and the commit redelivers the batch, which is harmless because the
    // write is idempotent (see EventDocument's deterministic _id).
    //
    // concurrency matches the partition count: one consumer thread per partition, the
    // most parallelism a consumer group can use (partition count is the ceiling).
    @KafkaListener(topics = "${app.topic}", batch = "true", concurrency = "${app.consumer-concurrency:3}")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        storeBatch(records);
    }

    /** Public so tests can replay records directly and prove idempotence. Returns newly stored count. */
    public int storeBatch(List<ConsumerRecord<String, String>> records) {
        Instant now = clock.instant();
        List<EventDocument> docs = records.stream().map(record -> toDocument(record, now)).toList();
        int inserted = events.insertAllIgnoringDuplicates(docs);

        long failed = docs.stream().filter(d -> d.status() == EventStatus.FAILED).count();
        if (failed > 0) {
            // One line per batch, not per event: a producer sending 100,000 bad events must not
            // drown the log. The per-event reason is stored on the event and visible in the API.
            log.warn("{} of {} events in this batch failed processing and were queued for retry", failed, docs.size());
        }
        // A redelivered batch is counted again here; the counters are rates for dashboards, the
        // database is the source of truth for exact numbers.
        metrics.counter("eventconsole.consume.stored").increment(inserted);
        metrics.counter("eventconsole.consume.duplicates").increment(docs.size() - inserted);
        metrics.counter("eventconsole.consume.failed").increment(failed);
        if (docs.size() != inserted) {
            log.debug("{} duplicate delivery(ies) ignored in a batch of {}", docs.size() - inserted, docs.size());
        }
        return inserted;
    }

    private EventDocument toDocument(ConsumerRecord<String, String> record, Instant now) {
        EventStatus status = EventStatus.SUCCESS;
        String error = null;
        String schemaId = schemaIdOf(record);
        try {
            processor.process(new ConsumedEvent(record.key(), record.value(), schemaId));
        } catch (InfrastructureUnavailableException outage) {
            // The Schema Registry (or a broken contract) is not this event's fault: do not record a
            // failure. Propagate so the whole batch waits and is retried, exactly like a MongoDB outage.
            metrics.counter("eventconsole.contract.unavailable").increment();
            throw outage;
        } catch (RuntimeException e) {
            status = EventStatus.FAILED;
            error = describe(e);
        }
        return new EventDocument(
                EventDocument.idFor(record.topic(), record.partition(), record.offset()),
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                schemaId,
                Instant.ofEpochMilli(record.timestamp()),
                now,
                status,
                0,
                error,
                status == EventStatus.FAILED ? now.plus(props.retry().delayAfter(0)) : null,
                null,
                status == EventStatus.SUCCESS ? now : null,
                null,
                null);
    }

    /** Kafka header naming the Schema Registry id of the contract the producer says the value follows. */
    public static final String SCHEMA_ID_HEADER = "x-schema-id";

    private static String schemaIdOf(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(SCHEMA_ID_HEADER);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** One line, bounded: it is stored with the event, returned by the API and rendered in the UI. */
    public static String describe(RuntimeException e) {
        String text = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
