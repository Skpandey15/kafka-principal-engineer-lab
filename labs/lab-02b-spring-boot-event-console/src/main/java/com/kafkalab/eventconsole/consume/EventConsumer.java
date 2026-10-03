package com.kafkalab.eventconsole.consume;

import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.model.EventDocument;
import com.kafkalab.eventconsole.repo.EventQueryRepository;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final EventQueryRepository events;
    private final MeterRegistry metrics;

    public EventConsumer(EventQueryRepository events, MeterRegistry metrics) {
        this.events = events;
        this.metrics = metrics;
    }

    // Batch listener: one poll() worth of records becomes one bulk write, which is
    // what makes a 10,000-event publish land in seconds instead of 10,000 round trips.
    //
    // Delivery is at-least-once: offsets are committed only after this method returns.
    // A crash between the Mongo write and the commit redelivers the batch, which is
    // harmless because the write is idempotent (see EventDocument's deterministic _id).
    //
    // concurrency matches the partition count: one consumer thread per partition, the
    // most parallelism a consumer group can use (partition count is the ceiling).
    @KafkaListener(topics = "${app.topic}", batch = "true", concurrency = "${app.consumer-concurrency:3}")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        storeBatch(records);
    }

    /** Public so tests can replay records directly and prove idempotence. Returns newly stored count. */
    public int storeBatch(List<ConsumerRecord<String, String>> records) {
        List<EventDocument> docs = records.stream().map(EventConsumer::toDocument).toList();
        int inserted;
        try {
            inserted = events.insertAllIgnoringDuplicates(docs);
        } catch (DataAccessResourceFailureException | TransientDataAccessException outage) {
            // The database is unavailable: this says nothing about any particular record, so
            // do NOT hunt for a bad one (every single-record retry would fail the same way).
            // Rethrow as-is; the error handler backs off and retries until it is back.
            throw outage;
        } catch (RuntimeException bulkFailure) {
            // Something other than a duplicate went wrong. Find the exact record that
            // can't be stored, so the error handler retries / dead-letters THAT record
            // only and commits everything before it, instead of replaying the whole batch.
            inserted = storeOneByOne(docs, bulkFailure);
        }
        metrics.counter("eventconsole.consume.stored").increment(inserted);
        metrics.counter("eventconsole.consume.duplicates").increment(docs.size() - inserted);
        if (docs.size() != inserted) {
            log.debug("{} duplicate delivery(ies) ignored in a batch of {}", docs.size() - inserted, docs.size());
        }
        return inserted;
    }

    private int storeOneByOne(List<EventDocument> docs, RuntimeException cause) {
        int inserted = 0;
        for (int i = 0; i < docs.size(); i++) {
            try {
                if (events.insertIfAbsent(docs.get(i))) {
                    inserted++;
                }
            } catch (RuntimeException e) {
                log.warn("Could not store {} (bulk failure: {}): {}", docs.get(i).id(), cause.getMessage(), e.getMessage());
                throw new BatchListenerFailedException("Failed to store " + docs.get(i).id(), e, i);
            }
        }
        return inserted;
    }

    private static EventDocument toDocument(ConsumerRecord<String, String> record) {
        return new EventDocument(
                EventDocument.idFor(record.topic(), record.partition(), record.offset()),
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                Instant.ofEpochMilli(record.timestamp()),
                Instant.now());
    }
}
