package com.kafkalab.eventconsole.consumer.config;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The numbers an alert needs: how many events are in each status, and how many dead letters are
 * not yet confirmed on the dead-letter topic. They live in MongoDB, so they are read on a timer and
 * served from memory -- a scrape never queries the database, and a MongoDB outage leaves the last
 * known values in place (and counts the failure) instead of making the series vanish.
 *
 * <p>Each instance reports the same cluster-wide numbers; query with {@code max by (status)} rather
 * than {@code sum} when several consumers run.
 */
@Component
public class EventStatusMetrics {

    private static final Logger log = LoggerFactory.getLogger(EventStatusMetrics.class);

    private final EventQueryRepository events;
    private final EventRetryRepository retries;
    private final Counter refreshFailures;
    private final Map<EventStatus, AtomicLong> byStatus = new java.util.EnumMap<>(EventStatus.class);
    private final AtomicLong deadLettersUnconfirmed = new AtomicLong();

    public EventStatusMetrics(EventQueryRepository events, EventRetryRepository retries, MeterRegistry registry) {
        this.events = events;
        this.retries = retries;
        for (EventStatus status : EventStatus.values()) {
            AtomicLong value = new AtomicLong();
            byStatus.put(status, value);
            Gauge.builder("eventconsole.events", value, AtomicLong::get).tag("status", status.name())
                    .description("Events stored, by status").register(registry);
        }
        Gauge.builder("eventconsole.dead.letters.unconfirmed", deadLettersUnconfirmed, AtomicLong::get)
                .description("DEAD events whose dead-letter topic copy is not confirmed yet").register(registry);
        this.refreshFailures = Counter.builder("eventconsole.metrics.refresh.failures")
                .description("Times the status counts could not be read from MongoDB").register(registry);
    }

    @Scheduled(fixedDelayString = "${app.metrics-refresh-ms:15000}", initialDelayString = "${app.metrics-refresh-ms:15000}")
    void scheduledRefresh() {
        refresh();
    }

    /** Reads the counts now. Public so tests need not wait for the timer. */
    public void refresh() {
        try {
            Map<String, Long> counts = events.countByStatus();
            for (EventStatus status : EventStatus.values()) {
                byStatus.get(status).set(counts.getOrDefault(status.name(), 0L));
            }
            deadLettersUnconfirmed.set(retries.countUnconfirmedDeadLetters());
        } catch (DataAccessException e) {
            refreshFailures.increment();
            log.warn("Could not refresh status metrics, keeping the last values: {}", e.getMessage());
        }
    }
}
