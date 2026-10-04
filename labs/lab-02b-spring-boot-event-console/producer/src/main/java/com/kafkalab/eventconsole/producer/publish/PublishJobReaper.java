package com.kafkalab.eventconsole.producer.publish;

import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.model.PublishJob;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Labels audit rows that were STARTED and never finished.
 *
 * <p>A publish writes its audit row first and replaces it with the outcome at the end. If the process
 * dies in between, the row stays STARTED forever and would look like a publish still in progress. After
 * {@code app.audit.interrupted-after} (comfortably longer than the longest possible publish) it is
 * marked INTERRUPTED: "events from this job may or may not be in Kafka; the outcome is unknown".
 * The sweep is one conditional update, so any number of producer replicas can run it.
 */
@Component
public class PublishJobReaper {

    private static final Logger log = LoggerFactory.getLogger(PublishJobReaper.class);

    private final MongoTemplate mongo;
    private final AppProperties props;
    private final MeterRegistry metrics;
    private final Clock clock;

    public PublishJobReaper(MongoTemplate mongo, AppProperties props, MeterRegistry metrics, Clock clock) {
        this.mongo = mongo;
        this.props = props;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${app.audit.sweep-interval-ms:60000}", initialDelayString = "${app.audit.sweep-interval-ms:60000}")
    void sweepOnSchedule() {
        try {
            sweep(clock.instant());
        } catch (DataAccessException e) {
            log.warn("Could not sweep interrupted publish jobs, will try again: {}", e.getMessage());
        }
    }

    /** @return how many jobs were labelled INTERRUPTED. Public so tests need not wait for the timer. */
    public long sweep(Instant now) {
        Instant cutoff = now.minus(props.audit().interruptedAfter());
        long marked = mongo.updateMulti(
                Query.query(Criteria.where("status").is(PublishJob.STARTED).and("createdAt").lt(cutoff)),
                new Update().set("status", PublishJob.INTERRUPTED), PublishJob.class).getModifiedCount();
        if (marked > 0) {
            metrics.counter("eventconsole.audit.interrupted").increment(marked);
            log.warn("{} publish job(s) were STARTED more than {} ago and never finished; marked INTERRUPTED (their outcome is unknown)",
                    marked, props.audit().interruptedAfter());
        }
        return marked;
    }
}
