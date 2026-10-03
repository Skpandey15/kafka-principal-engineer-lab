package com.kafkalab.eventconsole.consumer.repo;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;

/**
 * Every state change of an unfinished event. The rules that make it safe for several workers (and
 * a crashing one) to share the work:
 *
 * <ul>
 *   <li><b>Claim = lease.</b> A worker takes an event with ONE atomic find-and-modify that stamps
 *       {@code lockedUntil}. Two workers can never claim the same event; a worker that dies simply
 *       lets its lease run out and the event becomes claimable again.</li>
 *   <li><b>Every transition is conditional.</b> {@code markX} only applies if the event is still in
 *       the state (and retry count) the worker claimed it in. A worker that was paused past its
 *       lease cannot overwrite what the worker that took over has already decided: its update
 *       matches nothing and returns false.</li>
 *   <li><b>One document, one update.</b> No transition touches two documents or collections, so no
 *       transaction is needed and no failure can leave a half-moved event behind.</li>
 * </ul>
 */
@Repository
public class EventRetryRepository {

    private static final FindAndModifyOptions RETURN_UPDATED = FindAndModifyOptions.options().returnNew(true);

    private final MongoTemplate mongo;

    public EventRetryRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /** Takes the FAILED event that has been due longest, or empty if none is due / all are leased. */
    public Optional<EventDocument> claimDueForRetry(Instant now, Duration lease) {
        Query due = new Query(new Criteria().andOperator(
                Criteria.where("status").is(EventStatus.FAILED.name()),
                Criteria.where("nextRetryAt").lte(now),
                notLeased(now)))
                .with(Sort.by(Sort.Order.asc("nextRetryAt")));
        return Optional.ofNullable(mongo.findAndModify(due, new Update().set("lockedUntil", now.plus(lease)),
                RETURN_UPDATED, EventDocument.class));
    }

    /** Takes a DEAD event whose dead-letter copy is not confirmed yet. */
    public Optional<EventDocument> claimDeadForDlt(Instant now, Duration lease) {
        Query unpublished = new Query(new Criteria().andOperator(
                Criteria.where("status").is(EventStatus.DEAD.name()),
                Criteria.where("dltPublished").is(false),
                notLeased(now)))
                .with(Sort.by(Sort.Order.asc("deadAt")));
        return Optional.ofNullable(mongo.findAndModify(unpublished, new Update().set("lockedUntil", now.plus(lease)),
                RETURN_UPDATED, EventDocument.class));
    }

    /** The retry succeeded; {@code retries} then counts it, so it reads "succeeded after N retries". */
    public boolean markSucceeded(String id, int claimedRetries, Instant now) {
        Update update = new Update()
                .set("status", EventStatus.SUCCESS.name())
                .set("retries", claimedRetries + 1)
                .set("processedAt", now)
                .unset("nextRetryAt")
                .unset("lockedUntil");
        return transition(id, EventStatus.FAILED, claimedRetries, update);
    }

    /** The retry failed and another is allowed: count it, remember why, schedule the next one. */
    public boolean markRetryFailed(String id, int claimedRetries, String error, Instant nextRetryAt) {
        Update update = new Update()
                .set("retries", claimedRetries + 1)
                .set("lastError", error)
                .set("nextRetryAt", nextRetryAt)
                .unset("lockedUntil");
        return transition(id, EventStatus.FAILED, claimedRetries, update);
    }

    /** The last allowed retry failed: park the event as DEAD until the dead-letter copy is confirmed and an operator acts. */
    public boolean markDead(String id, int claimedRetries, String error, Instant now) {
        Update update = new Update()
                .set("status", EventStatus.DEAD.name())
                .set("retries", claimedRetries + 1)
                .set("lastError", error)
                .set("deadAt", now)
                .set("dltPublished", false)
                .unset("nextRetryAt")
                .unset("lockedUntil");
        return transition(id, EventStatus.FAILED, claimedRetries, update);
    }

    public boolean markDltPublished(String id) {
        Query query = Query.query(Criteria.where("_id").is(id).and("status").is(EventStatus.DEAD.name()));
        Update update = new Update().set("dltPublished", true).unset("lockedUntil");
        return mongo.updateFirst(query, update, EventDocument.class).getMatchedCount() > 0;
    }

    /** Operator action: give a DEAD event a fresh retry budget. Returns false if it is not DEAD (any more). */
    public boolean requeue(String id, Instant now) {
        Query query = Query.query(Criteria.where("_id").is(id).and("status").is(EventStatus.DEAD.name()));
        Update update = new Update()
                .set("status", EventStatus.FAILED.name())
                .set("retries", 0)
                .set("nextRetryAt", now)
                .unset("lockedUntil")
                .unset("deadAt")
                .unset("dltPublished");
        return mongo.updateFirst(query, update, EventDocument.class).getMatchedCount() > 0;
    }

    private boolean transition(String id, EventStatus expectedStatus, int expectedRetries, Update update) {
        Query query = Query.query(Criteria.where("_id").is(id)
                .and("status").is(expectedStatus.name())
                .and("retries").is(expectedRetries));
        return mongo.updateFirst(query, update, EventDocument.class).getMatchedCount() > 0;
    }

    private static Criteria notLeased(Instant now) {
        return new Criteria().orOperator(Criteria.where("lockedUntil").is(null), Criteria.where("lockedUntil").lt(now));
    }
}
