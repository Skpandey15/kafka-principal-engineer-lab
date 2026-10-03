package com.kafkalab.eventconsole.consumer.model;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * One consumed Kafka record, in whatever state processing left it. The id is derived from the
 * record's full address, (topic, partition, offset), the only globally unique identifier a Kafka
 * record has. That makes the consumer's MongoDB write idempotent without any extra bookkeeping:
 * a redelivered record produces the same _id and is rejected.
 *
 * <p>Keeping SUCCESS, FAILED and DEAD events in the SAME document (rather than moving a failed
 * event between collections) is a consistency decision: MongoDB guarantees a single-document
 * update is atomic, but not an update spanning two collections without a transaction. Every state
 * change below is therefore one conditional update on one document.
 *
 * <p>This collection is a read model for successful events -- Kafka is the system of record -- so
 * SUCCESS documents expire (partial TTL index, created by {@code EventStoreSetup}). FAILED and
 * DEAD documents never expire on their own: they are unfinished business, and silently deleting
 * them would lose the only record of why an event did not make it.
 *
 * @param schemaId     the x-schema-id header the producer sent: which contract it says the value follows
 * @param retries      how many RETRIES have run (not counting the first attempt at consume time)
 * @param lastError    why the most recent attempt failed
 * @param nextRetryAt  FAILED only: the retry worker will not touch the event before this instant
 * @param lockedUntil  lease held by a worker that claimed this event; expires by itself if the worker dies
 * @param processedAt  when it became SUCCESS
 * @param deadAt       when it became DEAD
 * @param dltPublished DEAD only: whether the copy on the dead-letter topic is confirmed written
 */
@Document("events")
@CompoundIndex(name = "newest_first", def = "{'consumedAt': -1, 'offset': -1}")
@CompoundIndex(name = "status_next_retry", def = "{'status': 1, 'nextRetryAt': 1}")
public record EventDocument(
        @Id String id,
        String topic,
        @Indexed int partition,
        long offset,
        @Indexed String key,
        String value,
        String schemaId,
        Instant kafkaTimestamp,
        Instant consumedAt,
        EventStatus status,
        int retries,
        String lastError,
        Instant nextRetryAt,
        Instant lockedUntil,
        Instant processedAt,
        Instant deadAt,
        Boolean dltPublished) {

    public static String idFor(String topic, int partition, long offset) {
        return topic + "-" + partition + "-" + offset;
    }
}
