package com.kafkalab.eventconsole.producer.model;

import java.time.Instant;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Audit record of one bulk publish: what was asked for and what Kafka actually acknowledged.
 * Owned by the producer service, in the producer's own database. Old records expire (TTL).
 *
 * <p>It is written <b>twice</b>: as {@link #STARTED} BEFORE anything is sent (a write-ahead record
 * that exists even if this process dies mid-publish), then replaced by the outcome. A row that is
 * still STARTED long after is a publish whose result is unknown; {@code PublishJobReaper} labels it
 * {@link #INTERRUPTED} so nobody mistakes it for one in progress.
 */
@Document("publish_jobs")
public record PublishJob(
        @Id String id,
        @Indexed(name = "ttl_createdAt", expireAfter = "${app.jobs-ttl:30d}") Instant createdAt,
        String topic,
        String mode,
        String keyStrategy,
        int requested,
        int acked,
        int failed,
        long durationMs,
        /** The contract the events were checked against (null only for records written before contracts existed). */
        Integer schemaId,
        Integer schemaVersion,
        /** Acknowledged records per partition number (as a string: Mongo map keys must be strings). */
        Map<String, Long> partitionCounts,
        String firstError,
        /** One of the constants below; null only on records written before this field existed (they are complete). */
        String status) {

    /** Recorded before anything is sent; the outcome is not known yet. */
    public static final String STARTED = "STARTED";
    /** The outcome (what Kafka acknowledged, what failed) is recorded. */
    public static final String COMPLETED = "COMPLETED";
    /** STARTED for so long that the publishing process must have died: the outcome is unknown. */
    public static final String INTERRUPTED = "INTERRUPTED";
    /**
     * Returned to the caller (never stored) when the events WERE sent but the final audit write failed:
     * the database still says STARTED. The caller gets the real outcome rather than an error for a
     * publish that happened, because an error invites a retry, and a retry publishes everything twice.
     */
    public static final String COMPLETED_UNRECORDED = "COMPLETED_UNRECORDED";

    public PublishJob withStatus(String newStatus) {
        return new PublishJob(id, createdAt, topic, mode, keyStrategy, requested, acked, failed, durationMs, schemaId,
                schemaVersion, partitionCounts, firstError, newStatus);
    }
}
