package com.kafkalab.eventconsole.producer.model;

import java.time.Instant;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Audit record of one bulk publish: what was asked for and what Kafka actually acknowledged.
 * Owned by the producer service, in the producer's own database. Old records expire (TTL).
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
        /** Acknowledged records per partition number (as a string: Mongo map keys must be strings). */
        Map<String, Long> partitionCounts,
        String firstError) {
}
