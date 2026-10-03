package com.kafkalab.eventconsole.model;

import java.time.Instant;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/** Audit record of one bulk publish: what was asked for and what Kafka actually acknowledged. */
@Document("publish_jobs")
public record PublishJob(
        @Id String id,
        Instant createdAt,
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
