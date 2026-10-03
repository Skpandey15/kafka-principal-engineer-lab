package com.kafkalab.eventconsole.model;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * One consumed Kafka record. The id is derived from the record's full address,
 * (topic, partition, offset), the only globally unique identifier a Kafka record
 * has. That makes the consumer's MongoDB write idempotent without any extra
 * bookkeeping: a redelivered record produces the same _id and is rejected.
 *
 * <p>This collection is a read model, not the system of record -- Kafka is. So it
 * is allowed to expire: the TTL index on {@code consumedAt} keeps it from growing
 * without bound, and anything older can be rebuilt by replaying the topic.
 */
@Document("events")
@CompoundIndex(name = "newest_first", def = "{'consumedAt': -1, 'offset': -1}")
public record EventDocument(
        @Id String id,
        String topic,
        @Indexed int partition,
        long offset,
        @Indexed String key,
        String value,
        Instant kafkaTimestamp,
        @Indexed(name = "ttl_consumedAt", expireAfter = "${app.events-ttl:7d}") Instant consumedAt) {

    public static String idFor(String topic, int partition, long offset) {
        return topic + "-" + partition + "-" + offset;
    }
}
