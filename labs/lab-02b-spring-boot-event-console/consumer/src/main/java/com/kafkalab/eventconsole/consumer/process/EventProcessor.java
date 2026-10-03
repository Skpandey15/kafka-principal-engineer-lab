package com.kafkalab.eventconsole.consumer.process;

/**
 * The business step applied to every consumed event BEFORE it is accepted as SUCCESS. Whatever it
 * throws is treated as "this event could not be processed": the event is stored as FAILED with
 * the reason and handed to the retry worker, instead of blocking the partition or being dropped.
 *
 * <p>Implementations must be idempotent: the retry worker can run an event again after a crash
 * (at-least-once), and an operator can requeue a dead one.
 *
 * <p>They must also NOT touch MongoDB. A database outage is not the event's fault, and it is
 * handled elsewhere (the Kafka listener stalls and retries until the database is back); mixing the
 * two would burn an event's retry budget on someone else's outage.
 */
@FunctionalInterface
public interface EventProcessor {

    void process(String key, String value);
}
