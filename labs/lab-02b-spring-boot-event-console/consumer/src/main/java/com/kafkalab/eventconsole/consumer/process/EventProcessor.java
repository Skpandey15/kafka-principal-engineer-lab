package com.kafkalab.eventconsole.consumer.process;

/**
 * The business step applied to every consumed event BEFORE it is accepted as SUCCESS. An
 * {@link EventProcessingException} (or any other RuntimeException) means "this event could not be
 * processed": the event is stored as FAILED with the reason and handed to the retry worker,
 * instead of blocking the partition or being dropped.
 *
 * <p>Implementations must be idempotent: the retry worker can run an event again after a crash
 * (at-least-once), and an operator can requeue a dead one.
 *
 * <p>They must NOT touch MongoDB, and if a dependency they use is down they must throw
 * {@link InfrastructureUnavailableException} rather than an event failure: an outage is not the
 * event's fault, and treating it as such would burn the event's retry budget (or dead-letter a
 * healthy event) on someone else's outage.
 */
@FunctionalInterface
public interface EventProcessor {

    void process(ConsumedEvent event);
}
