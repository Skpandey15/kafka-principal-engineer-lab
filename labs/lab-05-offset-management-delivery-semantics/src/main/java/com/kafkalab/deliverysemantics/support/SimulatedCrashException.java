package com.kafkalab.deliverysemantics.support;

/**
 * Thrown at a deliberately chosen, configured {@link FailurePoint} to
 * simulate this consumer process dying at that exact moment -- never
 * caught and handled gracefully by the loop that throws it.
 *
 * <p>This is this lab's entire crash-injection mechanism. The standalone
 * CLI apps ({@code DeliverySemanticsApp.main}) catch it only at the very
 * top level, log it, and exit via {@code System.exit(1)} -- deliberately
 * skipping {@code consumer.close()} and therefore any {@code LeaveGroup},
 * so a "restart" genuinely means "a brand-new consumer resumes from
 * whatever was actually committed," not a graceful handoff. Integration
 * tests catch it directly and construct a fresh {@code KafkaConsumer} to
 * simulate the same restart deterministically, in-process, with no OS
 * signal or real process kill involved -- see the WP-05 lab's own
 * documented finding that OS-level graceful signals are not reliably
 * deliverable from this repository's Windows automation shell; this
 * mechanism sidesteps that limitation entirely rather than working around
 * it per experiment.
 */
public final class SimulatedCrashException extends RuntimeException {

    private final FailurePoint failurePoint;

    public SimulatedCrashException(FailurePoint failurePoint, String eventId) {
        super("Simulated crash at " + failurePoint + " for eventId=" + eventId);
        this.failurePoint = failurePoint;
    }

    public FailurePoint failurePoint() {
        return failurePoint;
    }
}
