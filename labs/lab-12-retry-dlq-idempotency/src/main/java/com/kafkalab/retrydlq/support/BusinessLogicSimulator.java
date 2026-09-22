package com.kafkalab.retrydlq.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic, configured-in-advance failure injection for this lab's
 * experiments -- never a random failure, matching WP-06's own
 * established convention ("Deterministic crash injection throughout,
 * never a random process kill"). A test configures exactly how many
 * times a given {@code eventId} should fail before succeeding (or that
 * it should fail forever, the "poison message" case), then asserts on
 * the real number of attempts actually made via {@link #attemptsFor}.
 */
public final class BusinessLogicSimulator {

    private final Map<String, Integer> failuresBeforeSuccess = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    /** This eventId's processing will throw on its first {@code failCount} attempts, then succeed. */
    public void failNTimesThenSucceed(String eventId, int failCount) {
        failuresBeforeSuccess.put(eventId, failCount);
    }

    /** This eventId's processing will always throw -- a permanently poisoned business event. */
    public void alwaysFail(String eventId) {
        failuresBeforeSuccess.put(eventId, Integer.MAX_VALUE);
    }

    /** Invoked by the code under test for each attempt; throws according to this eventId's configured policy. */
    public void process(OrderEvent event) throws ProcessingException {
        int attempt = attemptCounts.computeIfAbsent(event.eventId(), id -> new AtomicInteger()).incrementAndGet();
        int failCount = failuresBeforeSuccess.getOrDefault(event.eventId(), 0);
        if (attempt <= failCount) {
            throw new ProcessingException("simulated failure on attempt " + attempt + " for " + event.eventId());
        }
    }

    public int attemptsFor(String eventId) {
        AtomicInteger counter = attemptCounts.get(eventId);
        return counter == null ? 0 : counter.get();
    }

    public static final class ProcessingException extends Exception {
        public ProcessingException(String message) {
            super(message);
        }
    }
}
