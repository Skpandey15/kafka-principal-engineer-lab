package com.kafkalab.deliverysemantics.support;

/**
 * A deterministic, explicit point in the poll -&gt; process -&gt; commit loop
 * at which to simulate this consumer crashing -- the crash-injection
 * framework WP-06 asks for, so every delivery-semantics experiment in this
 * lab is reproducible on demand rather than depending on randomly killing
 * a process (see {@link SimulatedCrashException}).
 */
public enum FailurePoint {
    /** No simulated crash -- the loop runs to completion normally. */
    NONE,

    /**
     * Crash before this record's business processing runs at all. Paired
     * with {@code commitTiming=AFTER_PROCESS}, this is a plain "died
     * before doing anything with this record" case -- nothing was
     * committed and nothing was processed, so replay on restart is simply
     * correct, not a duplicate.
     */
    BEFORE_PROCESS,

    /**
     * Crash after this record has been processed (its business side
     * effect has already happened) but before its offset is committed.
     * This is the at-least-once duplicate window: on restart, this
     * record is fetched and processed again, because Kafka has no record
     * that it was ever finished.
     */
    AFTER_PROCESS_BEFORE_COMMIT,

    /**
     * Crash after this record's offset has already been committed, but
     * before its business processing has run. This is the at-most-once
     * loss window: on restart, the consumer resumes strictly AFTER this
     * offset -- this record's processing never happens, ever.
     */
    AFTER_COMMIT_BEFORE_PROCESS
}
