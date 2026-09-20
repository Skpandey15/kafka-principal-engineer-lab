package com.kafkalab.transactions.support;

/**
 * A deterministic, explicit point in the transactional
 * consume-transform-produce loop at which to simulate this process
 * crashing -- the same crash-injection philosophy lab-05's {@code FailurePoint}
 * uses for the non-transactional delivery-semantics loop (WP-06), applied
 * here to the transaction commit boundary itself (WP-09 Sections 15-16).
 */
public enum FailurePoint {
    /** No simulated crash -- the loop runs to completion normally. */
    NONE,

    /**
     * Crash after the output record(s) have been sent and
     * {@code sendOffsetsToTransaction()} has been called, but before
     * {@code commitTransaction()} returns. On restart, this transaction was
     * never committed -- the transaction coordinator (or the next
     * transactional producer sharing this {@code transactional.id}) aborts
     * it, so a {@code read_committed} consumer of the output topic never
     * sees these records, and the input offsets were never actually
     * advanced either. Reprocessing the same input from its last truly
     * committed offset is therefore correct, not a duplicate.
     */
    BEFORE_COMMIT,

    /**
     * Crash immediately after {@code commitTransaction()} has returned
     * successfully. The output records are already committed and visible,
     * and -- critically -- the input offsets were committed atomically as
     * part of the SAME transaction, so on restart the consumer resumes
     * strictly after the already-processed input. This is what
     * distinguishes this pipeline from lab-05's non-transactional one: a
     * crash here does not cause reprocessing.
     */
    AFTER_COMMIT
}
