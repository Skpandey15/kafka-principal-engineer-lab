package com.kafkalab.eventconsole.consumer.model;

/**
 * Where an event is in its life. Exactly one document exists per Kafka record, and it only ever
 * moves forward through this state machine:
 *
 * <pre>
 *            consume                       retry succeeds
 *   record ---------&gt; SUCCESS     FAILED ----------------&gt; SUCCESS
 *      |                            |  ^
 *      | processing fails           |  | requeue (operator)
 *      +----------&gt; FAILED          |  |
 *                                   |  |
 *                      retries used up |
 *                                   v  |
 *                                  DEAD
 * </pre>
 */
public enum EventStatus {
    /** Processed and stored. The only state that is allowed to expire (see the partial TTL index). */
    SUCCESS,
    /** Processing failed; the retry worker will try again when {@code nextRetryAt} is due. */
    FAILED,
    /** Every retry failed. Parked here (and published to the dead-letter topic) until an operator acts. */
    DEAD
}
