package com.kafkalab.eventconsole.consumer.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;

/**
 * The line between "the database is down, wait" and "this record is bad, dead-letter it" is
 * the most consequential decision in the consumer: getting it wrong either empties the topic
 * into the DLT during an outage, or retries a poison record forever.
 */
class ConsumerErrorClassificationTest {

    @Test
    void databaseUnavailabilityIsAnOutage() {
        assertThat(ConsumerErrorHandlingConfig.isOutage(new DataAccessResourceFailureException("down"))).isTrue();
        assertThat(ConsumerErrorHandlingConfig.isOutage(new TransientDataAccessResourceException("blip"))).isTrue();
    }

    @Test
    void anOutageWrappedByFrameworkExceptionsIsStillRecognised() {
        // Spring Kafka wraps whatever the listener throws; the handler sees the wrapper.
        Exception wrapped = new RuntimeException("Listener failed",
                new IllegalStateException("outer", new DataAccessResourceFailureException("mongo timeout")));

        assertThat(ConsumerErrorHandlingConfig.isOutage(wrapped)).isTrue();
    }

    @Test
    void anythingElseIsTreatedAsARecordProblem() {
        assertThat(ConsumerErrorHandlingConfig.isOutage(new IllegalArgumentException("bad record"))).isFalse();
        assertThat(ConsumerErrorHandlingConfig.isOutage(new RuntimeException("x", new NullPointerException()))).isFalse();
    }

    @Test
    void aSelfReferencingCauseChainDoesNotLoopForever() {
        Exception selfCause = new RuntimeException("loop") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        assertThat(ConsumerErrorHandlingConfig.isOutage(selfCause)).isFalse();
    }
}
