package com.kafkalab.eventconsole.consumer.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

class RetryPolicyTest {

    private final AppProperties.Retry retry = new AppProperties.Retry(true, 5, 5_000, 300_000, 2_000, 50, 60_000, 15_000);

    @Test
    void theWaitBeforeEachRetryDoublesAndIsCapped() {
        assertThat(retry.delayAfter(0)).isEqualTo(Duration.ofSeconds(5));
        assertThat(retry.delayAfter(1)).isEqualTo(Duration.ofSeconds(10));
        assertThat(retry.delayAfter(2)).isEqualTo(Duration.ofSeconds(20));
        assertThat(retry.delayAfter(3)).isEqualTo(Duration.ofSeconds(40));
        assertThat(retry.delayAfter(4)).isEqualTo(Duration.ofSeconds(80));
        assertThat(retry.delayAfter(6)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void aHugeRetryCountNeverOverflowsIntoANegativeOrZeroWait() {
        assertThat(retry.delayAfter(1_000)).isEqualTo(Duration.ofMinutes(5));
        assertThat(retry.delayAfter(Integer.MAX_VALUE)).isEqualTo(Duration.ofMinutes(5));
        assertThat(retry.delayAfter(-3)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void theLastAllowedRetryIsTheOneThatExhaustsTheBudget() {
        assertThat(retry.isExhausted(1)).isFalse();
        assertThat(retry.isExhausted(4)).isFalse();
        assertThat(retry.isExhausted(5)).isTrue();
        assertThat(retry.isExhausted(6)).isTrue();
    }

    @Test
    void whenTheDatabaseIsDownTheConsumerRetriesForeverWithACappedBackoff() {
        AppProperties props = new AppProperties("orders", 3, 1, 1, 500, 5_000, Duration.ofDays(7), retry,
                new AppProperties.Schema("http://localhost:8081", 500, 1_000));
        BackOffExecution execution = ((BackOff) ConsumerErrorHandlingConfig.retryForever(props)).start();

        assertThat(execution.nextBackOff()).isEqualTo(500);
        long previous = 0;
        for (int i = 0; i < 10_000; i++) {
            long next = execution.nextBackOff();
            assertThat(next).as("attempt %d", i).isNotEqualTo(BackOffExecution.STOP).isLessThanOrEqualTo(5_000);
            assertThat(next).isGreaterThanOrEqualTo(previous);
            previous = next;
        }
        assertThat(previous).isEqualTo(5_000);
    }
}
