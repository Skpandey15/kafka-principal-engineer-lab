package com.kafkalab.eventconsole.consumer.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class EventStatusMetricsTest {

    private final EventQueryRepository events = mock(EventQueryRepository.class);
    private final EventRetryRepository retries = mock(EventRetryRepository.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final EventStatusMetrics metrics = new EventStatusMetrics(events, retries, registry);

    private double gauge(String status) {
        return registry.get("eventconsole.events").tag("status", status).gauge().value();
    }

    @Test
    void theGaugesReportWhatMongoDbHolds() {
        when(events.countByStatus()).thenReturn(Map.of("SUCCESS", 1200L, "FAILED", 7L, "DEAD", 3L));
        when(retries.countUnconfirmedDeadLetters()).thenReturn(2L);

        metrics.refresh();

        assertThat(gauge("SUCCESS")).isEqualTo(1200);
        assertThat(gauge("FAILED")).isEqualTo(7);
        assertThat(gauge("DEAD")).isEqualTo(3);
        assertThat(registry.get("eventconsole.dead.letters.unconfirmed").gauge().value()).isEqualTo(2);
    }

    @Test
    void everyStatusHasAGaugeFromTheStartSoAnAlertNeverSeesAMissingSeries() {
        assertThat(gauge("SUCCESS")).isZero();
        assertThat(gauge("FAILED")).isZero();
        assertThat(gauge("DEAD")).isZero();
    }

    @Test
    void aMongoDbOutageKeepsTheLastValuesAndIsCounted() {
        when(events.countByStatus()).thenReturn(Map.of("DEAD", 4L));
        when(retries.countUnconfirmedDeadLetters()).thenReturn(0L);
        metrics.refresh();

        when(events.countByStatus()).thenThrow(new DataAccessResourceFailureException("mongo down"));
        metrics.refresh();
        metrics.refresh();

        assertThat(gauge("DEAD")).as("stale, not zero: a frozen gauge is better than a false all-clear").isEqualTo(4);
        assertThat(registry.get("eventconsole.metrics.refresh.failures").counter().count()).isEqualTo(2);
    }
}
