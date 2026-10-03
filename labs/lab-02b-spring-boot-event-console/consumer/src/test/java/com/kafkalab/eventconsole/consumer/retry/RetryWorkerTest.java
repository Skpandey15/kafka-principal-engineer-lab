package com.kafkalab.eventconsole.consumer.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The worker's decisions, one event at a time: what it records when a retry succeeds, fails, or
 * uses up the last allowed attempt, and what it does when the world misbehaves. The storage
 * semantics (atomic claims, leases, conditional updates) are proved against a real MongoDB in the
 * integration tests.
 */
class RetryWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-03T10:00:00Z");
    private static final AppProperties.Retry RETRY = new AppProperties.Retry(true, 5, 5_000, 300_000, 2_000, 50, 60_000, 15_000);
    private static final AppProperties PROPS = new AppProperties("orders", 3, 1, 1, 500, 5_000, Duration.ofDays(7), RETRY,
            new AppProperties.Schema("http://localhost:8081", 500, 1_000));

    private final EventRetryRepository repository = mock(EventRetryRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    private EventProcessor processor = event -> {
    };

    private RetryWorker worker() {
        return new RetryWorker(repository, event -> processor.process(event), kafka, PROPS, metrics, clock);
    }

    private static EventDocument failed(int retries) {
        return new EventDocument("orders-1-42", "orders", 1, 42, "k", "{\"a\":1}", "1", NOW.minusSeconds(60), NOW.minusSeconds(60),
                EventStatus.FAILED, retries, "earlier error", NOW.minusSeconds(1), NOW.plusSeconds(60), null, null, null);
    }

    private static EventDocument dead() {
        return new EventDocument("orders-1-43", "orders", 1, 43, "k", "bad", "1", NOW, NOW, EventStatus.DEAD, 5, "gave up",
                null, NOW.plusSeconds(60), null, NOW, false);
    }

    private void oneDueEvent(EventDocument event) {
        when(repository.claimDueForRetry(any(), any())).thenReturn(Optional.of(event)).thenReturn(Optional.empty());
        when(repository.claimDeadForDlt(any(), any())).thenReturn(Optional.empty());
    }

    @Test
    void aRetryThatSucceedsMarksTheEventSuccessful() {
        oneDueEvent(failed(2));
        when(repository.markSucceeded("orders-1-42", 2, NOW)).thenReturn(true);

        worker().runOnce();

        verify(repository).markSucceeded("orders-1-42", 2, NOW);
        verify(repository, never()).markRetryFailed(anyString(), anyInt(), anyString(), any());
        verify(repository, never()).markDead(anyString(), anyInt(), anyString(), any());
        assertThat(metrics.counter("eventconsole.retry.succeeded").count()).isEqualTo(1);
    }

    @Test
    void aRetryThatFailsSchedulesTheNextOneWithDoubledBackoff() {
        oneDueEvent(failed(0));
        processor = event -> {
            throw new EventProcessingException("still broken");
        };
        when(repository.markRetryFailed(anyString(), anyInt(), anyString(), any())).thenReturn(true);

        worker().runOnce();

        // First retry just failed: the next one waits 10s (initial 5s, doubled once).
        verify(repository).markRetryFailed("orders-1-42", 0, "EventProcessingException: still broken", NOW.plusSeconds(10));
        verify(repository, never()).markDead(anyString(), anyInt(), anyString(), any());
        assertThat(metrics.counter("eventconsole.retry.failed").count()).isEqualTo(1);
    }

    @Test
    void theFifthFailedRetryMakesTheEventDeadInsteadOfSchedulingASixth() {
        oneDueEvent(failed(4));
        processor = event -> {
            throw new EventProcessingException("never going to work");
        };
        when(repository.markDead(anyString(), anyInt(), anyString(), any())).thenReturn(true);

        worker().runOnce();

        verify(repository).markDead("orders-1-42", 4, "EventProcessingException: never going to work", NOW);
        verify(repository, never()).markRetryFailed(anyString(), anyInt(), anyString(), any());
        assertThat(metrics.counter("eventconsole.retry.dead").count()).isEqualTo(1);
    }

    @Test
    void theFourthFailedRetryStillSchedulesAFifth() {
        oneDueEvent(failed(3));
        processor = event -> {
            throw new EventProcessingException("nope");
        };
        when(repository.markRetryFailed(anyString(), anyInt(), anyString(), any())).thenReturn(true);

        worker().runOnce();

        verify(repository).markRetryFailed(eq("orders-1-42"), eq(3), anyString(), eq(NOW.plusSeconds(80)));
        verify(repository, never()).markDead(anyString(), anyInt(), anyString(), any());
    }

    @Test
    void anyExceptionFromTheProcessorCountsAsAFailedAttemptNotACrashOfTheWorker() {
        oneDueEvent(failed(0));
        processor = event -> {
            throw new IllegalStateException("downstream exploded");
        };
        when(repository.markRetryFailed(anyString(), anyInt(), anyString(), any())).thenReturn(true);

        worker().runOnce();

        verify(repository).markRetryFailed(eq("orders-1-42"), eq(0), eq("IllegalStateException: downstream exploded"), any());
    }

    @Test
    void theRetryIsGivenTheSchemaIdTheEventDeclaredWhenItWasConsumed() {
        oneDueEvent(failed(0));
        java.util.concurrent.atomic.AtomicReference<com.kafkalab.eventconsole.consumer.process.ConsumedEvent> seen =
                new java.util.concurrent.atomic.AtomicReference<>();
        processor = seen::set;
        when(repository.markSucceeded(anyString(), anyInt(), any())).thenReturn(true);

        worker().runOnce();

        assertThat(seen.get()).isEqualTo(new com.kafkalab.eventconsole.consumer.process.ConsumedEvent("k", "{\"a\":1}", "1"));
    }

    @Test
    void aSchemaRegistryOutageDuringARetryRecordsNothingAndCountsNoAttempt() {
        oneDueEvent(failed(2));
        processor = event -> {
            throw new com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException("registry down");
        };

        worker().runOnce(); // must not throw, and must not decide anything about the event

        verify(repository, never()).markSucceeded(anyString(), anyInt(), any());
        verify(repository, never()).markRetryFailed(anyString(), anyInt(), anyString(), any());
        verify(repository, never()).markDead(anyString(), anyInt(), anyString(), any());
        assertThat(metrics.counter("eventconsole.retry.failed").count()).isZero();
    }

    @Test
    void aSchemaRegistryOutageStopsTheRetryPassButNotTheDeadLetterSweep() {
        when(repository.claimDueForRetry(any(), any())).thenReturn(Optional.of(failed(0)));
        when(repository.claimDeadForDlt(any(), any())).thenReturn(Optional.of(dead())).thenReturn(Optional.empty());
        processor = event -> {
            throw new com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException("registry down");
        };
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture((SendResult<String, String>) null));

        worker().runOnce();

        // One claim only: the pass stopped at the first dependency failure instead of leasing every due event...
        verify(repository, times(1)).claimDueForRetry(any(), any());
        // ...and the dead letters, which need no registry, still went out.
        verify(repository).markDltPublished("orders-1-43");
    }

    @Test
    void aStaleResultIsNotCounted() {
        // Another worker took the event over (our lease ran out) and already decided: our update matches nothing.
        oneDueEvent(failed(1));
        when(repository.markSucceeded(anyString(), anyInt(), any())).thenReturn(false);

        worker().runOnce();

        assertThat(metrics.counter("eventconsole.retry.succeeded").count()).isZero();
    }

    @Test
    void aDatabaseOutageWhileClaimingPropagatesSoNoAttemptIsRecordedOrCounted() {
        when(repository.claimDueForRetry(any(), any())).thenThrow(new DataAccessResourceFailureException("mongo down"));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> worker().runOnce())
                .isInstanceOf(DataAccessResourceFailureException.class);
        verify(repository, never()).markRetryFailed(anyString(), anyInt(), anyString(), any());
        verify(repository, never()).markDead(anyString(), anyInt(), anyString(), any());
    }

    @Test
    void theSchedulerEntryPointSurvivesADatabaseOutage() {
        when(repository.claimDueForRetry(any(), any())).thenThrow(new DataAccessResourceFailureException("mongo down"));

        worker().tick(); // must not throw: the next tick simply tries again
    }

    @Test
    void aDeadEventIsPublishedToTheDeadLetterTopicWithItsHistoryAndOnlyThenConfirmed() {
        when(repository.claimDueForRetry(any(), any())).thenReturn(Optional.empty());
        when(repository.claimDeadForDlt(any(), any())).thenReturn(Optional.of(dead())).thenReturn(Optional.empty());
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture((SendResult<String, String>) null));

        worker().runOnce();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(sent.capture());
        ProducerRecord<String, String> record = sent.getValue();
        assertThat(record.topic()).isEqualTo("orders.DLT");
        assertThat(record.partition()).isEqualTo(1);
        assertThat(record.key()).isEqualTo("k");
        assertThat(record.value()).isEqualTo("bad");
        assertThat(new String(record.headers().lastHeader("x-event-id").value())).isEqualTo("orders-1-43");
        assertThat(new String(record.headers().lastHeader("x-original-offset").value())).isEqualTo("43");
        assertThat(new String(record.headers().lastHeader("x-retries").value())).isEqualTo("5");
        assertThat(new String(record.headers().lastHeader("x-last-error").value())).isEqualTo("gave up");
        verify(repository).markDltPublished("orders-1-43");
    }

    @Test
    void ifTheBrokerRejectsTheDeadLetterWriteTheEventStaysUnconfirmedForALaterPass() {
        when(repository.claimDueForRetry(any(), any())).thenReturn(Optional.empty());
        when(repository.claimDeadForDlt(any(), any())).thenReturn(Optional.of(dead())).thenReturn(Optional.empty());
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        worker().runOnce();

        verify(repository, never()).markDltPublished(anyString());
        assertThat(metrics.counter("eventconsole.retry.dlt.published").count()).isZero();
    }
}
