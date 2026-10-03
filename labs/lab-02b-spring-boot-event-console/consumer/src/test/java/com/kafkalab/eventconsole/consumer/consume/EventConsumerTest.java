package com.kafkalab.eventconsole.consumer.consume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import com.kafkalab.eventconsole.consumer.process.JsonObjectEventProcessor;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** What the consumer decides for each record, with no Kafka and no MongoDB. */
class EventConsumerTest {

    private static final Instant NOW = Instant.parse("2026-10-03T10:00:00Z");
    private static final AppProperties.Retry RETRY = new AppProperties.Retry(true, 5, 5_000, 300_000, 2_000, 50, 60_000, 15_000);
    private static final AppProperties PROPS = new AppProperties("orders", 3, 1, 1, 500, 5_000, Duration.ofDays(7), RETRY);

    private final EventQueryRepository events = mock(EventQueryRepository.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @BeforeEach
    void storeEverything() {
        when(events.insertAllIgnoringDuplicates(anyList())).thenAnswer(inv -> ((List<?>) inv.getArgument(0)).size());
    }

    private EventConsumer consumer(EventProcessor processor) {
        return new EventConsumer(events, processor, PROPS, metrics, clock);
    }

    private static ConsumerRecord<String, String> record(long offset, String value) {
        return new ConsumerRecord<>("orders", 1, offset, "key-" + offset, value);
    }

    @SuppressWarnings("unchecked")
    private List<EventDocument> stored() {
        ArgumentCaptor<List<EventDocument>> captor = ArgumentCaptor.forClass(List.class);
        verify(events).insertAllIgnoringDuplicates(captor.capture());
        return captor.getValue();
    }

    @Test
    void aValidEventIsStoredAsSuccess() {
        consumer(new JsonObjectEventProcessor()).storeBatch(List.of(record(7, "{\"a\":1}")));

        EventDocument doc = stored().getFirst();
        assertThat(doc.id()).isEqualTo("orders-1-7");
        assertThat(doc.status()).isEqualTo(EventStatus.SUCCESS);
        assertThat(doc.retries()).isZero();
        assertThat(doc.processedAt()).isEqualTo(NOW);
        assertThat(doc.nextRetryAt()).isNull();
        assertThat(doc.lastError()).isNull();
    }

    @Test
    void anInvalidEventIsStoredAsFailedWithItsReasonAndAFirstRetryTime() {
        consumer(new JsonObjectEventProcessor()).storeBatch(List.of(record(8, "not json at all")));

        EventDocument doc = stored().getFirst();
        assertThat(doc.status()).isEqualTo(EventStatus.FAILED);
        assertThat(doc.retries()).isZero();
        assertThat(doc.lastError()).startsWith("EventProcessingException: value is not valid JSON");
        assertThat(doc.nextRetryAt()).isEqualTo(NOW.plusSeconds(5));
        assertThat(doc.processedAt()).isNull();
        // The payload is kept exactly as received: the retry worker needs it, and so does an operator.
        assertThat(doc.value()).isEqualTo("not json at all");
    }

    @Test
    void aBadEventNeverStopsItsNeighboursFromBeingStoredInTheSameWrite() {
        int inserted = consumer(new JsonObjectEventProcessor())
                .storeBatch(List.of(record(1, "{}"), record(2, "oops"), record(3, "{}")));

        assertThat(inserted).isEqualTo(3);
        assertThat(stored()).extracting(EventDocument::status)
                .containsExactly(EventStatus.SUCCESS, EventStatus.FAILED, EventStatus.SUCCESS);
        assertThat(metrics.counter("eventconsole.consume.failed").count()).isEqualTo(1);
        assertThat(metrics.counter("eventconsole.consume.stored").count()).isEqualTo(3);
    }

    @Test
    void aTombstoneIsAFailureNotACrash() {
        consumer(new JsonObjectEventProcessor()).storeBatch(List.of(record(4, null)));

        assertThat(stored().getFirst().status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored().getFirst().lastError()).contains("value is empty");
    }

    @Test
    void anyExceptionFromTheProcessorIsAFailureOfThatEventOnly() {
        EventProcessor buggy = (key, value) -> {
            throw new NullPointerException("a bug in business code");
        };

        consumer(buggy).storeBatch(List.of(record(5, "{}")));

        assertThat(stored().getFirst().status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored().getFirst().lastError()).startsWith("NullPointerException");
    }

    @Test
    void theStoredReasonIsBounded() {
        EventProcessor chatty = (key, value) -> {
            throw new IllegalStateException("x".repeat(5_000));
        };

        consumer(chatty).storeBatch(List.of(record(6, "{}")));

        assertThat(stored().getFirst().lastError()).hasSize(EventConsumer.MAX_ERROR_LENGTH);
    }

    @Test
    void aDatabaseOutageIsNotSwallowedSoThatKafkaKeepsTheOffsetAndTheBatchIsRetried() {
        when(events.insertAllIgnoringDuplicates(anyList())).thenThrow(new DataAccessResourceFailureException("mongo down"));

        assertThatThrownBy(() -> consumer(new JsonObjectEventProcessor()).storeBatch(List.of(record(9, "{}"))))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }
}
