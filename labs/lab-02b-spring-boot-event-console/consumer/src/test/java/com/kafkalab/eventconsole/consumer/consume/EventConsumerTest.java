package com.kafkalab.eventconsole.consumer.consume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.contract.ContractEventProcessor;
import com.kafkalab.eventconsole.consumer.contract.EventContract;
import com.kafkalab.eventconsole.consumer.contract.SchemaRegistryClient;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import com.kafkalab.eventconsole.consumer.testsupport.FakeSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** What the consumer decides for each record, with no Kafka and no MongoDB (the registry is a real HTTP fake). */
class EventConsumerTest {

    private static final Instant NOW = Instant.parse("2026-10-03T10:00:00Z");
    private static final AppProperties.Retry RETRY = new AppProperties.Retry(true, 5, 5_000, 300_000, 2_000, 50, 60_000, 15_000);

    private final EventQueryRepository events = mock(EventQueryRepository.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    private FakeSchemaRegistry registry;
    private AppProperties props;
    private EventProcessor contractRules;

    @BeforeEach
    void start() {
        registry = new FakeSchemaRegistry().withJsonSchema(1, FakeSchemaRegistry.V1);
        props = new AppProperties("orders", 3, 1, 1, 500, 5_000, Duration.ofDays(7), RETRY,
                new AppProperties.Schema(registry.url(), 2_000, 5_000));
        contractRules = new ContractEventProcessor(new EventContract(new SchemaRegistryClient(props)));
        when(events.insertAllIgnoringDuplicates(anyList())).thenAnswer(inv -> ((List<?>) inv.getArgument(0)).size());
    }

    @AfterEach
    void stop() {
        registry.close();
    }

    private EventConsumer consumer(EventProcessor processor) {
        return new EventConsumer(events, processor, props, metrics, clock);
    }

    private static ConsumerRecord<String, String> record(long offset, String value, String schemaId) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 1, offset, "key-" + offset, value);
        if (schemaId != null) {
            record.headers().add("x-schema-id", schemaId.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    private static ConsumerRecord<String, String> record(long offset, String value) {
        return record(offset, value, "1");
    }

    @SuppressWarnings("unchecked")
    private List<EventDocument> stored() {
        ArgumentCaptor<List<EventDocument>> captor = ArgumentCaptor.forClass(List.class);
        verify(events).insertAllIgnoringDuplicates(captor.capture());
        return captor.getValue();
    }

    @Test
    void anEventThatFollowsItsContractIsStoredAsSuccessWithTheSchemaItDeclared() {
        consumer(contractRules).storeBatch(List.of(record(7, "{\"id\":\"a\"}")));

        EventDocument doc = stored().getFirst();
        assertThat(doc.id()).isEqualTo("orders-1-7");
        assertThat(doc.status()).isEqualTo(EventStatus.SUCCESS);
        assertThat(doc.schemaId()).isEqualTo("1");
        assertThat(doc.retries()).isZero();
        assertThat(doc.processedAt()).isEqualTo(NOW);
        assertThat(doc.nextRetryAt()).isNull();
        assertThat(doc.lastError()).isNull();
    }

    @Test
    void anEventThatBreaksItsContractIsStoredAsFailedWithTheViolationAndAFirstRetryTime() {
        consumer(contractRules).storeBatch(List.of(record(8, "{\"seq\":1}")));

        EventDocument doc = stored().getFirst();
        assertThat(doc.status()).isEqualTo(EventStatus.FAILED);
        assertThat(doc.retries()).isZero();
        assertThat(doc.lastError()).startsWith("EventProcessingException: violates schema 1:").contains("id");
        assertThat(doc.nextRetryAt()).isEqualTo(NOW.plusSeconds(5));
        assertThat(doc.processedAt()).isNull();
        // The payload and its declared schema are kept exactly as received: the retry worker needs both.
        assertThat(doc.value()).isEqualTo("{\"seq\":1}");
        assertThat(doc.schemaId()).isEqualTo("1");
    }

    @Test
    void anEventWithNoSchemaHeaderIsFailedBecauseItDeclaresNoContract() {
        consumer(contractRules).storeBatch(List.of(record(9, "{\"id\":\"a\"}", null)));

        EventDocument doc = stored().getFirst();
        assertThat(doc.status()).isEqualTo(EventStatus.FAILED);
        assertThat(doc.schemaId()).isNull();
        assertThat(doc.lastError()).contains("no x-schema-id header");
    }

    @Test
    void aBadEventNeverStopsItsNeighboursFromBeingStoredInTheSameWrite() {
        int inserted = consumer(contractRules)
                .storeBatch(List.of(record(1, "{\"id\":\"a\"}"), record(2, "oops"), record(3, "{\"id\":\"c\"}")));

        assertThat(inserted).isEqualTo(3);
        assertThat(stored()).extracting(EventDocument::status)
                .containsExactly(EventStatus.SUCCESS, EventStatus.FAILED, EventStatus.SUCCESS);
        assertThat(metrics.counter("eventconsole.consume.failed").count()).isEqualTo(1);
        assertThat(metrics.counter("eventconsole.consume.stored").count()).isEqualTo(3);
    }

    @Test
    void aTombstoneIsAFailureNotACrash() {
        consumer(contractRules).storeBatch(List.of(record(4, null)));

        assertThat(stored().getFirst().status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored().getFirst().lastError()).contains("value is empty");
    }

    @Test
    void anyExceptionFromTheProcessorIsAFailureOfThatEventOnly() {
        EventProcessor buggy = event -> {
            throw new NullPointerException("a bug in business code");
        };

        consumer(buggy).storeBatch(List.of(record(5, "{}")));

        assertThat(stored().getFirst().status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored().getFirst().lastError()).startsWith("NullPointerException");
    }

    @Test
    void theStoredReasonIsBounded() {
        EventProcessor chatty = event -> {
            throw new IllegalStateException("x".repeat(5_000));
        };

        consumer(chatty).storeBatch(List.of(record(6, "{}")));

        assertThat(stored().getFirst().lastError()).hasSize(EventConsumer.MAX_ERROR_LENGTH);
    }

    @Test
    void aDatabaseOutageIsNotSwallowedSoThatKafkaKeepsTheOffsetAndTheBatchIsRetried() {
        when(events.insertAllIgnoringDuplicates(anyList())).thenThrow(new DataAccessResourceFailureException("mongo down"));

        assertThatThrownBy(() -> consumer(contractRules).storeBatch(List.of(record(9, "{\"id\":\"a\"}"))))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }

    @Test
    void aSchemaRegistryOutageIsNotAFailureOfAnyEventSoNothingIsStoredAndTheBatchWaits() {
        registry.setDown(true);

        assertThatThrownBy(() -> consumer(contractRules)
                .storeBatch(List.of(record(10, "{\"id\":\"a\"}"), record(11, "{\"id\":\"b\"}"))))
                .isInstanceOf(InfrastructureUnavailableException.class);

        // Not a single event was recorded as FAILED because of somebody else's outage.
        verify(events, never()).insertAllIgnoringDuplicates(anyList());
    }
}
