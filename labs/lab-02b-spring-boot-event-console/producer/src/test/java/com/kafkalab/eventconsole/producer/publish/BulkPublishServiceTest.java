package com.kafkalab.eventconsole.producer.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.contract.ContractUnavailableException;
import com.kafkalab.eventconsole.producer.contract.ContractViolationException;
import com.kafkalab.eventconsole.producer.contract.EventContract;
import com.kafkalab.eventconsole.producer.contract.SchemaRegistryClient;
import com.kafkalab.eventconsole.producer.model.PublishJob;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.KeyStrategy;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.Mode;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.PastedEvent;
import com.kafkalab.eventconsole.producer.repo.PublishJobRepository;
import com.kafkalab.eventconsole.producer.testsupport.FakeSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Pure logic, no Docker and no broker: how requests become (key, value) pairs, how the contract
 * gates them, and how acknowledgements are tallied. The KafkaTemplate is mocked ONLY here, to
 * observe what the service sends; whether Kafka really places and stores those records is the
 * integration suite's job, against a real broker. The registry is real HTTP.
 */
class BulkPublishServiceTest {

    private FakeSchemaRegistry registry;
    private AppProperties props;
    private EventContract contract;
    private final List<String> sentKeys = new ArrayList<>();
    private final List<String> sentTopics = new ArrayList<>();
    private final List<String> sentSchemaIds = new ArrayList<>();
    private KafkaTemplate<String, String> kafka;
    private PublishJobRepository jobsRepo;
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private BulkPublishService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        registry = new FakeSchemaRegistry(7, 3, FakeSchemaRegistry.V1);
        props = new AppProperties("orders", 3, 1, 1, 1000,
                new AppProperties.Schema(registry.url(), "event-console-value", 2_000, 5_000, 60_000));
        contract = new EventContract(new SchemaRegistryClient(props), props, Clock.systemUTC());

        kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, String> sent = inv.getArgument(0);
            sentTopics.add(sent.topic());
            sentKeys.add(sent.key());
            sentSchemaIds.add(new String(sent.headers().lastHeader("x-schema-id").value()));
            // Same key -> same partition, like a real partitioner would; null keys spread out.
            int partition = sent.key() == null ? sentKeys.size() % 3 : Math.floorMod(sent.key().hashCode(), 3);
            RecordMetadata md = new RecordMetadata(new TopicPartition(sent.topic(), partition), 0, sentKeys.size(), 0L, 0, 0);
            return CompletableFuture.completedFuture(new SendResult<>(sent, md));
        });
        PublishJobRepository jobs = mock(PublishJobRepository.class);
        when(jobs.save(any(PublishJob.class))).thenAnswer(inv -> inv.getArgument(0));
        jobsRepo = jobs;
        service = new BulkPublishService(kafka, jobs, props, contract, meters);
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    private BulkPublishRequest generate(int count, KeyStrategy strategy, Integer keyCount, String fixedKey) {
        return new BulkPublishRequest(Mode.GENERATE, count, strategy, keyCount, "k-", fixedKey, "evt", null);
    }

    private static BulkPublishRequest paste(PastedEvent... events) {
        return new BulkPublishRequest(Mode.PASTE, null, null, null, null, null, null, List.of(events));
    }

    @Test
    void uniqueStrategyGivesEveryEventItsOwnKey() {
        PublishJob job = service.publish(generate(50, KeyStrategy.UNIQUE, null, null));

        assertThat(sentKeys).hasSize(50).doesNotHaveDuplicates().allMatch(k -> k.startsWith("k-"));
        assertThat(job.requested()).isEqualTo(50);
        assertThat(job.acked()).isEqualTo(50);
        assertThat(job.failed()).isZero();
    }

    @Test
    void cycleStrategyRepeatsExactlyKeyCountDistinctKeys() {
        service.publish(generate(40, KeyStrategy.CYCLE, 4, null));

        assertThat(sentKeys).hasSize(40);
        assertThat(sentKeys.stream().distinct()).hasSize(4);
        // Round-robin: key i % 4, so the first four events are the four keys in order.
        assertThat(sentKeys.subList(0, 4)).containsExactly("k-0", "k-1", "k-2", "k-3");
    }

    @Test
    void fixedStrategyUsesOneKeyForEverything() {
        service.publish(generate(25, KeyStrategy.FIXED, null, "hot"));

        assertThat(sentKeys).hasSize(25).containsOnly("hot");
    }

    @Test
    void noneStrategySendsNullKeys() {
        service.publish(generate(10, KeyStrategy.NONE, null, null));

        assertThat(sentKeys).hasSize(10).containsOnlyNulls();
    }

    @Test
    void theTopicIsAlwaysTheConfiguredOneNeverCallerChosen() {
        service.publish(generate(5, KeyStrategy.UNIQUE, null, null));

        assertThat(sentTopics).hasSize(5).containsOnly("orders");
    }

    @Test
    void pastedEventsAreSentVerbatimAndBlankKeysBecomeNull() {
        service.publish(paste(new PastedEvent("a", "{\"id\":\"1\"}"), new PastedEvent("  ", "{\"id\":\"2\"}"),
                new PastedEvent(null, "{\"id\":\"3\"}")));

        assertThat(sentKeys).containsExactly("a", null, null);
    }

    @Test
    void partitionCountsComeFromWhatKafkaAcknowledged() {
        PublishJob job = service.publish(generate(30, KeyStrategy.FIXED, null, "same"));

        // One key -> one partition: the whole batch must be tallied on a single partition.
        assertThat(job.partitionCounts()).hasSize(1);
        assertThat(job.partitionCounts().values().iterator().next()).isEqualTo(30L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aFailedSendIsCountedAsFailedNotSilentlyDropped() {
        KafkaTemplate<String, String> failing = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> boom = new CompletableFuture<>();
        boom.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(failing.send(any(ProducerRecord.class))).thenReturn(boom);
        PublishJobRepository jobs = mock(PublishJobRepository.class);
        when(jobs.save(any(PublishJob.class))).thenAnswer(inv -> inv.getArgument(0));
        BulkPublishService s = new BulkPublishService(failing, jobs, props, contract, new SimpleMeterRegistry());

        PublishJob job = s.publish(generate(3, KeyStrategy.UNIQUE, null, null));

        assertThat(job.acked()).isZero();
        assertThat(job.failed()).isEqualTo(3);
        assertThat(job.firstError()).contains("broker unavailable");
    }

    @Test
    void limitsAreEnforcedEvenIfValidationWasBypassed() {
        assertThatThrownBy(() -> service.publish(generate(1001, KeyStrategy.UNIQUE, null, null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1000");
        assertThatThrownBy(() -> service.publish(
                new BulkPublishRequest(Mode.PASTE, null, null, null, null, null, null, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.publish(
                new BulkPublishRequest(Mode.GENERATE, null, null, null, null, null, null, null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- the audit record --------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void theJobIsRecordedAsStartedBeforeAnythingIsSentThenReplacedByTheOutcome() {
        PublishJob outcome = service.publish(generate(5, KeyStrategy.UNIQUE, null, null));

        var order = inOrder(jobsRepo, kafka);
        order.verify(jobsRepo).save(argThat((PublishJob j) -> PublishJob.STARTED.equals(j.status()) && j.acked() == 0));
        order.verify(kafka, atLeastOnce()).send(any(ProducerRecord.class));
        order.verify(jobsRepo).save(argThat((PublishJob j) -> PublishJob.COMPLETED.equals(j.status()) && j.acked() == 5));
        assertThat(outcome.status()).isEqualTo(PublishJob.COMPLETED);
        assertThat(outcome.acked()).isEqualTo(5);
    }

    @Test
    void bothWritesAreTheSameJobSoTheSecondReplacesTheFirst() {
        var ids = new ArrayList<String>();
        when(jobsRepo.save(any(PublishJob.class))).thenAnswer(inv -> {
            PublishJob j = inv.getArgument(0);
            ids.add(j.id());
            return j;
        });

        service.publish(generate(3, KeyStrategy.UNIQUE, null, null));

        assertThat(ids).hasSize(2).containsOnly(ids.getFirst());
    }

    @Test
    @SuppressWarnings("unchecked")
    void ifTheStartRecordCannotBeWrittenNothingIsSentSoTheCallerMayRetrySafely() {
        when(jobsRepo.save(any(PublishJob.class))).thenThrow(new DataAccessResourceFailureException("mongo down"));

        assertThatThrownBy(() -> service.publish(generate(5, KeyStrategy.UNIQUE, null, null)))
                .isInstanceOf(AuditUnavailableException.class);

        verify(kafka, never()).send(any(ProducerRecord.class));
        assertThat(sentKeys).isEmpty();
        assertThat(meters.counter("eventconsole.audit.unavailable").count()).isEqualTo(1);
    }

    @Test
    void ifTheOutcomeCannotBeRecordedTheCallerStillGetsTheRealOutcomeInsteadOfAnErrorThatInvitesADuplicatePublish() {
        // The START row is written; the final write fails (MongoDB went away while the events were in flight).
        when(jobsRepo.save(any(PublishJob.class)))
                .thenAnswer(inv -> inv.getArgument(0))
                .thenThrow(new DataAccessResourceFailureException("mongo down"));

        PublishJob outcome = service.publish(generate(8, KeyStrategy.UNIQUE, null, null));

        assertThat(sentKeys).as("the events really were sent").hasSize(8);
        assertThat(outcome.status()).isEqualTo(PublishJob.COMPLETED_UNRECORDED);
        assertThat(outcome.acked()).as("and the caller is told what Kafka acknowledged").isEqualTo(8);
        assertThat(meters.counter("eventconsole.audit.unrecorded").count()).isEqualTo(1);
    }

    @Test
    void withAuditingNotRequiredPublishingContinuesWhenTheAuditStoreIsDown() {
        AppProperties relaxed = new AppProperties("orders", 3, 1, 1, 1000, props.schema(),
                new AppProperties.Audit(false, java.time.Duration.ofMinutes(10)));
        BulkPublishService lenient = new BulkPublishService(kafka, jobsRepo, relaxed, contract, meters);
        when(jobsRepo.save(any(PublishJob.class))).thenThrow(new DataAccessResourceFailureException("mongo down"));

        PublishJob outcome = lenient.publish(generate(4, KeyStrategy.UNIQUE, null, null));

        assertThat(sentKeys).hasSize(4);
        assertThat(outcome.status()).isEqualTo(PublishJob.COMPLETED_UNRECORDED);
        assertThat(outcome.acked()).isEqualTo(4);
    }

    @Test
    void aRejectedRequestLeavesNoAuditRowBecauseNothingWasStarted() {
        BulkPublishRequest bad = paste(new PastedEvent("a", "not json"));

        assertThatThrownBy(() -> service.publish(bad)).isInstanceOf(ContractViolationException.class);

        verify(jobsRepo, never()).save(any(PublishJob.class));
    }

    // --- the contract ----------------------------------------------------------------------

    @Test
    void generatedEventsAlwaysSatisfyTheContract() {
        PublishJob job = service.publish(generate(200, KeyStrategy.UNIQUE, null, null));

        assertThat(job.acked()).isEqualTo(200);
    }

    @Test
    void everyRecordCarriesTheIdOfTheContractVersionItWasCheckedAgainst() {
        PublishJob job = service.publish(generate(12, KeyStrategy.CYCLE, 3, null));

        assertThat(sentSchemaIds).hasSize(12).containsOnly("7");
        assertThat(job.schemaId()).isEqualTo(7);
        assertThat(job.schemaVersion()).isEqualTo(3);
    }

    @Test
    void oneBadEventPublishesNothingAtAll() {
        BulkPublishRequest req = paste(new PastedEvent("a", "{\"id\":\"ok\"}"), new PastedEvent("b", "this is not json"),
                new PastedEvent("c", "{\"id\":\"ok2\"}"));

        assertThatThrownBy(() -> service.publish(req)).isInstanceOfSatisfying(ContractViolationException.class, e -> {
            assertThat(e.total()).isEqualTo(1);
            assertThat(e.violations().getFirst().index()).isEqualTo(1);
            assertThat(e.violations().getFirst().key()).isEqualTo("b");
        });
        // Not even the two good events went out, and no audit record claims they did.
        assertThat(sentKeys).isEmpty();
        verify(kafka, never()).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void withNoContractAvailableNothingIsPublishedUnchecked() {
        registry.setDown(true);
        EventContract fresh = new EventContract(new SchemaRegistryClient(props), props, Clock.systemUTC());
        BulkPublishService s = new BulkPublishService(kafka, mock(PublishJobRepository.class), props, fresh,
                new SimpleMeterRegistry());

        assertThatThrownBy(() -> s.publish(generate(5, KeyStrategy.UNIQUE, null, null)))
                .isInstanceOf(ContractUnavailableException.class);
        verify(kafka, never()).send(any(ProducerRecord.class));
    }
}
