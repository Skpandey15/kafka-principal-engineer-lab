package com.kafkalab.eventconsole.producer.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.model.PublishJob;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.KeyStrategy;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.Mode;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.PastedEvent;
import com.kafkalab.eventconsole.producer.repo.PublishJobRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Pure logic, no Docker and no broker: how requests become (key, value) pairs and how
 * acknowledgements are tallied. The KafkaTemplate is mocked ONLY here, to observe what the
 * service sends; whether Kafka really places and stores those records is the integration
 * suite's job, against a real broker.
 */
class BulkPublishServiceTest {

    private final AppProperties props = new AppProperties("orders", 3, 1, 1, 1000);
    private final List<String> sentKeys = new ArrayList<>();
    private final List<String> sentTopics = new ArrayList<>();
    private BulkPublishService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(String.class), any(), any(String.class))).thenAnswer(inv -> {
            String topic = inv.getArgument(0);
            String key = inv.getArgument(1);
            sentTopics.add(topic);
            sentKeys.add(key);
            // Same key -> same partition, like a real partitioner would; null keys spread out.
            int partition = key == null ? sentKeys.size() % 3 : Math.floorMod(key.hashCode(), 3);
            RecordMetadata md = new RecordMetadata(new TopicPartition(topic, partition), 0, sentKeys.size(), 0L, 0, 0);
            return CompletableFuture.completedFuture(new SendResult<>(new ProducerRecord<>(topic, key, "v"), md));
        });
        PublishJobRepository jobs = mock(PublishJobRepository.class);
        when(jobs.save(any(PublishJob.class))).thenAnswer(inv -> inv.getArgument(0));
        service = new BulkPublishService(kafka, jobs, props, new SimpleMeterRegistry());
    }

    private BulkPublishRequest generate(int count, KeyStrategy strategy, Integer keyCount, String fixedKey) {
        return new BulkPublishRequest(Mode.GENERATE, count, strategy, keyCount, "k-", fixedKey, "evt", null);
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
        BulkPublishRequest req = new BulkPublishRequest(Mode.PASTE, null, null, null, null, null, null,
                List.of(new PastedEvent("a", "1"), new PastedEvent("  ", "2"), new PastedEvent(null, "3")));

        service.publish(req);

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
        when(failing.send(eq("orders"), any(), any(String.class))).thenReturn(boom);
        PublishJobRepository jobs = mock(PublishJobRepository.class);
        when(jobs.save(any(PublishJob.class))).thenAnswer(inv -> inv.getArgument(0));
        BulkPublishService s = new BulkPublishService(failing, jobs, props, new SimpleMeterRegistry());

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
}
