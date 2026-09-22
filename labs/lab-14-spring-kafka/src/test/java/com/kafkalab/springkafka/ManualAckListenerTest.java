package com.kafkalab.springkafka;

import com.kafkalab.springkafka.listener.ManualAckListener;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Spring application context + a real, embedded (in-process, not
 * mocked) Kafka broker -- {@code @EmbeddedKafka} is Spring Kafka's own
 * first-class test support, the tool this WP's reference material
 * (`docs/references/REFERENCE_REPOSITORIES.md`) names specifically.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "manual-ack-in")
class ManualAckListenerTest {

    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private ManualAckListener listener;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void unacknowledgedRecordsOffsetIsNeverCommittedWhileAcknowledgedOnesIs() throws Exception {
        kafkaTemplate.send(new ProducerRecord<>("manual-ack-in", "k1", "value-1")).get();
        // The LAST record on the partition, deliberately never
        // acknowledged -- nothing sent AFTER it could accidentally
        // "overtake" its commit, isolating exactly what MANUAL ack mode
        // does with a genuinely unacknowledged record.
        kafkaTemplate.send(new ProducerRecord<>("manual-ack-in", "skip-ack", "value-2")).get();

        waitUntil(() -> listener.processedKeys().contains("k1") && listener.processedKeys().contains("skip-ack"),
                Duration.ofSeconds(20));

        // k1 was acknowledged (offset 0 -> committed offset 1);
        // skip-ack (offset 1) never was. The native mechanism this
        // wraps: WP-06's own manual commitSync() -- an offset this
        // consumer group never explicitly commits is never durably
        // recorded, exactly like calling commitSync() for some records
        // and deliberately skipping it for another.
        waitUntil(() -> committedOffset("lab14-manual-ack-group", "manual-ack-in") != null
                        && committedOffset("lab14-manual-ack-group", "manual-ack-in") == 1L,
                Duration.ofSeconds(20));

        Long committed = committedOffset("lab14-manual-ack-group", "manual-ack-in");
        assertEquals(1L, committed,
                "the committed offset must stop at k1 (offset 0, committed as 1) -- skip-ack's offset (1) was never acknowledged and must never be committed");
    }

    private static Long committedOffset(String groupId, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("spring.embedded.kafka.brokers")))) {
            var result = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            var offsetAndMetadata = result.get(new TopicPartition(topic, 0));
            return offsetAndMetadata == null ? null : offsetAndMetadata.offset();
        }
    }

    private static void waitUntil(ThrowingBooleanSupplier condition, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
