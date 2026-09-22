package com.kafkalab.springkafka;

import com.kafkalab.springkafka.txn.TransactionalOrderPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The native mechanism {@code @Transactional} + {@code TransactionalProducerConfig}'s
 * dedicated {@code KafkaTransactionManager} wraps: WP-09's own
 * hand-configured transactional producer (init/begin/commit/abort). A
 * {@code read_committed} consumer must never see a rolled-back send,
 * and must see a committed one -- the exact same guarantee, this time
 * declarative.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "transactional-in")
class TransactionalOrderPublisherTest {

    @Autowired
    private TransactionalOrderPublisher publisher;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void aRolledBackSendIsNeverVisibleUnderReadCommittedWhileACommittedOneIs() {
        assertThrowsIllegalState(() -> publisher.publish("transactional-in", "rolled-back", "must-never-be-visible", true));
        publisher.publish("transactional-in", "committed", "must-be-visible", false);

        List<ConsumerRecord<String, String>> visible = consumeReadCommitted("transactional-in", Duration.ofSeconds(15));
        assertEquals(1, visible.size(), "only the committed send must be visible under read_committed: " + visible);
        assertEquals("committed", visible.get(0).key());
        assertEquals("must-be-visible", visible.get(0).value());
    }

    private static void assertThrowsIllegalState(Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("forced rollback"));
        }
    }

    private static List<ConsumerRecord<String, String>> consumeReadCommitted(String topic, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("spring.embedded.kafka.brokers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-txn-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                records.forEach(collected::add);
            }
        }
        return collected;
    }
}
