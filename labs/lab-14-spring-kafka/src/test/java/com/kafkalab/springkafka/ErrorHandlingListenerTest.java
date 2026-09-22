package com.kafkalab.springkafka;

import com.kafkalab.springkafka.listener.ErrorHandlingListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The native mechanism {@code errorHandlingContainerFactory}'s
 * {@code DefaultErrorHandler} + {@code DeadLetterPublishingRecoverer}
 * wraps is WP-13's own hand-rolled {@code RetryingRecordProcessor} --
 * bounded retry, then a DLQ, this time entirely framework-provided.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "error-handling-in")
class ErrorHandlingListenerTest {

    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private ErrorHandlingListener listener;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void aPermanentlyFailingRecordIsRetriedThenRoutedToTheDeadLetterTopicWithRealHeaders() throws Exception {
        kafkaTemplate.send(new ProducerRecord<>("error-handling-in", "poison", "poison-payload")).get();

        // FixedBackOff(200L, 2L): 2 retries after the first attempt --
        // 3 real attempts total -- before the DefaultErrorHandler gives
        // up and hands the record to the recoverer.
        waitUntil(() -> listener.attemptsFor("poison") == 3, Duration.ofSeconds(20));

        // Spring Kafka's DeadLetterPublishingRecoverer's real default
        // topic-naming convention (confirmed against the real class
        // file's own constant, DeadLetterPublishingRecoverer.class):
        // "<topic>-dlt" -- NOT ".DLT" as a first guess assumed.
        ConsumerRecord<String, String> dltRecord = consumeOne("error-handling-in-dlt", Duration.ofSeconds(20));
        assertEquals("poison-payload", dltRecord.value(), "the DLT record must preserve the original payload unmodified");
        assertEquals("poison", dltRecord.key());
        // A real finding: the listener's own exception is WRAPPED --
        // "kafka_dlt-exception-fqcn" is Spring's own
        // ListenerExecutionFailedException, not the listener's raw
        // IllegalStateException. The original cause lives in a
        // SEPARATE header, "kafka_dlt-exception-cause-fqcn".
        assertTrue(headerValue(dltRecord, "kafka_dlt-exception-fqcn").contains("ListenerExecutionFailedException"));
        assertTrue(headerValue(dltRecord, "kafka_dlt-exception-cause-fqcn").contains("IllegalStateException"));
        assertEquals("error-handling-in", headerValue(dltRecord, "kafka_dlt-original-topic"));
        // Another real finding: unlike WP-13's own hand-rolled
        // recoverer (which encoded original-partition/offset as UTF-8
        // text), Spring's real DeadLetterPublishingRecoverer encodes
        // these as raw big-endian binary (int32/int64) headers, not
        // strings -- decoded here via ByteBuffer, not `new String(...)`.
        assertEquals(0, headerIntValue(dltRecord, "kafka_dlt-original-partition"));
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int headerIntValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return java.nio.ByteBuffer.wrap(header.value()).getInt();
    }

    private static ConsumerRecord<String, String> consumeOne(String topic, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("spring.embedded.kafka.brokers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-dlt-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("no record found on " + topic + " within " + timeout);
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
