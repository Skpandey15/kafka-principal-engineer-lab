package com.kafkalab.springkafka;

import com.kafkalab.springkafka.listener.RetryableTopicListener;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Non-blocking retry topics: unlike WP-13's {@code RetryingRecordProcessor}
 * (which blocks the SAME partition's poll loop with {@code Thread.sleep}
 * between in-process retry attempts), {@code @RetryableTopic} makes
 * Spring Kafka create SEPARATE real topics for each retry attempt, each
 * with its own dedicated consumer -- this listener always fails, so it
 * traverses every one of them before landing in {@code @DltHandler}.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "retryable-in")
class RetryableTopicListenerTest {

    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private RetryableTopicListener listener;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void anAlwaysFailingRecordTraversesEveryRetryTopicBeforeReachingTheDltHandler() throws Exception {
        kafkaTemplate.send(new ProducerRecord<>("retryable-in", "k1", "always-fails-payload")).get();

        // attempts = "3" -- the original attempt on "retryable-in"
        // plus 2 more on real, separate retry topics
        // ("retryable-in-retry-0", "retryable-in-retry-1"), each with
        // its own dedicated consumer, before the @DltHandler method
        // finally receives it.
        waitUntil(() -> listener.attemptedValues().size() == 3, Duration.ofSeconds(30));
        waitUntil(() -> listener.dltPayload() != null, Duration.ofSeconds(30));

        assertEquals("always-fails-payload", listener.dltPayload(),
                "the @DltHandler must still receive the original payload after traversing every retry topic");
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
