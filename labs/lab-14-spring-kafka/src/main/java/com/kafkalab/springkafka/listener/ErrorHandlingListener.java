package com.kafkalab.springkafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Every record whose key is {@code "poison"} always throws --
 * {@code errorHandlingContainerFactory}'s {@code DefaultErrorHandler}
 * (configured with a {@code FixedBackOff(200L, 2L)}: 2 retries after
 * the first attempt, so 3 real attempts total) retries it in-process,
 * then hands it to a {@code DeadLetterPublishingRecoverer}. The native
 * mechanism this wraps is WP-13's own hand-rolled
 * {@code RetryingRecordProcessor} -- same shape (bounded retry, then a
 * DLQ), this time entirely framework-provided.
 */
@Component
public class ErrorHandlingListener {

    private final Map<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    @KafkaListener(topics = "error-handling-in", containerFactory = "errorHandlingContainerFactory", groupId = "lab14-error-handling-group")
    public void onMessage(ConsumerRecord<String, String> record) {
        attemptCounts.computeIfAbsent(record.key(), k -> new AtomicInteger()).incrementAndGet();
        if ("poison".equals(record.key())) {
            throw new IllegalStateException("simulated permanent failure for " + record.key());
        }
    }

    public int attemptsFor(String key) {
        AtomicInteger counter = attemptCounts.get(key);
        return counter == null ? 0 : counter.get();
    }
}
