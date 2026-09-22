package com.kafkalab.springkafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Non-blocking retry topics: {@code @RetryableTopic} makes Spring Kafka
 * create SEPARATE real topics for each retry attempt (this method
 * always throws, so every attempt traverses one) and a dedicated
 * consumer for each, rather than WP-13's approach of blocking the same
 * partition's poll loop with {@code Thread.sleep} between in-process
 * retries. The trade-off this buys: the main topic's OTHER records keep
 * flowing immediately even while one record is mid-retry, at the cost
 * of strict per-partition ordering across the retry window (a retried
 * record's eventual reprocessing happens on a DIFFERENT topic/partition
 * than its original neighbors).
 */
@Component
public class RetryableTopicListener {

    private final List<String> attemptedValues = new CopyOnWriteArrayList<>();
    private volatile String dltPayload;

    @RetryableTopic(attempts = "3", backOff = @BackOff(delay = 300L), autoCreateTopics = "true")
    @KafkaListener(topics = "retryable-in", groupId = "lab14-retryable-group")
    public void onMessage(ConsumerRecord<String, String> record) {
        attemptedValues.add(record.value());
        throw new IllegalStateException("always fails, to force traversal through every retry topic");
    }

    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        dltPayload = record.value();
    }

    public List<String> attemptedValues() {
        return attemptedValues;
    }

    public String dltPayload() {
        return dltPayload;
    }
}
