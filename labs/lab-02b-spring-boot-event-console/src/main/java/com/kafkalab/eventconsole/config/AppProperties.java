package com.kafkalab.eventconsole.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/** Every tunable the app has, validated at startup so a bad value fails fast instead of at 3am. */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @NotBlank String topic,
        @Min(1) int topicPartitions,
        @Min(1) int topicReplicas,
        @Min(1) int topicMinInsyncReplicas,
        @Min(1) int maxBulkEvents,
        @Min(0) int consumerMaxRetries,
        @Min(1) long consumerBackoffInitialMs,
        @Min(1) long consumerBackoffMaxMs) {

    /** Records that exhaust their retries are published here, never silently dropped. */
    public String deadLetterTopic() {
        return topic + ".DLT";
    }
}
