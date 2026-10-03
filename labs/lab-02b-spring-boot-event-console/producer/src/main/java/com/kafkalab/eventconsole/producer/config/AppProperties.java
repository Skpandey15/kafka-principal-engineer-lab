package com.kafkalab.eventconsole.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/** Every tunable the producer has, validated at startup so a bad value fails fast instead of at 3am. */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @NotBlank String topic,
        @Min(1) int topicPartitions,
        @Min(1) int topicReplicas,
        @Min(1) int topicMinInsyncReplicas,
        @Min(1) int maxBulkEvents) {
}
