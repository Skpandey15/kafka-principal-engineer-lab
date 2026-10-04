package com.kafkalab.eventconsole.consumer.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Every tunable the consumer has, validated at startup so a bad value fails fast instead of at 3am. */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @NotBlank String topic,
        @Min(1) int topicPartitions,
        @Min(1) int topicReplicas,
        @Min(1) int topicMinInsyncReplicas,
        @Min(1) long consumerBackoffInitialMs,
        @Min(1) long consumerBackoffMaxMs,
        @NotNull Duration eventsTtl,
        @Valid @NotNull Retry retry,
        @Valid @NotNull Schema schema) {

    /** Events that exhaust their retries are published here, never silently dropped. */
    public String deadLetterTopic() {
        return topic + ".DLT";
    }

    /**
     * The event contract: where the Schema Registry is. Schemas are read by id; the service never registers any.
     *
     * @param registryUrl      base URL of a Confluent-compatible Schema Registry
     * @param connectTimeoutMs how long to wait to connect before treating the registry as unavailable
     * @param readTimeoutMs    how long to wait for an answer
     * @param legacyUntilOffsets per partition, the offset below which records predate the contract and are
     *                         accepted without a schema header (empty = the contract covers the whole topic;
     *                         see {@code LegacyHistory})
     */
    public record Schema(
            @NotBlank String registryUrl,
            @Min(1) long connectTimeoutMs,
            @Min(1) long readTimeoutMs,
            Map<Integer, Long> legacyUntilOffsets) {

        // The canonical constructor is the one configuration binds through (there are two).
        @ConstructorBinding
        public Schema {
            legacyUntilOffsets = legacyUntilOffsets == null ? Map.of() : Map.copyOf(legacyUntilOffsets);
        }

        /** For callers that have no history to excuse. */
        public Schema(String registryUrl, long connectTimeoutMs, long readTimeoutMs) {
            this(registryUrl, connectTimeoutMs, readTimeoutMs, Map.of());
        }
    }

    /**
     * The retry worker's settings.
     *
     * @param enabled          run the worker in this instance (several instances may; they share the work safely)
     * @param maxRetries       retries after the first failed attempt; the event is DEAD when the last one fails
     * @param backoffInitialMs wait before the first retry; doubles for each further one
     * @param backoffMaxMs     ceiling for that wait
     * @param pollIntervalMs   how often the worker looks for due events
     * @param batchSize        at most this many events per poll, so a big backlog cannot starve the dead-letter sweep
     * @param leaseMs          how long a claimed event stays invisible to other workers; must exceed one processing attempt
     * @param dltSendTimeoutMs how long to wait for the broker to confirm a dead-letter write
     */
    public record Retry(
            boolean enabled,
            @Min(1) int maxRetries,
            @Min(1) long backoffInitialMs,
            @Min(1) long backoffMaxMs,
            @Min(1) long pollIntervalMs,
            @Min(1) int batchSize,
            @Min(1000) long leaseMs,
            @Min(1) long dltSendTimeoutMs) {

        /** Wait before the next retry, given how many retries have already run (0 = first retry): initial, 2x, 4x ... capped. */
        public Duration delayAfter(int retriesDone) {
            int doublings = Math.min(Math.max(retriesDone, 0), 30);
            long delay = backoffInitialMs << doublings;
            if (delay <= 0 || delay > backoffMaxMs) {
                delay = backoffMaxMs;
            }
            return Duration.ofMillis(delay);
        }

        /** True when a retry that just failed was the last one allowed. */
        public boolean isExhausted(int retriesDoneIncludingThisOne) {
            return retriesDoneIncludingThisOne >= maxRetries;
        }
    }
}
