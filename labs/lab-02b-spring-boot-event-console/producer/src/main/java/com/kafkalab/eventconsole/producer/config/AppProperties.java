package com.kafkalab.eventconsole.producer.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Every tunable the producer has, validated at startup so a bad value fails fast instead of at 3am. */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @NotBlank String topic,
        @Min(1) int topicPartitions,
        @Min(1) int topicReplicas,
        @Min(1) int topicMinInsyncReplicas,
        @Min(1) int maxBulkEvents,
        @Valid @NotNull Schema schema,
        @Valid @NotNull Audit audit) {

    // The canonical constructor is the one configuration binds through (there are two).
    @ConstructorBinding
    public AppProperties {
    }

    /** For callers that take the audit defaults. */
    public AppProperties(String topic, int topicPartitions, int topicReplicas, int topicMinInsyncReplicas, int maxBulkEvents,
            Schema schema) {
        this(topic, topicPartitions, topicReplicas, topicMinInsyncReplicas, maxBulkEvents, schema, new Audit(true, Duration.ofMinutes(10)));
    }

    /**
     * The event contract this service publishes under.
     *
     * @param registryUrl      base URL of a Confluent-compatible Schema Registry
     * @param subject          the registry subject holding the contract's versions; the LATEST one is enforced
     * @param connectTimeoutMs how long to wait to connect before treating the registry as unavailable
     * @param readTimeoutMs    how long to wait for an answer
     * @param cacheTtlMs       how long the latest version is reused before asking the registry again; if the
     *                         registry is down at that point the last known version keeps being used
     */
    public record Schema(
            @NotBlank String registryUrl,
            @NotBlank String subject,
            @Min(1) long connectTimeoutMs,
            @Min(1) long readTimeoutMs,
            @Min(1) long cacheTtlMs) {
    }

    /**
     * How strictly a publish is tied to its audit record.
     *
     * @param required         true (the default): the audit row is written BEFORE anything is sent, and if it
     *                         cannot be, nothing is sent (HTTP 503). Every event in Kafka then has a job that
     *                         accounts for it. False trades that for availability: publishing continues when
     *                         the audit store is down, and the job simply has no record.
     * @param interruptedAfter a job still STARTED after this long is labelled INTERRUPTED: its process died
     *                         before recording an outcome. Must comfortably exceed the longest publish (about 60 s).
     */
    public record Audit(boolean required, @NotNull Duration interruptedAfter) {
    }
}
