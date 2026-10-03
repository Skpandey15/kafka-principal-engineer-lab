package com.kafkalab.eventconsole.producer.publish;

import java.util.List;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Either GENERATE (the server builds {@code count} events from a key strategy) or
 * PASTE (the caller supplies the exact events in {@code events}).
 *
 * <p>There is deliberately no {@code topic} field: callers can only publish to the
 * topic this service is configured for. A free-form topic name on a public endpoint
 * would let any caller write into any topic on the cluster, including internal ones.
 */
public record BulkPublishRequest(
        @NotNull Mode mode,
        @Min(1) @Max(50000) Integer count,
        KeyStrategy keyStrategy,
        @Min(1) @Max(10000) Integer keyCount,
        @Size(max = 100) @Pattern(regexp = SAFE, message = "may only contain letters, digits and . _ : -") String keyPrefix,
        @Size(max = 100) @Pattern(regexp = SAFE, message = "may only contain letters, digits and . _ : -") String fixedKey,
        @Size(max = 100) @Pattern(regexp = SAFE, message = "may only contain letters, digits and . _ : -") String valuePrefix,
        @Size(max = 10000) List<@Valid PastedEvent> events) {

    /** Characters safe to embed in a key and in the generated JSON value without escaping. */
    static final String SAFE = "[A-Za-z0-9._:-]*";

    public enum Mode {
        GENERATE, PASTE
    }

    /**
     * UNIQUE: a different key for every event (best spread across partitions).
     * CYCLE: keyCount distinct keys, repeated round-robin (shows per-key partition affinity).
     * FIXED: one key for all events (a deliberate hot partition).
     * NONE: no key (sticky-batch placement, no per-key ordering).
     */
    public enum KeyStrategy {
        UNIQUE, CYCLE, FIXED, NONE
    }

    public record PastedEvent(@Size(max = 100) String key, @NotBlank @Size(max = 10000) String value) {
    }
}
