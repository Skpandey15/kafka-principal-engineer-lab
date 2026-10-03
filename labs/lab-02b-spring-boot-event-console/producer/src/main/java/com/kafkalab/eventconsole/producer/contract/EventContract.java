package com.kafkalab.eventconsole.producer.contract;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.contract.ContractViolationException.Violation;

/**
 * Holds the current contract and checks events against it BEFORE they reach Kafka -- the
 * cheapest place to stop a bad event, and the only place the sender can still be told about it.
 *
 * <p>The latest version is refreshed from the registry at most once per TTL. If the registry is
 * down when a refresh is due, the last known version keeps being enforced (the contract does not
 * silently switch off); only a producer that has NEVER obtained a contract refuses to publish.
 *
 * <p>Remote {@code $ref}s are never fetched (the library's default loader reads only embedded
 * schemas): the registry is the single source of truth.
 */
@Component
public class EventContract {

    private static final Logger log = LoggerFactory.getLogger(EventContract.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** The caller needs enough to fix the request, not a flood. */
    private static final int MAX_REPORTED_EVENTS = 20;
    private static final int MAX_VIOLATIONS_PER_EVENT = 3;
    private static final long RETRY_AFTER_FAILURE_MS = 5_000;

    /** A compiled version of the contract. */
    public record Current(int id, int version, Schema schema) {
    }

    private final SchemaRegistryClient registry;
    private final AppProperties.Schema config;
    private final Clock clock;
    private final SchemaRegistry compiler = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_7);

    private volatile Current current;
    private volatile long refreshedAtMs;

    public EventContract(SchemaRegistryClient registry, AppProperties props, Clock clock) {
        this.registry = registry;
        this.config = props.schema();
        this.clock = clock;
    }

    /**
     * @throws ContractUnavailableException no version is known and the registry cannot provide one
     */
    public synchronized Current current() {
        long now = clock.millis();
        if (current != null && now - refreshedAtMs < config.cacheTtlMs()) {
            return current;
        }
        try {
            SchemaRegistryClient.Registered latest = registry.latest();
            if (current == null || current.id() != latest.id()) {
                current = new Current(latest.id(), latest.version(), compile(latest));
                log.info("Enforcing contract '{}' version {} (schema id {})", config.subject(), latest.version(), latest.id());
            }
            refreshedAtMs = now;
        } catch (ContractUnavailableException e) {
            if (current == null) {
                throw e;
            }
            // Keep enforcing what we know. Ask again soon, but not on every request: each failed call can
            // cost a connect timeout, and publishing must not slow to a crawl during a registry outage.
            refreshedAtMs = now - config.cacheTtlMs() + RETRY_AFTER_FAILURE_MS;
            log.warn("Could not refresh the contract, continuing with version {} (schema id {}): {}", current.version(),
                    current.id(), e.getMessage());
        }
        return current;
    }

    /** The version in force, or null if none is known and the registry is unreachable. For display only. */
    public Current currentOrNull() {
        try {
            return current();
        } catch (ContractUnavailableException e) {
            return null;
        }
    }

    /**
     * Checks every event; nothing is partially accepted.
     *
     * @param values the event values, in request order; {@code keys} are for the error report only
     * @throws ContractViolationException if any event breaks the contract
     */
    public void requireAllValid(Current contract, List<String> keys, List<String> values) {
        List<Violation> found = new ArrayList<>();
        int total = 0;
        for (int i = 0; i < values.size(); i++) {
            String reason = violationOf(contract, values.get(i));
            if (reason != null) {
                total++;
                if (found.size() < MAX_REPORTED_EVENTS) {
                    found.add(new Violation(i, keys.get(i), reason));
                }
            }
        }
        if (total > 0) {
            throw new ContractViolationException(contract.id(), found, total);
        }
    }

    /** Why a value breaks the contract, or null if it does not. */
    static String violationOf(Current contract, String value) {
        if (value == null || value.isBlank()) {
            return "value is empty";
        }
        JsonNode node;
        try {
            node = JSON.readTree(value);
        } catch (JacksonException e) {
            return "value is not valid JSON: " + e.getOriginalMessage();
        }
        List<Error> errors = contract.schema().validate(node);
        if (errors.isEmpty()) {
            return null;
        }
        String shown = errors.stream().limit(MAX_VIOLATIONS_PER_EVENT)
                .map(e -> where(e) + ": " + e.getMessage()).collect(Collectors.joining("; "));
        int more = errors.size() - MAX_VIOLATIONS_PER_EVENT;
        return shown + (more > 0 ? " (+" + more + " more)" : "");
    }

    private Schema compile(SchemaRegistryClient.Registered latest) {
        try {
            return compiler.getSchema(latest.schema());
        } catch (RuntimeException e) {
            throw new ContractUnavailableException("the registered schema (id " + latest.id() + ") cannot be compiled: "
                    + e.getMessage(), e);
        }
    }

    /** Where in the event a violation is: a JSON pointer such as /seq, or a name for the event as a whole. */
    private static String where(Error error) {
        String location = String.valueOf(error.getInstanceLocation());
        return location.isEmpty() || "$".equals(location) ? "(the event)" : location;
    }
}
