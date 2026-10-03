package com.kafkalab.eventconsole.consumer.contract;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException;

/**
 * Checks an event's value against the schema it declared. Schemas are fetched from the registry
 * by id and cached for good: a registry id is immutable, so a cached schema can never be stale --
 * and the consumer keeps validating, without the registry, every event whose schema it has
 * already seen.
 *
 * <p>Remote {@code $ref}s are never fetched (the library's default loader reads only embedded
 * schemas): the registry is the single source of truth, and a schema must not be able to make the
 * consumer call arbitrary URLs.
 */
@Component
public class EventContract {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** The reason is stored on the event and shown in the UI; the first few violations are enough to act on. */
    private static final int MAX_REPORTED_VIOLATIONS = 3;

    private final SchemaRegistryClient registry;
    private final SchemaRegistry compiler = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_7);
    private final ConcurrentMap<Integer, Schema> schemas = new ConcurrentHashMap<>();

    public EventContract(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    /**
     * @throws EventProcessingException           the value is not valid JSON, or violates the schema, or the schema id is unknown
     * @throws InfrastructureUnavailableException the schema could not be obtained or compiled
     */
    public void validate(int schemaId, String value) {
        Schema schema = schemas.get(schemaId);
        if (schema == null) {
            schema = load(schemaId);
            schemas.put(schemaId, schema);
        }

        JsonNode node;
        try {
            node = JSON.readTree(value);
        } catch (JacksonException e) {
            throw new EventProcessingException("value is not valid JSON: " + e.getOriginalMessage());
        }

        List<Error> violations = schema.validate(node);
        if (!violations.isEmpty()) {
            String shown = violations.stream().limit(MAX_REPORTED_VIOLATIONS).map(e -> where(e) + ": " + e.getMessage())
                    .collect(Collectors.joining("; "));
            int more = violations.size() - MAX_REPORTED_VIOLATIONS;
            throw new EventProcessingException("violates schema " + schemaId + ": " + shown
                    + (more > 0 ? " (+" + more + " more)" : ""));
        }
    }

    private Schema load(int schemaId) {
        String text = registry.jsonSchemaById(schemaId);
        try {
            return compiler.getSchema(text);
        } catch (RuntimeException e) {
            // A schema that does not compile affects every event that uses it: that is a problem
            // with the contract, not with any one event, so stall and alert instead of failing them.
            throw new InfrastructureUnavailableException("schema id " + schemaId + " cannot be compiled: " + e.getMessage(), e);
        }
    }

    /** For tests and diagnostics. */
    int cachedSchemas() {
        return schemas.size();
    }

    /** Where in the event a violation is: a JSON pointer such as /seq, or a name for the event as a whole. */
    private static String where(Error error) {
        String location = String.valueOf(error.getInstanceLocation());
        return location.isEmpty() || "$".equals(location) ? "(the event)" : location;
    }
}
