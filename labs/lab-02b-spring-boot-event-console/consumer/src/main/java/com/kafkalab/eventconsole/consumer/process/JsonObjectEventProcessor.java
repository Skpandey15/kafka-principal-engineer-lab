package com.kafkalab.eventconsole.consumer.process;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The event contract this service enforces today: the value must be a JSON object. It is the
 * smallest real validation, and it gives the failure path something real to do -- the producer
 * service lets a user paste arbitrary text, so a non-JSON value is a realistic bad event.
 *
 * <p>A proper contract (Schema Registry + Avro/JSON Schema) would replace this class, not the
 * machinery around it.
 */
@Component
public class JsonObjectEventProcessor implements EventProcessor {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Override
    public void process(String key, String value) {
        if (value == null || value.isBlank()) {
            throw new EventProcessingException("value is empty");
        }
        JsonNode node;
        try {
            node = JSON.readTree(value);
        } catch (JacksonException e) {
            // e.getOriginalMessage() describes the syntax problem without quoting the payload.
            throw new EventProcessingException("value is not valid JSON: " + e.getOriginalMessage());
        }
        if (!node.isObject()) {
            throw new EventProcessingException("value must be a JSON object, not " + node.getNodeType().name().toLowerCase());
        }
    }
}
