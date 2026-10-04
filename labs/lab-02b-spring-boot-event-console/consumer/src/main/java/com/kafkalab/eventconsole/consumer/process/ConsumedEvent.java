package com.kafkalab.eventconsole.consumer.process;

/**
 * What the processing step is given: the record's key and value, and the contract the producer
 * says it followed.
 *
 * @param schemaId the {@code x-schema-id} Kafka header exactly as received (a Schema Registry
 *                 schema id), or null if the record carried none
 */
public record ConsumedEvent(String key, String value, String schemaId) {
}
