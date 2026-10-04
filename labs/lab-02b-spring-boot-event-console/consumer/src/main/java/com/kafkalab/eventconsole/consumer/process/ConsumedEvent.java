package com.kafkalab.eventconsole.consumer.process;

/**
 * What the processing step is given: the record's key and value, the contract the producer says it
 * followed, and where in the topic the record sits.
 *
 * @param schemaId  the {@code x-schema-id} Kafka header exactly as received (a Schema Registry
 *                  schema id), or null if the record carried none
 * @param partition the record's partition, and
 * @param offset    its offset there: assigned by the broker, so (unlike a header or a timestamp) a
 *                  producer cannot forge it. A header-less record is only excused if it sits in the
 *                  legacy part of the topic, which is decided by this address (see
 *                  {@code LegacyHistory}).
 */
public record ConsumedEvent(String key, String value, String schemaId, int partition, long offset) {
}
