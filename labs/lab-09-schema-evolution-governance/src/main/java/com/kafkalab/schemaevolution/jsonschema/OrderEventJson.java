package com.kafkalab.schemaevolution.jsonschema;

/**
 * Section 20's plain POJO. {@code KafkaJsonSchemaSerializer} derives a
 * real JSON Schema from this class's shape via reflection (Jackson's
 * schema-generation module) the first time it serializes one -- this
 * class is intentionally the ONLY schema source; there is no separate
 * hand-written {@code .json} schema file, unlike the Avro (.avsc) and
 * Protobuf (.proto) sides of this lab, to show that JSON Schema
 * governance CAN be derived from code, though a hand-authored schema
 * file is also fully supported by the ecosystem and often preferable for
 * exactly the reasons a hand-authored .avsc is: the schema becomes a
 * reviewable artifact independent of any one language's class shape.
 */
public class OrderEventJson {
    public String orderId;
    public String customerId;
    public String amount;

    public OrderEventJson() {
    }

    public OrderEventJson(String orderId, String customerId, String amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
    }
}
