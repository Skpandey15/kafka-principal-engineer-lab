package com.kafkalab.deliverysemantics.support;

/**
 * The record value this lab's topic carries: a tiny, hand-rolled wire
 * format (no JSON library dependency, deliberately -- this lab is about
 * offsets and commits, not serialization) of the form
 * {@code eventId=ORDER-1007|customerId=CUSTOMER-104|amount=42.50}.
 *
 * <p>{@code eventId} is the field the idempotent-consumer experiment
 * (section 4 of the WP-06 spec) keys its {@link ProcessedEventStore}
 * lookups on -- it identifies "this business event," which is a distinct
 * concept from "this Kafka record's offset." The same eventId can
 * legitimately arrive at two different offsets (that is exactly what a
 * replay after an at-least-once crash looks like); offset identifies a
 * position in a partition, eventId identifies a business occurrence.
 */
public record OrderEvent(String eventId, String customerId, double amount) {

    public String toWireFormat() {
        return "eventId=" + eventId + "|customerId=" + customerId + "|amount=" + amount;
    }

    public static OrderEvent parse(String wireFormat) {
        String eventId = null;
        String customerId = null;
        double amount = 0.0;
        for (String field : wireFormat.split("\\|")) {
            int eq = field.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String name = field.substring(0, eq);
            String value = field.substring(eq + 1);
            switch (name) {
                case "eventId" -> eventId = value;
                case "customerId" -> customerId = value;
                case "amount" -> amount = Double.parseDouble(value);
                default -> { }
            }
        }
        if (eventId == null || customerId == null) {
            throw new IllegalArgumentException("Malformed OrderEvent wire format: " + wireFormat);
        }
        return new OrderEvent(eventId, customerId, amount);
    }
}
