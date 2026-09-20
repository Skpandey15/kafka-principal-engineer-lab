package com.kafkalab.transactions.support;

/**
 * The record value this lab's topics carry: the same tiny, hand-rolled wire
 * format prior labs use (no JSON library dependency, deliberately -- this
 * lab is about transactional semantics, not serialization) of the form
 * {@code eventId=ORDER-1007|customerId=CUSTOMER-104|amount=42.50}. Reused
 * as-is for the orders/payments/audit topics in the multi-partition
 * transaction experiment (Section 10) -- only the topic name and the
 * {@code eventId} prefix change, not the wire shape.
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
