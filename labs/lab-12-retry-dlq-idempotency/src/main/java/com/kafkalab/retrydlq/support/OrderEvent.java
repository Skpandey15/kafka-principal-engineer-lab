package com.kafkalab.retrydlq.support;

/**
 * The same tiny hand-rolled wire format WP-06's lab-05 uses
 * ({@code eventId=ORDER-1007|customerId=CUSTOMER-104|amount=42.50}) --
 * copied rather than shared, per this lab's README ("Why a separate
 * project"). {@code eventId} is this lab's idempotency key, same role
 * it played in WP-06's {@code ProcessedEventStore} experiment.
 */
public record OrderEvent(String eventId, String customerId, double amount) {

    public String toWireFormat() {
        return "eventId=" + eventId + "|customerId=" + customerId + "|amount=" + amount;
    }

    /** Throws IllegalArgumentException for malformed input -- this lab's "poison message" case. */
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
