package com.kafkalab.controllerquorum.support;

/**
 * The record value this lab's topic carries: a tiny, hand-rolled wire
 * format (no JSON library dependency, deliberately) of the form
 * {@code eventId=ORDER-1007|customerId=CUSTOMER-104|amount=42.50}.
 */
public record OrderEvent(String eventId, String customerId, double amount) {

    public String toWireFormat() {
        return "eventId=" + eventId + "|customerId=" + customerId + "|amount=" + amount;
    }
}
