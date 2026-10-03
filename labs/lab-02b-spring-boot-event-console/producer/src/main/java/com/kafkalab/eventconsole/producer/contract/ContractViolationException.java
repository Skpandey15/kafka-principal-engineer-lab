package com.kafkalab.eventconsole.producer.contract;

import java.util.List;

/** One or more events in a request break the contract. Nothing from that request was published. */
public class ContractViolationException extends RuntimeException {

    /** @param index position of the event in the request (0-based); {@code reason} says what is wrong with it */
    public record Violation(int index, String key, String reason) {
    }

    private final transient List<Violation> violations;
    private final int total;
    private final int schemaId;

    public ContractViolationException(int schemaId, List<Violation> shown, int total) {
        super(total + " event(s) violate schema " + schemaId);
        this.schemaId = schemaId;
        this.violations = List.copyOf(shown);
        this.total = total;
    }

    /** The first few violations, enough to fix the request. */
    public List<Violation> violations() {
        return violations;
    }

    /** How many events violated the contract in all (may exceed {@link #violations()}). */
    public int total() {
        return total;
    }

    public int schemaId() {
        return schemaId;
    }
}
