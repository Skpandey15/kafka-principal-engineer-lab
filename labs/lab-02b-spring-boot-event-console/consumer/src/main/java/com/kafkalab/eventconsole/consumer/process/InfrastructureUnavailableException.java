package com.kafkalab.eventconsole.consumer.process;

/**
 * Something the processing step depends on (the Schema Registry) is unreachable or broken. That
 * says nothing about the event, so it must NOT turn into a FAILED event or use up one of its
 * retries: the consumer lets it propagate and waits (like a MongoDB outage), and the retry worker
 * leaves the event alone until the lease expires.
 */
public class InfrastructureUnavailableException extends RuntimeException {

    public InfrastructureUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public InfrastructureUnavailableException(String message) {
        super(message);
    }
}
