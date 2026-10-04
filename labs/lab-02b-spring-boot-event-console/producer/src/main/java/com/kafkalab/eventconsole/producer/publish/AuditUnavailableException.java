package com.kafkalab.eventconsole.producer.publish;

/**
 * The audit record could not be written BEFORE publishing, and auditing is required, so nothing was
 * sent. Retryable and not the caller's fault (HTTP 503) -- and, unlike a failure after the send, safe
 * to retry: no event reached Kafka.
 */
public class AuditUnavailableException extends RuntimeException {

    public AuditUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
