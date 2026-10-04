package com.kafkalab.eventconsole.producer.contract;

/**
 * The contract cannot be obtained (registry unreachable, subject missing, schema unusable) and
 * there is no previously known version to fall back on. Publishing without checking would defeat
 * the contract, so the request is refused (HTTP 503) -- it is not the caller's fault and can be retried.
 */
public class ContractUnavailableException extends RuntimeException {

    public ContractUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ContractUnavailableException(String message) {
        super(message);
    }
}
