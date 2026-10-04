package com.kafkalab.eventconsole.producer.web;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import com.kafkalab.eventconsole.producer.contract.ContractUnavailableException;
import com.kafkalab.eventconsole.producer.contract.ContractViolationException;
import com.kafkalab.eventconsole.producer.publish.AuditUnavailableException;

/**
 * Extends Spring's own handler so framework errors (404, 405, 415, ...) keep their
 * correct status codes. A bare catch-all {@code @ExceptionHandler(Exception.class)}
 * would silently turn every one of them into a 500.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException e,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + " " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", msg));
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException e,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        // Never echo a parser's internal message (it can quote the request body or class names).
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "Malformed request body"));
    }

    /** The request is well-formed but its events break the contract: 422, with exactly what is wrong and where. */
    @ExceptionHandler(ContractViolationException.class)
    ResponseEntity<Map<String, Object>> contractViolation(ContractViolationException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                "error", e.total() + " event(s) break the event contract (schema " + e.schemaId() + "); nothing was published",
                "violations", e.violations(),
                "total", e.total()));
    }

    /** The audit record could not be written first, so nothing was sent: safe to retry, unlike a failure after the send. */
    @ExceptionHandler(AuditUnavailableException.class)
    ResponseEntity<Map<String, String>> auditUnavailable(AuditUnavailableException e) {
        log.warn("Refusing to publish: {}: {}", e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "The audit store is unavailable, so nothing was published. It is safe to retry."));
    }

    /** No contract could be obtained: refuse rather than publish unchecked. Retryable, and not the caller's fault. */
    @ExceptionHandler(ContractUnavailableException.class)
    ResponseEntity<Map<String, String>> contractUnavailable(ContractUnavailableException e) {
        log.warn("Refusing to publish: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "The event contract is unavailable, so nothing was published. Try again shortly."));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", String.valueOf(e.getMessage())));
    }

    /** Last resort: log the real cause server-side, tell the client nothing about internals. */
    @ExceptionHandler(Exception.class)
    ResponseEntity<Map<String, String>> unexpected(Exception e) {
        String reference = UUID.randomUUID().toString().substring(0, 8);
        log.error("Unhandled error, reference {}", reference, e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal error, reference " + reference));
    }
}
