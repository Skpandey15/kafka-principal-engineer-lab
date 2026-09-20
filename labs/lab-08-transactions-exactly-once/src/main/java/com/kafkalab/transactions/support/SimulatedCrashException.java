package com.kafkalab.transactions.support;

/**
 * Thrown at a deliberately chosen, configured {@link FailurePoint} to
 * simulate this process dying at that exact moment -- never caught and
 * handled gracefully by the loop that throws it. Same mechanism and
 * rationale as lab-05's {@code SimulatedCrashException} (WP-06): a real OS
 * process kill is not reliably deliverable from this repository's Windows
 * automation shell, and more importantly a real kill cannot be aimed at an
 * exact line of application logic -- this can. The standalone CLI app
 * catches it only at the very top level and exits via {@code System.exit(1)}
 * without closing the producer/consumer cleanly; integration tests catch it
 * directly and construct fresh clients to simulate the same restart
 * deterministically, in-process.
 */
public final class SimulatedCrashException extends RuntimeException {

    private final FailurePoint failurePoint;

    public SimulatedCrashException(FailurePoint failurePoint, String detail) {
        super("Simulated crash at " + failurePoint + " (" + detail + ")");
        this.failurePoint = failurePoint;
    }

    public FailurePoint failurePoint() {
        return failurePoint;
    }
}
