package com.kafkalab.security.support;

/**
 * Reads the handful of values this lab's experiments need from system
 * properties, each with a sensible default. Same small helper pattern
 * every prior lab uses -- copied rather than shared.
 */
public final class LabConfig {

    private LabConfig() {
    }

    public static String bootstrapServers() {
        return System.getProperty("bootstrapServers", "localhost:9096");
    }

    /** Relative to this lab's own working directory -- the REAL host-side path generate-certs.sh wrote to. */
    public static String truststorePath() {
        return System.getProperty("truststorePath", "../../platform/kafka-security/certs/truststore.p12");
    }

    public static String storePassword() {
        return System.getProperty("storePassword", "lab-security-changeit");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }
}
