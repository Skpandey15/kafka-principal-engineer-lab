package com.kafkalab.observability.support;

/**
 * Reads the handful of values this lab's experiments need from system
 * properties (set by the Gradle tasks in build.gradle via -P project
 * properties), each with a sensible default. Same small helper pattern
 * every prior lab uses -- copied rather than shared.
 */
public final class LabConfig {

    private LabConfig() {
    }

    public static String bootstrapServers() {
        return System.getProperty("bootstrapServers", "localhost:9093,localhost:9094,localhost:9095");
    }

    public static String prometheusUrl() {
        return System.getProperty("prometheusUrl", "http://localhost:9090");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }
}
