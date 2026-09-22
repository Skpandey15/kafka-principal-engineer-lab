package com.kafkalab.performance.support;

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

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }

    public static int getInt(String key, int defaultValue) {
        String value = get(key, "");
        return value.isBlank() ? defaultValue : Integer.parseInt(value);
    }
}
