package com.kafkalab.outbox.support;

/**
 * Reads the handful of values this lab's experiments need from system
 * properties (set by the Gradle tasks in build.gradle via -P project
 * properties), each with a sensible default. Same small helper pattern
 * every prior lab uses -- copied rather than shared, per this lab's
 * README ("Why a separate project").
 */
public final class LabConfig {

    private LabConfig() {
    }

    public static String bootstrapServers() {
        return System.getProperty("bootstrapServers", "localhost:9093,localhost:9094,localhost:9095");
    }

    public static String connectUrl() {
        return System.getProperty("connectUrl", "http://localhost:8083");
    }

    public static String jdbcUrl() {
        return System.getProperty("jdbcUrl", "jdbc:postgresql://localhost:5432/inventory");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }
}
