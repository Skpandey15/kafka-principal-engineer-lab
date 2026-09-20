package com.kafkalab.controllerquorum.support;

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
        return System.getProperty("bootstrapServers", "localhost:9096,localhost:9097,localhost:9098");
    }

    public static String topic() {
        return System.getProperty("topic", "quorum-orders");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }

    public static long getLong(String key, long defaultValue) {
        String value = get(key, "");
        return value.isBlank() ? defaultValue : Long.parseLong(value);
    }

    public static int getInt(String key, int defaultValue) {
        String value = get(key, "");
        return value.isBlank() ? defaultValue : Integer.parseInt(value);
    }
}
