package com.kafkalab.nativeclient.support;

/**
 * Reads the handful of values every experiment in this lab needs, from
 * system properties (set by the Gradle tasks in build.gradle via -P
 * project properties), each with a sensible default. This exists purely to
 * avoid repeating the same three lines of {@code System.getProperty(...)}
 * in every app class below — it is not a configuration framework.
 */
public final class LabConfig {

    private LabConfig() {
    }

    public static String bootstrapServers() {
        return System.getProperty("bootstrapServers", "localhost:9092");
    }

    public static String topic() {
        return System.getProperty("topic", "orders-java");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }
}
