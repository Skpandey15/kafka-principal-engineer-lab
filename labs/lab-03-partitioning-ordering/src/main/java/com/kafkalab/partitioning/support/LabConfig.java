package com.kafkalab.partitioning.support;

/**
 * Reads the handful of values this lab's experiments need from system
 * properties (set by the Gradle tasks in build.gradle via -P project
 * properties), each with a sensible default. Same small helper pattern
 * WP-03's lab-02 used -- copied rather than shared across a multi-module
 * build, per this lab's README ("Why a separate project").
 */
public final class LabConfig {

    private LabConfig() {
    }

    public static String bootstrapServers() {
        return System.getProperty("bootstrapServers", "localhost:9092");
    }

    public static String topic() {
        return System.getProperty("topic", "distribution-demo");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }
}
