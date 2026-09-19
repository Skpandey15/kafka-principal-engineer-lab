package com.kafkalab.consumergroups.support;

/**
 * Reads the handful of values this lab's experiments need from system
 * properties (set by the Gradle tasks in build.gradle via -P project
 * properties), each with a sensible default. Same small helper pattern
 * WP-03 (lab-02) and WP-04 (lab-03) used -- copied rather than shared, per
 * this lab's README ("Why a separate project").
 */
public final class LabConfig {

    private LabConfig() {
    }

    public static String bootstrapServers() {
        return System.getProperty("bootstrapServers", "localhost:9092");
    }

    public static String topic() {
        return System.getProperty("topic", "orders.consumer-group.lab");
    }

    public static String get(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }
}
