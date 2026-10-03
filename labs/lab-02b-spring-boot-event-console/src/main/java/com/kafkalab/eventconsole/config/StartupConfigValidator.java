package com.kafkalab.eventconsole.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Fails startup with a message that names exactly what is missing.
 *
 * <p>The k3d and aws profiles deliberately have NO defaults for endpoints and secrets, so a
 * half-configured deployment cannot quietly connect to localhost. Left alone, though, Spring
 * reports that as a MongoDB driver error ("The connection string is invalid") that never
 * mentions which variable was forgotten. This runs as soon as the environment is prepared,
 * before any bean is created, and says so plainly.
 */
public final class StartupConfigValidator {

    /** Properties each profile requires from its deployment environment. */
    static final Map<String, List<String>> REQUIRED = Map.of(
            "k3d", List.of("spring.mongodb.uri"),
            "aws", List.of("spring.kafka.bootstrap-servers", "spring.kafka.properties.sasl.jaas.config",
                    "spring.mongodb.uri"));

    private StartupConfigValidator() {
    }

    public static void validate(ConfigurableEnvironment env) {
        List<String> problems = new ArrayList<>();
        for (String profile : env.getActiveProfiles()) {
            for (String key : REQUIRED.getOrDefault(profile, List.of())) {
                try {
                    String value = env.getProperty(key);
                    if (value == null || value.isBlank()) {
                        problems.add("'" + key + "' is empty");
                    }
                } catch (IllegalArgumentException unresolved) {
                    // The message names the placeholder, e.g. "Could not resolve placeholder 'MONGODB_URI'".
                    problems.add("'" + key + "': " + unresolved.getMessage());
                }
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Missing required configuration for profile(s) "
                    + String.join(", ", env.getActiveProfiles()) + ":\n  - " + String.join("\n  - ", problems)
                    + "\nSet the environment variables named above (see application-<profile>.yml).");
        }
    }
}
