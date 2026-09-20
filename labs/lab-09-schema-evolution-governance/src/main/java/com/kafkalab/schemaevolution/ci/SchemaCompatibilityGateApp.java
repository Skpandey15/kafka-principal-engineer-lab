package com.kafkalab.schemaevolution.ci;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Section 23's mandatory CI compatibility gate. A deterministic, exit-code-
 * driven check -- exactly what a CI pipeline step needs -- with no
 * dependency on any CI platform: it is a plain {@code JavaExec} Gradle
 * task ({@code checkAvroCompatibility}) any CI system can invoke as
 * {@code ./gradlew checkAvroCompatibility -PschemaFile=<candidate> -Psubject=<subject>}.
 *
 * <pre>
 * Pull Request -&gt; schema changed -&gt; ./gradlew checkAvroCompatibility (this class)
 *      -&gt; PASS (exit 0) --&gt; merge allowed
 *      -&gt; FAIL (exit 1) --&gt; PR blocked, real rejection reasons printed
 * </pre>
 *
 * <h2>A real correctness bug this app's FIRST version had, found empirically</h2>
 * An earlier version of this class used
 * {@link SchemaRegistryClient#testCompatibilityVerbose(String, io.confluent.kafka.schemaregistry.ParsedSchema)},
 * the obvious, documented convenience method. Verified directly against
 * the running registry, that method -- and the ad-hoc
 * {@code POST /compatibility/subjects/{subject}/versions/latest} REST
 * endpoint it calls -- ONLY ever compares the candidate against the
 * LATEST registered version, REGARDLESS of whether the subject's
 * configured compatibility mode is a transitive one
 * ({@code BACKWARD_TRANSITIVE}, etc.). A schema that is compatible with
 * v2 but NOT with v1 (Section 17's exact transitive-compatibility trap)
 * was reported as a false PASS by that endpoint, even with
 * {@code BACKWARD_TRANSITIVE} configured -- while the REAL registration
 * endpoint ({@code POST /subjects/{subject}/versions}) correctly rejected
 * the exact same schema with HTTP 409, citing {@code oldSchemaVersion: 1}.
 * A CI gate built on the convenience method alone would have given teams
 * false confidence under a transitive compatibility policy -- a real,
 * consequential finding, not a hypothetical one.
 *
 * <p>This version fixes that by checking the subject's ACTUAL configured
 * compatibility mode first: if it is one of the {@code *_TRANSITIVE}
 * variants, it tests the candidate against EVERY registered version
 * individually (via {@link RestService#testCompatibility(String, String,
 * java.util.List, String, String, boolean)}, the lower-level, version-
 * parameterized call {@code testCompatibilityVerbose} does not expose),
 * exactly mirroring what real registration would do.
 */
public final class SchemaCompatibilityGateApp {

    public static void main(String[] args) throws Exception {
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String subject = LabConfig.get("subject", "avro-orders-value");
        String schemaFilePath = LabConfig.get("schemaFile", "src/main/avro/order-event-v2.avsc");
        String schemaJson = Files.readString(new File(schemaFilePath).toPath());

        System.out.printf("Schema compatibility gate | subject=%s | candidate=%s%n", subject, schemaFilePath);

        try (SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)) {
            if (!client.getAllSubjects().contains(subject)) {
                System.out.printf("PASS -- subject '%s' does not exist yet; the first version of a subject has nothing to be incompatible with.%n", subject);
                System.exit(0);
            }

            String compatibility = effectiveCompatibility(client, subject);
            RestService restService = new RestService(schemaRegistryUrl);

            List<Integer> versionsToCheck = compatibility.contains("TRANSITIVE")
                    ? client.getAllVersions(subject)
                    : List.of(client.getLatestSchemaMetadata(subject).getVersion());
            System.out.printf("Effective compatibility: %s -> checking against version(s) %s%n", compatibility, versionsToCheck);

            List<String> failures = new ArrayList<>();
            for (int version : versionsToCheck) {
                List<String> messages = restService.testCompatibility(
                        schemaJson, "AVRO", Collections.emptyList(), subject, String.valueOf(version), true);
                if (!messages.isEmpty()) {
                    failures.add("against version " + version + ":");
                    for (String message : messages) {
                        failures.add("  " + message);
                    }
                }
            }

            if (failures.isEmpty()) {
                System.out.println("PASS -- compatible with subject '" + subject + "' under " + compatibility + ".");
                System.exit(0);
            } else {
                System.out.println("FAIL -- incompatible with subject '" + subject + "' under " + compatibility + ". Real registry rejection reasons:");
                failures.forEach(System.out::println);
                System.exit(1);
            }
        }
    }

    /**
     * Most subjects never have their OWN compatibility override -- they
     * inherit the registry's global default -- and asking the registry for
     * a subject-level config that was never set returns a real 404
     * ({@code SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED}), not a fallback
     * value. Discovered empirically against the running registry.
     */
    private static String effectiveCompatibility(SchemaRegistryClient client, String subject) throws Exception {
        try {
            return client.getCompatibility(subject);
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                return client.getCompatibility(null);
            }
            throw e;
        }
    }
}
