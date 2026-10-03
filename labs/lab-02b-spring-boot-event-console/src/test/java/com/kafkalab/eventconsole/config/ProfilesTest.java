package com.kafkalab.eventconsole.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

/**
 * Loads the real application*.properties for each profile and checks that they bind to the
 * validated AppProperties and say what the profile promises. No Docker, no broker, no database:
 * this guards the CONFIGURATION, so a typo in a profile fails the build instead of a deploy.
 */
class ProfilesTest {

    @EnableConfigurationProperties(AppProperties.class)
    static class PropsOnly {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(PropsOnly.class);

    @Test
    void localIsTheDefaultProfileAndTargetsLocalhost() {
        runner.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            Environment env = ctx.getEnvironment();
            assertThat(env.getActiveProfiles()).isEmpty();
            assertThat(env.getDefaultProfiles()).contains("local");
            assertThat(env.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("localhost:9092");
            assertThat(env.getProperty("spring.mongodb.uri")).isEqualTo("mongodb://localhost:27017/eventconsole");
            AppProperties p = ctx.getBean(AppProperties.class);
            assertThat(p.topicReplicas()).isEqualTo(1);
            assertThat(p.topicMinInsyncReplicas()).isEqualTo(1);
        });
    }

    @Test
    void localCanBePointedElsewhereWithoutEditingFiles() {
        runner.withPropertyValues("KAFKA_BOOTSTRAP=other:9999", "MONGODB_URI=mongodb://other:1/db").run(ctx -> {
            assertThat(ctx.getEnvironment().getProperty("spring.kafka.bootstrap-servers")).isEqualTo("other:9999");
            assertThat(ctx.getEnvironment().getProperty("spring.mongodb.uri")).isEqualTo("mongodb://other:1/db");
        });
    }

    @Test
    void k3dUsesTheInClusterKafkaListenerAndRequiresTheMongoSecret() {
        runner.withPropertyValues("spring.profiles.active=k3d", "MONGODB_URI=mongodb://root:pw@mongo:27017/eventconsole")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    Environment env = ctx.getEnvironment();
                    assertThat(env.getProperty("spring.kafka.bootstrap-servers"))
                            .isEqualTo("kafka.kafka.svc.cluster.local:19092");
                    AppProperties p = ctx.getBean(AppProperties.class);
                    assertThat(p.topicReplicas()).isEqualTo(1);
                    assertThat(p.topicPartitions()).isEqualTo(3);
                });

        // No MONGODB_URI injected: must fail rather than quietly connect somewhere wrong.
        runner.withPropertyValues("spring.profiles.active=k3d").run(ctx ->
                assertThatThrownBy(() -> ctx.getEnvironment().getProperty("spring.mongodb.uri"))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("MONGODB_URI"));
    }

    @Test
    void awsIsSecureAndHighlyAvailableAndRequiresItsEndpoints() {
        runner.withPropertyValues("spring.profiles.active=aws",
                "KAFKA_BOOTSTRAP=b-1.msk.example:9096,b-2.msk.example:9096",
                "KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";",
                "MONGODB_URI=mongodb://u:p@docdb.example:27017/eventconsole?tls=true&retryWrites=false")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    Environment env = ctx.getEnvironment();
                    assertThat(env.getProperty("spring.kafka.bootstrap-servers")).startsWith("b-1.msk.example:9096");
                    assertThat(env.getProperty("spring.kafka.properties.security.protocol")).isEqualTo("SASL_SSL");
                    assertThat(env.getProperty("spring.kafka.properties.sasl.mechanism")).isEqualTo("SCRAM-SHA-512");
                    AppProperties p = ctx.getBean(AppProperties.class);
                    assertThat(p.topicReplicas()).isEqualTo(3);
                    assertThat(p.topicMinInsyncReplicas()).isEqualTo(2);
                    // min.insync.replicas must be below the replication factor, or losing one broker blocks all writes.
                    assertThat(p.topicMinInsyncReplicas()).isLessThan(p.topicReplicas());
                });

        // Nothing defaults to localhost in aws: every endpoint and secret is required.
        for (String missing : new String[] { "spring.kafka.bootstrap-servers", "spring.mongodb.uri",
                "spring.kafka.properties.sasl.jaas.config" }) {
            runner.withPropertyValues("spring.profiles.active=aws").run(ctx ->
                    assertThatThrownBy(() -> ctx.getEnvironment().getProperty(missing))
                            .isInstanceOf(IllegalArgumentException.class));
        }
    }

    @Test
    void everyProfileKeepsConsumerConcurrencyEqualToPartitions() {
        for (String profile : new String[] { "local", "k3d", "aws" }) {
            runner.withPropertyValues("spring.profiles.active=" + profile, "MONGODB_URI=mongodb://x/y",
                    "KAFKA_BOOTSTRAP=b:1", "KAFKA_SASL_JAAS_CONFIG=x").run(ctx -> {
                        AppProperties p = ctx.getBean(AppProperties.class);
                        assertThat(ctx.getEnvironment().getProperty("app.consumer-concurrency", Integer.class))
                                .isEqualTo(p.topicPartitions());
                    });
        }
    }

    @Test
    void startupValidationNamesExactlyWhatIsMissing() {
        runner.withPropertyValues("spring.profiles.active=aws", "KAFKA_BOOTSTRAP=b:9096").run(ctx -> {
            assertThatThrownBy(() -> StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("aws")
                    .hasMessageContaining("MONGODB_URI")
                    .hasMessageContaining("KAFKA_SASL_JAAS_CONFIG")
                    // already provided, so it must NOT be reported as missing
                    .satisfies(e -> assertThat(e.getMessage()).doesNotContain("KAFKA_BOOTSTRAP"));
        });

        runner.withPropertyValues("spring.profiles.active=k3d").run(ctx ->
                assertThatThrownBy(() -> StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()))
                        .hasMessageContaining("MONGODB_URI"));
    }

    @Test
    void startupValidationPassesWhenEverythingIsProvidedAndForProfilesWithNoRequirements() {
        runner.withPropertyValues("spring.profiles.active=k3d", "MONGODB_URI=mongodb://u:p@mongo/db").run(ctx ->
                StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()));
        // local has defaults for everything: nothing required.
        runner.run(ctx -> StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()));
    }
}
