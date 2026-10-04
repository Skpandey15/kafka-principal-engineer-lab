package com.kafkalab.eventconsole.producer.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

/**
 * Loads the producer's real application*.yml for each profile and checks that they bind to the
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
    void localIsTheDefaultProfileAndTargetsLocalhostWithTheProducersOwnDatabase() {
        runner.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            Environment env = ctx.getEnvironment();
            assertThat(env.getActiveProfiles()).isEmpty();
            assertThat(env.getDefaultProfiles()).contains("local");
            assertThat(env.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("localhost:9092");
            assertThat(env.getProperty("spring.mongodb.uri")).isEqualTo("mongodb://localhost:27017/eventconsole_producer");
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
    void k3dUsesTheInClusterKafkaListenerAndRequiresItsOwnMongoCredentials() {
        runner.withPropertyValues("spring.profiles.active=k3d", "MONGODB_URI=mongodb://producer:pw@mongo:27017/eventconsole_producer")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getEnvironment().getProperty("spring.kafka.bootstrap-servers"))
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
                "MONGODB_URI=mongodb://u:p@docdb.example:27017/eventconsole_producer?tls=true&retryWrites=false")
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
    void startupValidationNamesExactlyWhatIsMissing() {
        runner.withPropertyValues("spring.profiles.active=aws", "KAFKA_BOOTSTRAP=b:9096").run(ctx ->
                assertThatThrownBy(() -> StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("aws")
                        .hasMessageContaining("MONGODB_URI")
                        .hasMessageContaining("KAFKA_SASL_JAAS_CONFIG")
                        .hasMessageContaining("SCHEMA_REGISTRY_URL")
                        // already provided, so it must NOT be reported as missing
                        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("KAFKA_BOOTSTRAP")));

        runner.withPropertyValues("spring.profiles.active=k3d").run(ctx ->
                assertThatThrownBy(() -> StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()))
                        .hasMessageContaining("MONGODB_URI"));
    }

    @Test
    void eachProfileSaysWhereTheEventContractLives() {
        runner.run(ctx -> assertThat(ctx.getBean(AppProperties.class).schema().registryUrl()).isEqualTo("http://localhost:8081"));
        runner.withPropertyValues("spring.profiles.active=k3d", "MONGODB_URI=mongodb://x/y").run(ctx ->
                assertThat(ctx.getBean(AppProperties.class).schema().registryUrl())
                        .isEqualTo("http://schema-registry.event-console.svc.cluster.local:8081"));
        runner.withPropertyValues("spring.profiles.active=aws", "SCHEMA_REGISTRY_URL=https://registry.example.com", "MONGODB_URI=mongodb://x/y",
                "KAFKA_BOOTSTRAP=b:1", "KAFKA_SASL_JAAS_CONFIG=x").run(ctx ->
                assertThat(ctx.getBean(AppProperties.class).schema().registryUrl()).isEqualTo("https://registry.example.com"));
    }

    @Test
    void everyPublishIsAuditedBeforeItIsSentByDefaultAndTheStrictnessCanBeRelaxedExplicitly() {
        runner.run(ctx -> {
            assertThat(ctx.getBean(AppProperties.class).audit().required()).isTrue();
            assertThat(ctx.getBean(AppProperties.class).audit().interruptedAfter()).isEqualTo(java.time.Duration.ofMinutes(10));
        });
        runner.withPropertyValues("APP_AUDIT_REQUIRED=false", "APP_AUDIT_INTERRUPTED_AFTER=2m").run(ctx -> {
            assertThat(ctx.getBean(AppProperties.class).audit().required()).isFalse();
            assertThat(ctx.getBean(AppProperties.class).audit().interruptedAfter()).isEqualTo(java.time.Duration.ofMinutes(2));
        });
    }

    @Test
    void startupValidationPassesWhenEverythingIsProvidedAndForProfilesWithNoRequirements() {
        runner.withPropertyValues("spring.profiles.active=k3d", "MONGODB_URI=mongodb://u:p@mongo/db").run(ctx ->
                StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()));
        // local has defaults for everything: nothing required.
        runner.run(ctx -> StartupConfigValidator.validate((ConfigurableEnvironment) ctx.getEnvironment()));
    }

    @Test
    void dottedKafkaClientKeysInYamlBindAsExactKafkaPropertyNames() {
        // YAML nests `producer.properties` -> `delivery.timeout.ms`; Spring must hand the Kafka
        // client the literal key "delivery.timeout.ms", not a nested structure.
        runner.run(ctx -> {
            var producer = Binder.get(ctx.getEnvironment())
                    .bind("spring.kafka.producer.properties", Bindable.mapOf(String.class, String.class)).get();
            assertThat(producer).containsEntry("delivery.timeout.ms", "30000")
                    .containsEntry("request.timeout.ms", "15000")
                    .containsEntry("linger.ms", "10")
                    .containsEntry("max.block.ms", "15000")
                    .containsEntry("enable.idempotence", "true");
        });

        runner.withPropertyValues("spring.profiles.active=aws", "KAFKA_BOOTSTRAP=b:9096",
                "KAFKA_SASL_JAAS_CONFIG=x", "MONGODB_URI=mongodb://u:p@h/db").run(ctx -> {
                    var kafka = Binder.get(ctx.getEnvironment())
                            .bind("spring.kafka.properties", Bindable.mapOf(String.class, String.class)).get();
                    assertThat(kafka).containsEntry("security.protocol", "SASL_SSL")
                            .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                            .containsEntry("sasl.jaas.config", "x");
                });
    }

    @Test
    void theProducerHasNoConsumerConfigurationAtAll() {
        // Segregation check: nothing consumer-shaped may leak into the producer service.
        runner.run(ctx -> {
            Environment env = ctx.getEnvironment();
            assertThat(env.getProperty("spring.kafka.consumer.group-id")).isNull();
            assertThat(env.getProperty("app.consumer-max-retries")).isNull();
            assertThat(env.getProperty("app.events-ttl")).isNull();
        });
    }
}
