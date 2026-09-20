package com.kafkalab.schemaevolution.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

/**
 * A real, single-node combined broker+controller KRaft cluster PLUS a real
 * Confluent Schema Registry container, wired together on one Testcontainers
 * {@link Network} -- mechanically the same two-service shape as this lab's
 * manual environment (the reused {@code platform/kafka-cluster/} plus the
 * added {@code platform/schema-registry/}), just single-node for test
 * speed, exactly like every prior lab's own Testcontainers helper explains
 * for its own single-node test cluster (see {@code TransactionsKafkaCluster}
 * in lab-08, whose reasoning applies identically here: none of the
 * schema-evolution correctness properties under test in this lab's
 * automated suite depend on replication factor).
 */
public final class SchemaRegistryKafkaCluster implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.2";
    private static final String CLUSTER_ID = "S3hqLzR5RS-Yw1Qm9Nf3Wg";
    private static final String KAFKA_ALIAS = "test-kafka";
    private static final String SCHEMA_REGISTRY_ALIAS = "test-schema-registry";

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> kafka;
    private final GenericContainer<?> schemaRegistry;
    private final int kafkaHostPort;

    public SchemaRegistryKafkaCluster(int kafkaHostPort) {
        this.kafkaHostPort = kafkaHostPort;
        this.kafka = new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
                .withFixedExposedPort(kafkaHostPort, 9092)
                .withNetwork(network)
                .withNetworkAliases(KAFKA_ALIAS)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(KAFKA_ALIAS))
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT_HOST://localhost:" + kafkaHostPort + ",PLAINTEXT://" + KAFKA_ALIAS + ":19092")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + KAFKA_ALIAS + ":29093")
                .withEnv("CLUSTER_ID", CLUSTER_ID)
                .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        this.schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(SCHEMA_REGISTRY_ALIAS)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", SCHEMA_REGISTRY_ALIAS)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_ALIAS + ":19092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Starts Kafka first and waits for it, THEN starts Schema Registry --
     * unlike the multi-broker KRaft clusters in prior labs, there is no
     * peer-discovery deadlock here (a single Kafka node doesn't wait on
     * anything), and Schema Registry itself needs Kafka already reachable
     * to create its {@code _schemas} topic, so sequential startup is
     * correct here, not a workaround.
     */
    public void start() {
        kafka.start();
        schemaRegistry.start();
    }

    public String bootstrapServers() {
        return "localhost:" + kafkaHostPort;
    }

    public String schemaRegistryUrl() {
        return "http://localhost:" + schemaRegistry.getMappedPort(8081);
    }

    /**
     * Stops ONLY the Schema Registry container, leaving Kafka running --
     * used by the registry-unavailable test (Sections 30-31) to produce a
     * real, deterministic outage of just the registry.
     */
    public void stopSchemaRegistryOnly() {
        schemaRegistry.stop();
    }

    @Override
    public void close() {
        schemaRegistry.stop();
        kafka.stop();
        network.close();
    }
}
