package com.kafkalab.retrydlq.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;

/**
 * A real, single-node combined broker+controller Kafka cluster plus a
 * real PostgreSQL container running ONLY this WP's
 * {@code processed_events} table -- no Kafka Connect worker, no
 * Debezium, unlike WP-11/WP-12's own Testcontainers helpers, because
 * this WP needs none of that: idempotency here is a plain JDBC table
 * with a unique constraint, read and written directly by the consumer
 * itself.
 */
public final class RetryDlqTestCluster implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String POSTGRES_IMAGE = "postgres:17.6";
    private static final String CLUSTER_ID = "K13ppNbT7UU-Ay3So1Ph5Ef";
    private static final String KAFKA_ALIAS = "it-kafka";

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> kafka;
    private final PostgreSQLContainer postgres;
    private final int kafkaHostPort;

    public RetryDlqTestCluster(int kafkaHostPort) {
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

        this.postgres = new PostgreSQLContainer(POSTGRES_IMAGE)
                .withNetwork(network)
                .withDatabaseName("inventory")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(Path.of("..", "..", "platform", "kafka-connect", "init-postgres-idempotency.sql").normalize()),
                        "/docker-entrypoint-initdb.d/init-postgres-idempotency.sql");
    }

    public void start() {
        kafka.start();
        postgres.start();
    }

    public String bootstrapServers() {
        return "localhost:" + kafkaHostPort;
    }

    public String jdbcUrl() {
        return postgres.getJdbcUrl();
    }

    @Override
    public void close() {
        postgres.stop();
        kafka.stop();
        network.close();
    }
}
