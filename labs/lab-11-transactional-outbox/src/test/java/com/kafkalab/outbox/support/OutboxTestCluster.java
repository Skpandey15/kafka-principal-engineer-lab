package com.kafkalab.outbox.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * A real, single-node combined broker+controller Kafka cluster, a real
 * PostgreSQL container (outbox schema only -- see
 * {@code platform/kafka-connect/init-postgres-outbox.sql}), and a real
 * Kafka Connect distributed-mode worker running the Debezium PostgreSQL
 * connector plugin -- the SAME three-service shape as WP-11's own
 * {@code KafkaConnectCluster}, adapted for this WP's outbox-only schema.
 *
 * <p>Reuses the ALREADY-FETCHED Debezium plugin from
 * {@code platform/kafka-connect/plugins/} (the outbox Event Router
 * transform ships inside that same plugin bundle -- confirmed via
 * {@code unzip -l} against debezium-connect-plugins-3.6.3.Final.jar,
 * see the lab README) -- run
 * {@code platform/kafka-connect/fetch-plugins.sh} once before running
 * this lab's tests, same prerequisite WP-11 already has.
 */
public final class OutboxTestCluster implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String POSTGRES_IMAGE = "postgres:17.6";
    private static final String CLUSTER_ID = "K12ooMaS6TT-Zx2Rn0Og4De";
    private static final String KAFKA_ALIAS = "it-kafka";
    private static final String POSTGRES_ALIAS = "it-postgres";
    private static final String CONNECT_ALIAS = "it-connect-worker";

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> kafka;
    private final PostgreSQLContainer postgres;
    private final GenericContainer<?> connectWorker;
    private final int kafkaHostPort;

    public OutboxTestCluster(int kafkaHostPort) {
        this.kafkaHostPort = kafkaHostPort;

        Path pluginsDir = Path.of("..", "..", "platform", "kafka-connect", "plugins").normalize();
        if (!Files.isDirectory(pluginsDir.resolve("debezium-connector-postgres"))) {
            throw new IllegalStateException(
                    "platform/kafka-connect/plugins/ is missing debezium-connector-postgres/ -- "
                            + "run `bash platform/kafka-connect/fetch-plugins.sh` once before running this lab's tests. Looked at: "
                            + pluginsDir.toAbsolutePath());
        }

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
                .withNetworkAliases(POSTGRES_ALIAS)
                .withDatabaseName("inventory")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=20", "-c", "max_replication_slots=20")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(Path.of("..", "..", "platform", "kafka-connect", "init-postgres-outbox.sql").normalize()),
                        "/docker-entrypoint-initdb.d/init-postgres-outbox.sql");

        this.connectWorker = new GenericContainer<>(KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(CONNECT_ALIAS)
                .withExposedPorts(8083)
                .withCopyFileToContainer(MountableFile.forClasspathResource("connect-distributed-it.properties"),
                        "/opt/kafka/config/connect-distributed.properties")
                .withFileSystemBind(pluginsDir.toAbsolutePath().toString(), "/opt/kafka/connect-plugins", org.testcontainers.containers.BindMode.READ_ONLY)
                .withCommand("/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/connect-distributed.properties")
                .waitingFor(Wait.forLogMessage(".*Kafka Connect started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    public void start() {
        kafka.start();
        postgres.start();
        connectWorker.start();
    }

    public String bootstrapServers() {
        return "localhost:" + kafkaHostPort;
    }

    public String connectUrl() {
        return "http://localhost:" + connectWorker.getMappedPort(8083);
    }

    public String jdbcUrl() {
        return postgres.getJdbcUrl();
    }

    @Override
    public void close() {
        connectWorker.stop();
        postgres.stop();
        kafka.stop();
        network.close();
    }
}
