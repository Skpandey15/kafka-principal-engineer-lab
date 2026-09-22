package com.kafkalab.connectcdc.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * A real, single-node combined broker+controller Kafka cluster, a real
 * PostgreSQL container configured for logical replication, and a real
 * Kafka Connect distributed-mode worker (both the {@code connect-file}
 * plugin and the Debezium PostgreSQL connector plugin), all on one
 * Testcontainers {@link Network} -- mechanically the same three-service
 * shape as this lab's manual environment
 * ({@code platform/kafka-cluster/} + {@code platform/kafka-connect/}),
 * single-node for test speed, exactly like every prior lab's own
 * Testcontainers helper explains for its own single-node test cluster
 * (see {@code SchemaRegistryKafkaCluster} in lab-09).
 *
 * <p><b>Reuses the ALREADY-FETCHED plugins from
 * {@code platform/kafka-connect/plugins/}</b> rather than re-downloading
 * them per test run -- run {@code platform/kafka-connect/fetch-plugins.sh}
 * once before running this lab's tests (the same prerequisite the
 * manual environment has); {@link #start()} fails fast with a clear
 * message if that directory is missing, rather than silently hanging.
 */
public final class KafkaConnectCluster implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String POSTGRES_IMAGE = "postgres:17.6";
    private static final String CLUSTER_ID = "K11nnLzR5RS-Yw1Qm9Nf3Cd";
    private static final String KAFKA_ALIAS = "it-kafka";
    private static final String POSTGRES_ALIAS = "it-postgres";
    private static final String CONNECT_ALIAS = "it-connect-worker";

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> kafka;
    // NOT generic in Testcontainers 2.x -- a real API change from the
    // 1.x line, where PostgreSQLContainer<SELF> used the library's old
    // self-type builder pattern. Testcontainers 2.0 dropped that pattern
    // across the library (confirmed by this exact compile error before
    // being fixed), so this is `PostgreSQLContainer`, not
    // `PostgreSQLContainer<?>`.
    private final PostgreSQLContainer postgres;
    private final GenericContainer<?> connectWorker;
    private final int kafkaHostPort;

    public KafkaConnectCluster(int kafkaHostPort) {
        this.kafkaHostPort = kafkaHostPort;

        Path pluginsDir = Path.of("..", "..", "platform", "kafka-connect", "plugins").normalize();
        if (!Files.isDirectory(pluginsDir.resolve("debezium-connector-postgres"))
                || !Files.isDirectory(pluginsDir.resolve("connect-file"))) {
            throw new IllegalStateException(
                    "platform/kafka-connect/plugins/ is missing debezium-connector-postgres/ or connect-file/ -- "
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
                // max_wal_senders/max_replication_slots raised well above
                // the manual environment's 4 -- a real finding from this
                // exact test suite: each Debezium connector this suite
                // registers holds ONE open WAL sender connection for as
                // long as it stays registered, and this test class never
                // deletes a connector between tests (deliberately, so
                // each test's own real evidence stays inspectable) -- by
                // the 5th connector, `max_wal_senders=4` (matching the
                // manual environment) was real, reproducibly exhausted:
                // "FATAL: number of requested standby connections
                // exceeds \"max_wal_senders\" (currently 4)".
                .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=20", "-c", "max_replication_slots=20")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(Path.of("..", "..", "platform", "kafka-connect", "init-postgres.sql").normalize()),
                        "/docker-entrypoint-initdb.d/init-postgres.sql");

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

    /** Starts Kafka first (Connect and Postgres both depend on it/are independent of it, but Connect needs Kafka reachable to form its cluster). */
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

    /** Runs a command inside the Connect worker container (e.g. to write a source file the FileStreamSourceConnector reads) and returns its stdout. */
    public String execInConnectWorker(String... command) throws Exception {
        org.testcontainers.containers.Container.ExecResult result = connectWorker.execInContainer(command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Command " + java.util.Arrays.toString(command)
                    + " exited " + result.getExitCode() + ": " + result.getStderr());
        }
        return result.getStdout();
    }

    @Override
    public void close() {
        connectWorker.stop();
        postgres.stop();
        kafka.stop();
        network.close();
    }
}
