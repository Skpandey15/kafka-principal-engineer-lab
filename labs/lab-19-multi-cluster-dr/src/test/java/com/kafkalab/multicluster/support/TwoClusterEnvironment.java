package com.kafkalab.multicluster.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TWO real, independent, single-node Kafka clusters ("primary" and
 * "secondary") on one Docker network, plus a real MirrorMaker 2
 * process (the SAME {@code connect-mirror-maker.sh} a production
 * deployment runs, confirmed present in the pinned
 * {@code apache/kafka:4.3.1} image -- not a hand-rolled replication
 * loop) replicating {@code primary -> secondary}.
 */
public final class TwoClusterEnvironment implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String PRIMARY_CLUSTER_ID = "K20vvThZ3aaGd9Yu7Vn1Lk";
    private static final String SECONDARY_CLUSTER_ID = "K20wwUiA4bbHe0Zv8Wo2Ml";
    private static final String PRIMARY_ALIAS = "primary-kafka";
    private static final String SECONDARY_ALIAS = "secondary-kafka";
    private static final String MM2_ALIAS = "mirror-maker";

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> primary;
    private final GenericContainer<?> secondary;
    private final GenericContainer<?> mirrorMaker;
    private final int primaryHostPort;
    private final int secondaryHostPort;

    public TwoClusterEnvironment(int primaryHostPort, int secondaryHostPort) {
        this.primaryHostPort = primaryHostPort;
        this.secondaryHostPort = secondaryHostPort;

        this.primary = singleNodeBroker(PRIMARY_ALIAS, PRIMARY_CLUSTER_ID, primaryHostPort);
        this.secondary = singleNodeBroker(SECONDARY_ALIAS, SECONDARY_CLUSTER_ID, secondaryHostPort);

        String mm2Properties = String.format("""
                clusters = primary, secondary
                primary.bootstrap.servers = %s:19092
                secondary.bootstrap.servers = %s:19092

                primary->secondary.enabled = true
                secondary->primary.enabled = false

                topics = .*
                groups = .*
                sync.topic.acls.enabled = false

                replication.factor = 1
                checkpoints.topic.replication.factor = 1
                heartbeats.topic.replication.factor = 1
                offset-syncs.topic.replication.factor = 1
                offset.storage.replication.factor = 1
                status.storage.replication.factor = 1
                config.storage.replication.factor = 1

                # Fast, deliberately -- lab-speed only (real production
                # MM2 deployments use longer intervals to reduce load).
                sync.topic.configs.interval.seconds = 5
                refresh.topics.interval.seconds = 5
                refresh.groups.interval.seconds = 5
                emit.checkpoints.interval.seconds = 5
                sync.group.offsets.enabled = true
                sync.group.offsets.interval.seconds = 5

                # A real finding building this lab: offset.lag.max (default
                # 100) governs how densely MM2 records the upstream/downstream
                # offset-sync pairs it uses to translate consumer group
                # checkpoints -- it is a RECORD-COUNT threshold, not a time
                # interval, and it is NOT scaled down by the lab-speed second
                # intervals above. At real production volume, sampling every
                # ~100 records is a reasonable precision/overhead tradeoff;
                # at this lab's scale (tens of records), it meant MM2 wrote
                # only ONE offset-sync pair for an entire topic, making
                # checkpoint translation wildly imprecise (confirmed: a
                # primary-side commit at offset 10 translated to downstream
                # offset 1). Lowered here so this lab's DR failover
                # experiment demonstrates ACCURATE translation -- production
                # deployments should size this against their own real
                # throughput, not copy this value verbatim.
                offset.lag.max = 1
                """, PRIMARY_ALIAS, SECONDARY_ALIAS);

        Path mm2PropertiesFile;
        try {
            mm2PropertiesFile = Files.createTempFile("mm2", ".properties");
            Files.writeString(mm2PropertiesFile, mm2Properties, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.mirrorMaker = new GenericContainer<>(KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(MM2_ALIAS)
                .withCopyFileToContainer(MountableFile.forHostPath(mm2PropertiesFile), "/opt/kafka/config/mm2.properties")
                .withCommand("/opt/kafka/bin/connect-mirror-maker.sh", "/opt/kafka/config/mm2.properties")
                .waitingFor(Wait.forLogMessage(".*Herder started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
    }

    private GenericContainer<?> singleNodeBroker(String alias, String clusterId, int hostPort) {
        return new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
                .withFixedExposedPort(hostPort, 9092)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(alias))
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT_HOST://localhost:" + hostPort + ",PLAINTEXT://" + alias + ":19092")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + alias + ":29093")
                .withEnv("CLUSTER_ID", clusterId)
                .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
    }

    public void start() {
        ExecutorService starter = Executors.newFixedThreadPool(2);
        try {
            var primaryFuture = starter.submit(primary::start, (Void) null);
            var secondaryFuture = starter.submit(secondary::start, (Void) null);
            primaryFuture.get();
            secondaryFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start primary/secondary clusters", e);
        } finally {
            starter.shutdown();
        }
        mirrorMaker.start();
    }

    public String primaryBootstrapServers() {
        return "localhost:" + primaryHostPort;
    }

    public String secondaryBootstrapServers() {
        return "localhost:" + secondaryHostPort;
    }

    /** Kills the PRIMARY cluster for real -- models the disaster this WP's DR failover experiment recovers from. */
    public void killPrimary() {
        primary.getDockerClient().killContainerCmd(primary.getContainerId()).exec();
    }

    @Override
    public void close() {
        try {
            mirrorMaker.stop();
        } catch (Exception ignored) {
        }
        try {
            secondary.stop();
        } catch (Exception ignored) {
        }
        try {
            primary.stop();
        } catch (Exception ignored) {
            // Already killed by killPrimary() in the failover test -- a real no-op, not an error.
        }
        network.close();
    }
}
