package com.kafkalab.failureeng.disk;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Map;

/**
 * A real, single-node Kafka broker whose log directory is mounted on a
 * SIZE-LIMITED tmpfs -- a genuinely small, real filesystem the broker
 * can actually exhaust, not a simulated or mocked "disk full" state.
 * Safe specifically because it's a disposable, in-memory,
 * size-capped mount (confirmed real via Testcontainers'
 * {@code withTmpFs}), never the host's real disk -- exactly the
 * "simulate safely... rather than via literal disk exhaustion"
 * guidance the failure matrix names for this WP.
 */
public final class DiskConstrainedKafkaContainer extends FixedHostPortGenericContainer<DiskConstrainedKafkaContainer> {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String CLUSTER_ID = "K19ttRfX1YY-Eb7Ws5Tl9Ji";
    private static final String ALIAS = "it-kafka-disk";

    public DiskConstrainedKafkaContainer(int hostPort, String tmpfsSizeBytes) {
        super(KAFKA_IMAGE);
        withFixedExposedPort(hostPort, 9092);
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(ALIAS));
        // mode=1777 (world-writable) is real, and necessary -- a real
        // finding building this test: Docker's default tmpfs mount is
        // root-owned, and this image's broker process runs as a
        // non-root user (appuser, uid 1000); without an explicit mode,
        // the broker failed at STARTUP (before any of this test's own
        // disk-pressure logic even ran) with a real
        // java.nio.file.AccessDeniedException writing its own bootstrap
        // metadata checkpoint file.
        withTmpFs(Map.of("/var/lib/kafka/data", "rw,size=" + tmpfsSizeBytes + ",mode=1777"));
        withEnv("KAFKA_NODE_ID", "1");
        withEnv("KAFKA_PROCESS_ROLES", "broker,controller");
        withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092");
        withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT_HOST://localhost:" + hostPort + ",PLAINTEXT://" + ALIAS + ":19092");
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + ALIAS + ":29093");
        withEnv("CLUSTER_ID", CLUSTER_ID);
        withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data");
        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");
        withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
    }

    public String bootstrapServers(int hostPort) {
        return "localhost:" + hostPort;
    }
}
