package com.kafkalab.transactions.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

/**
 * A real, single-node, combined broker+controller KRaft cluster for this
 * lab's automated tests -- mechanically the same shape as
 * {@code platform/kafka/docker-compose.yml} (WP-02's single-node
 * environment), NOT the WP-07 3-broker cluster
 * (@code platform/kafka-cluster/}) this lab's MANUAL experiments reuse.
 *
 * <h2>Why this test environment is single-node when the manual lab reuses a
 * 3-broker cluster</h2>
 * Every transactional-correctness property this lab's automated tests
 * check -- commit/abort visibility, isolation levels, atomicity across
 * partitions, producer fencing, and consume-transform-produce EOS -- is a
 * property of the TRANSACTION PROTOCOL (the coordinator, the producer
 * epoch, the two-phase commit-marker writes), not of replication factor.
 * None of it requires more than one broker to demonstrate correctly, and a
 * single node starts in seconds instead of tens of seconds and needs no
 * concurrent-startup workaround. Internal topics (including
 * {@code __transaction_state}) are therefore configured with replication
 * factor 1 here, exactly like {@code platform/kafka}'s single-node
 * environment already does for the same reason -- this is a single-node
 * NECESSITY, not a durability recommendation, and is not what this WP's
 * manual experiments run against. The manual lab experiments (see the
 * README) run against the reused WP-07 cluster specifically because that
 * environment already has {@code __transaction_state} configured with
 * replication factor 3 / min.insync.replicas 2, which is what "a cluster
 * configured appropriately for transactional state" means in production
 * terms.
 */
public final class TransactionsKafkaCluster implements AutoCloseable {

    private static final String IMAGE = "apache/kafka:4.3.1";
    private static final String CLUSTER_ID = "T8xqLzR5RS-Yw1Qm9Nf3Dg";
    private static final String ALIAS = "txn-lab-kafka";

    private final GenericContainer<?> container;
    private final int hostPort;

    public TransactionsKafkaCluster(int hostPort) {
        this.hostPort = hostPort;
        this.container = new FixedHostPortGenericContainer<>(IMAGE)
                .withFixedExposedPort(hostPort, 9092)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(ALIAS))
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT_HOST://localhost:" + hostPort + ",PLAINTEXT://" + ALIAS + ":19092")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + ALIAS + ":29093")
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
    }

    public void start() {
        container.start();
    }

    public String bootstrapServers() {
        return "localhost:" + hostPort;
    }

    @Override
    public void close() {
        container.stop();
    }
}
