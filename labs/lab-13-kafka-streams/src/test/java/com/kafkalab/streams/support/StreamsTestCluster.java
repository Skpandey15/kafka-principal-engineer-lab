package com.kafkalab.streams.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

/**
 * A real, single-node combined broker+controller Kafka cluster -- this
 * WP needs no Postgres, no Connect worker, no Debezium; Kafka Streams
 * is a plain client library on top of the broker.
 */
public final class StreamsTestCluster implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String CLUSTER_ID = "K14qqOcU8VV-Bz4Tp2Qi6Fg";
    private static final String KAFKA_ALIAS = "it-kafka";

    private final GenericContainer<?> kafka;
    private final int kafkaHostPort;

    public StreamsTestCluster(int kafkaHostPort) {
        this.kafkaHostPort = kafkaHostPort;
        this.kafka = new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
                .withFixedExposedPort(kafkaHostPort, 9092)
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
    }

    public void start() {
        kafka.start();
    }

    public String bootstrapServers() {
        return "localhost:" + kafkaHostPort;
    }

    @Override
    public void close() {
        kafka.stop();
    }
}
