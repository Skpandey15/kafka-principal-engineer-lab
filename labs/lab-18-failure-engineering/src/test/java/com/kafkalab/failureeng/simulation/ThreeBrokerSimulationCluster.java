package com.kafkalab.failureeng.simulation;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A real, 3-broker KRaft cluster (the same shape WP-07's
 * {@code platform/kafka-cluster/} uses) -- needed here specifically
 * because a believable "broker dies mid-traffic, and the cluster keeps
 * serving" scenario needs real replication and real leader failover,
 * which a single-node cluster can't demonstrate at all.
 */
public final class ThreeBrokerSimulationCluster implements AutoCloseable {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String CLUSTER_ID = "K19uuSgY2ZZ-Fc8Xt6Um0Kj";

    private final Network network = Network.newNetwork();
    private final Map<Integer, GenericContainer<?>> brokers = new LinkedHashMap<>();
    private final Map<Integer, Integer> hostPorts;

    public ThreeBrokerSimulationCluster(Map<Integer, Integer> nodeIdToHostPort) {
        this.hostPorts = nodeIdToHostPort;
        String voters = nodeIdToHostPort.keySet().stream()
                .map(id -> id + "@sim-broker-" + id + ":29093")
                .reduce((a, b) -> a + "," + b)
                .orElseThrow();

        for (var entry : nodeIdToHostPort.entrySet()) {
            int nodeId = entry.getKey();
            int hostPort = entry.getValue();
            String alias = "sim-broker-" + nodeId;
            GenericContainer<?> broker = new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
                    .withFixedExposedPort(hostPort, 9092)
                    .withNetwork(network)
                    .withNetworkAliases(alias)
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName(alias))
                    .withEnv("KAFKA_NODE_ID", String.valueOf(nodeId))
                    .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                    .withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092")
                    .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT_HOST://localhost:" + hostPort + ",PLAINTEXT://" + alias + ":19092")
                    .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                    .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                    .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                    .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", voters)
                    .withEnv("CLUSTER_ID", CLUSTER_ID)
                    .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
                    .withEnv("KAFKA_OFFSETS_TOPIC_MIN_ISR", "2")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
                    .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "3")
                    .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "2")
                    .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                    .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
            brokers.put(nodeId, broker);
        }
    }

    /**
     * A real finding building this cluster: {@code GenericContainer#start()}
     * BLOCKS until its own wait strategy succeeds -- starting all three
     * brokers SEQUENTIALLY (a plain {@code forEach}) means node 1 waits
     * for "Kafka Server started" alone, which never happens, because a
     * 3-voter KRaft quorum needs a MAJORITY of voters reachable to elect
     * a controller leader at all. Confirmed the hard way: every
     * sequential-start attempt timed out waiting for log output that
     * could only ever appear once nodes 2 and 3 were ALSO already
     * starting. Starting all three concurrently is not an optimization
     * here -- it's the only way three-voter KRaft bootstrap can succeed.
     */
    public void start() {
        java.util.concurrent.ExecutorService starter = java.util.concurrent.Executors.newFixedThreadPool(brokers.size());
        try {
            List<java.util.concurrent.Future<Void>> futures = brokers.values().stream()
                    .map(b -> starter.submit(b::start, (Void) null))
                    .toList();
            for (var future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to start the 3-broker simulation cluster", e);
        } finally {
            starter.shutdown();
        }
    }

    public String bootstrapServers() {
        StringBuilder sb = new StringBuilder();
        for (int hostPort : hostPorts.values()) {
            if (!sb.isEmpty()) {
                sb.append(",");
            }
            sb.append("localhost:").append(hostPort);
        }
        return sb.toString();
    }

    /** Kills ONE broker for real (not a graceful stop) -- models a real process crash, not a controlled shutdown. */
    public void killBroker(int nodeId) {
        brokers.get(nodeId).getDockerClient().killContainerCmd(brokers.get(nodeId).getContainerId()).exec();
    }

    @Override
    public void close() {
        brokers.values().forEach(b -> {
            try {
                b.stop();
            } catch (Exception ignored) {
                // Already killed by killBroker -- stopping an already-dead container is a real no-op, not an error.
            }
        });
        network.close();
    }
}
