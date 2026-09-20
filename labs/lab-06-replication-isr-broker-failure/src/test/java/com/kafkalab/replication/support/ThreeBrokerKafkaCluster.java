package com.kafkalab.replication.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A real, 3-node, combined broker+controller KRaft cluster driven entirely
 * through Testcontainers' generic {@link GenericContainer} API -- there is
 * no ready-made "multi-broker KafkaContainer" in Testcontainers, so this
 * class wires the same topology
 * {@code platform/kafka-cluster/docker-compose.yml} runs manually, using
 * the same environment variables, so what this test suite exercises is
 * mechanically identical to what the lab's manual experiments use, not a
 * simplified stand-in for it.
 *
 * <h2>Fixed host ports, on purpose</h2>
 * Kafka's advertised listeners must be known before the broker process
 * starts, but Testcontainers normally only assigns a container's mapped
 * host port after it starts -- the same chicken-and-egg problem the
 * official {@code org.testcontainers.kafka.KafkaContainer} solves
 * internally for a single broker. For three independently-addressed
 * brokers, this class sidesteps the problem the direct way:
 * {@link GenericContainer#withFixedExposedPort} pins each broker's
 * client-facing port to a specific, known host port chosen by this class,
 * so the advertised-listener value can be computed before {@code start()}
 * is ever called.
 *
 * <h2>What "broker recovery" means in these tests, precisely</h2>
 * {@link #killBroker} stops and removes that broker's container outright
 * -- Testcontainers does not support restarting the exact same container
 * with new configuration, and this class does not attach a persistent
 * volume to any broker's log directory. {@link #reviveBroker} therefore
 * starts a brand-new container with the SAME {@code node.id} and network
 * alias, backed by empty, freshly-formatted storage. This is not a
 * simplification of what "broker recovery" means -- it is a faithful
 * simulation of the real-world case where a broker's disk is lost and
 * replaced (a fresh log directory, same identity): Kafka's own replication
 * protocol treats both cases identically, since a broker's identity is its
 * {@code node.id}, not its container ID or its disk contents. The
 * recovered broker must fetch every record it's a replica for from the
 * current leader, exactly as it would after a real disk replacement.
 */
public final class ThreeBrokerKafkaCluster implements AutoCloseable {

    private static final String IMAGE = "apache/kafka:4.3.1";
    private static final String CLUSTER_ID = "K2cByfqsRAO_ktpfonxKLw";
    private static final int CONTAINER_CLIENT_PORT = 9092;

    private final Network network = Network.newNetwork();
    private final Map<Integer, Integer> hostPorts = new LinkedHashMap<>();
    private final Map<Integer, GenericContainer<?>> brokers = new LinkedHashMap<>();
    private final int basePort;

    public ThreeBrokerKafkaCluster(int basePort) {
        this.basePort = basePort;
        for (int nodeId = 1; nodeId <= 3; nodeId++) {
            hostPorts.put(nodeId, basePort + nodeId);
        }
    }

    /**
     * Starts all three nodes concurrently, on separate threads. This is
     * not an optimization -- it is required for correctness. Each node's
     * KRaft startup blocks (internally, before logging "Kafka Server
     * started") until it can actually reach the OTHER quorum voters; if
     * this method started them one at a time and waited for each to
     * become ready before creating the next, node 1 would wait forever
     * for nodes 2 and 3, which do not exist yet.
     */
    public void start() {
        List<Thread> starters = new java.util.ArrayList<>();
        for (int nodeId = 1; nodeId <= 3; nodeId++) {
            GenericContainer<?> broker = newBrokerContainer(nodeId);
            brokers.put(nodeId, broker);
            Thread starter = new Thread(broker::start, "kafka-broker-" + nodeId + "-starter");
            starters.add(starter);
            starter.start();
        }
        for (Thread starter : starters) {
            try {
                starter.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public void killBroker(int nodeId) {
        GenericContainer<?> broker = brokers.remove(nodeId);
        if (broker != null) {
            broker.stop();
        }
    }

    public void reviveBroker(int nodeId) {
        GenericContainer<?> broker = newBrokerContainer(nodeId);
        brokers.put(nodeId, broker);
        broker.start();
    }

    public String bootstrapServers() {
        return hostPorts.values().stream()
                .map(port -> "localhost:" + port)
                .collect(Collectors.joining(","));
    }

    @Override
    public void close() {
        brokers.values().forEach(GenericContainer::stop);
        network.close();
    }

    private GenericContainer<?> newBrokerContainer(int nodeId) {
        String alias = "kafka-broker-" + nodeId;
        int hostPort = hostPorts.get(nodeId);
        String controllerVoters = "1@kafka-broker-1:29093,2@kafka-broker-2:29093,3@kafka-broker-3:29093";

        GenericContainer<?> container = new FixedHostPortGenericContainer<>(IMAGE)
                .withFixedExposedPort(hostPort, CONTAINER_CLIENT_PORT)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(alias))
                .withEnv("KAFKA_NODE_ID", String.valueOf(nodeId))
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092")
                .withEnv("KAFKA_ADVERTISED_LISTENERS",
                        "PLAINTEXT_HOST://localhost:" + hostPort + ",PLAINTEXT://" + alias + ":19092")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerVoters)
                .withEnv("CLUSTER_ID", CLUSTER_ID)
                .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
                .withEnv("KAFKA_OFFSETS_TOPIC_MIN_ISR", "2")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "3")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "2")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        return container;
    }
}
