package com.kafkalab.controllerquorum.support;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A real KRaft cluster with the control plane and data plane on
 * SEPARATE nodes -- 3 controller-only voters plus 1 broker-only node --
 * driven entirely through Testcontainers' generic {@link GenericContainer}
 * API, mechanically mirroring
 * {@code platform/kraft-quorum/docker-compose.yml} (which uses 3 brokers
 * for the manual lab's fuller topology; this test helper uses only 1,
 * since every automated scenario this lab needs -- quorum formation,
 * controller failover, and whether metadata operations succeed or block
 * -- needs only that a broker exists, not how many).
 *
 * <p>Node IDs are unique across the whole cluster, exactly like the
 * manual environment: controllers are 1/2/3, the broker is 4.
 *
 * <p>See {@code com.kafkalab.replication.support.ThreeBrokerKafkaCluster}
 * (WP-07's equivalent test helper) for the concurrent-startup rationale
 * (KRaft nodes block waiting for their peers, so starting them one at a
 * time and waiting for each to become ready before creating the next
 * would deadlock) and the fixed-host-port rationale (Kafka's advertised
 * listener must be known before the broker process starts, but
 * Testcontainers only assigns a mapped port after the container starts).
 *
 * <p>Only the broker gets a fixed host port -- the controllers are never
 * given one, exactly like the manual environment, because nothing in
 * this cluster needs to reach a controller's listener directly from
 * outside the Docker network: {@code AdminClient.describeMetadataQuorum()}
 * bootstrapped via the broker is forwarded to the controller quorum
 * automatically.
 */
public final class ControllerQuorumCluster implements AutoCloseable {

    private static final String IMAGE = "apache/kafka:4.3.1";
    private static final String CLUSTER_ID = "6Zy-q9GlR3u9dQkH6Aj_4A";
    private static final int BROKER_NODE_ID = 4;

    private final Network network = Network.newNetwork();
    private final Map<Integer, GenericContainer<?>> controllers = new LinkedHashMap<>();
    private GenericContainer<?> broker;
    private final int brokerHostPort;

    public ControllerQuorumCluster(int brokerHostPort) {
        this.brokerHostPort = brokerHostPort;
    }

    /**
     * Starts the 3 controllers and the broker concurrently -- see class
     * Javadoc for why concurrently. Any exception thrown by a container's
     * own {@code start()} call (a real Testcontainers/Docker failure, not
     * just a slow startup) is captured and re-thrown from this method --
     * a plain {@code Thread}'s uncaught exception would otherwise be
     * silently swallowed (printed to stderr, never propagated), leaving
     * callers to hang forever waiting on peers that never actually
     * started.
     */
    public void start() {
        List<Thread> starters = new java.util.ArrayList<>();
        Map<String, Throwable> startupFailures = new java.util.concurrent.ConcurrentHashMap<>();

        for (int nodeId = 1; nodeId <= 3; nodeId++) {
            GenericContainer<?> controller = newControllerContainer(nodeId);
            controllers.put(nodeId, controller);
            String name = "kafka-controller-" + nodeId + "-starter";
            Thread starter = new Thread(() -> {
                try {
                    controller.start();
                } catch (Throwable t) {
                    startupFailures.put(name, t);
                }
            }, name);
            starters.add(starter);
            starter.start();
        }
        broker = newBrokerContainer();
        Thread brokerStarter = new Thread(() -> {
            try {
                broker.start();
            } catch (Throwable t) {
                startupFailures.put("kafka-broker-starter", t);
            }
        }, "kafka-broker-starter");
        starters.add(brokerStarter);
        brokerStarter.start();

        for (Thread starter : starters) {
            try {
                starter.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        if (!startupFailures.isEmpty()) {
            RuntimeException failure = new RuntimeException(
                    "Cluster startup failed for: " + startupFailures.keySet());
            startupFailures.values().forEach(failure::addSuppressed);
            throw failure;
        }
    }

    public void killController(int nodeId) {
        GenericContainer<?> controller = controllers.remove(nodeId);
        if (controller != null) {
            controller.stop();
        }
    }

    public void reviveController(int nodeId) {
        GenericContainer<?> controller = newControllerContainer(nodeId);
        controllers.put(nodeId, controller);
        controller.start();
    }

    public String bootstrapServers() {
        return "localhost:" + brokerHostPort;
    }

    @Override
    public void close() {
        if (broker != null) {
            broker.stop();
        }
        controllers.values().forEach(GenericContainer::stop);
        network.close();
    }

    private static String controllerQuorumVoters() {
        return "1@kafka-controller-1:29093,2@kafka-controller-2:29093,3@kafka-controller-3:29093";
    }

    private GenericContainer<?> newControllerContainer(int nodeId) {
        String alias = "kafka-controller-" + nodeId;
        return new GenericContainer<>(IMAGE)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(alias))
                .withEnv("KAFKA_NODE_ID", String.valueOf(nodeId))
                .withEnv("KAFKA_PROCESS_ROLES", "controller")
                .withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters())
                .withEnv("CLUSTER_ID", CLUSTER_ID)
                .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    private GenericContainer<?> newBrokerContainer() {
        String alias = "kafka-broker-1";
        return new FixedHostPortGenericContainer<>(IMAGE)
                .withFixedExposedPort(brokerHostPort, 9092)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(alias))
                .withEnv("KAFKA_NODE_ID", String.valueOf(BROKER_NODE_ID))
                .withEnv("KAFKA_PROCESS_ROLES", "broker")
                .withEnv("KAFKA_LISTENERS", "PLAINTEXT_HOST://:9092,PLAINTEXT://:19092")
                .withEnv("KAFKA_ADVERTISED_LISTENERS",
                        "PLAINTEXT_HOST://localhost:" + brokerHostPort + ",PLAINTEXT://" + alias + ":19092")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters())
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
}
