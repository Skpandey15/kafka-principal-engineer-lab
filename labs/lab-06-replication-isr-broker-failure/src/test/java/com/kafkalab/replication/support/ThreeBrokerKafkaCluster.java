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
 * <h2>What "broker recovery" means in these tests, precisely -- and what it
 * does NOT mean</h2>
 * There are two different things "a broker comes back" can refer to, and
 * this class deliberately implements only one of them:
 *
 * <ol>
 *   <li><b>Ordinary restart (NOT what this class does).</b> The same
 *       broker process stops and starts again with its original
 *       configuration AND its original log directory intact -- the same
 *       on-disk segments, the same directory identity KRaft already
 *       recorded for that storage. Kafka only needs that broker to catch
 *       up on whatever it missed while it was down. This is what
 *       {@code platform/kafka-cluster/docker-compose.yml}'s manual
 *       {@code docker start} experiment in the lab README exercises --
 *       {@code docker start} reuses the same container and the same named
 *       volume, so the broker's storage is genuinely retained.</li>
 *   <li><b>What {@link #reviveBroker} actually does.</b> {@link #killBroker}
 *       stops and removes that broker's container outright -- Testcontainers
 *       does not support restarting the exact same container with new
 *       configuration, and this class does not attach a persistent volume
 *       to any broker's log directory. {@link #reviveBroker} therefore
 *       starts a brand-new container, reusing the same configured
 *       {@code node.id} and network alias, but backed by empty,
 *       never-before-formatted storage. This is closer to a real
 *       broker/storage-replacement recovery -- disk lost and swapped, same
 *       broker slot reconfigured on the new disk -- than to the ordinary
 *       restart above.</li>
 * </ol>
 *
 * <p><b>Do not read this as "a broker's identity is just its {@code node.id},
 * independent of its storage."</b> That overstates it. {@code node.id} is
 * the broker's assigned identity within the cluster's membership and
 * partition assignments -- it is what lets a new container "become" the
 * same logical broker again. But the storage backing that broker carries
 * its own persistent identity too: modern KRaft tracks log-directory
 * identity as part of its metadata (introduced for JBOD-style per-directory
 * awareness), and this repository has not independently re-verified the
 * exact wire-level mechanics of that tracking against the pinned
 * {@code apache/kafka:4.3.1} build. What this class's tests DO verify
 * directly is the observable, practical consequence: a broker rejoining
 * under its old {@code node.id} but with fresh, empty storage is not
 * "instantly caught up" the way a retained-storage restart would be -- it
 * has to be fully re-replicated for every partition it's assigned, from
 * scratch, before {@link #reviveBroker}'s callers should expect it back in
 * ISR. {@code revivedBrokerRejoinsIsrAfterCatchingUp} asserts exactly that
 * bounded catch-up, not an instant rejoin.
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

    /**
     * Starts a fresh container under the same {@code node.id}, backed by
     * empty storage -- a storage-replacement-style recovery, not an
     * ordinary restart. See the class Javadoc's "What 'broker recovery'
     * means in these tests" for the full distinction.
     */
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
