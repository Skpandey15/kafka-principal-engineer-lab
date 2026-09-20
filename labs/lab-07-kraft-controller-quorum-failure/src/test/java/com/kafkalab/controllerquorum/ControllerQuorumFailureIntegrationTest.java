package com.kafkalab.controllerquorum;

import com.kafkalab.controllerquorum.support.ControllerQuorumCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A real KRaft cluster with dedicated controller and broker roles (see
 * {@link ControllerQuorumCluster}) -- no mocked quorum, no mocked
 * {@code AdminClient}. Every assertion below is reached through bounded
 * condition-polling with an overall deadline, never a fixed sleep used
 * as the synchronization mechanism, per this repository's established
 * test convention.
 *
 * <p>Tests run against ONE shared cluster ({@code @BeforeAll}/
 * {@code @AfterAll}) and are written to leave the quorum back at full
 * strength (3/3) by the end of each test that weakens it, so test order
 * does not matter and each test can assume a healthy quorum at its
 * start.
 */
class ControllerQuorumFailureIntegrationTest {

    private static ControllerQuorumCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new ControllerQuorumCluster(19_500);
        cluster.start();
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Test
    void threeControllerQuorumFormsWithAnActiveLeader() throws Exception {
        try (Admin admin = newAdmin()) {
            waitUntil(() -> {
                QuorumInfo info = describeQuorumOrNull(admin);
                return info != null && info.voters().size() == 3;
            }, Duration.ofSeconds(30));

            QuorumInfo info = admin.describeMetadataQuorum().quorumInfo().get();
            assertEquals(3, info.voters().size(), "all 3 controllers should be voters");
            assertTrue(Set.of(1, 2, 3).contains(info.leaderId()),
                    "the active controller must be one of the 3 configured voters");
        }
    }

    @Test
    void controllerLeaderFailureCausesANewLeaderToEmerge() throws Exception {
        try (Admin admin = newAdmin()) {
            waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(30));
            int originalLeader = admin.describeMetadataQuorum().quorumInfo().get().leaderId();

            cluster.killController(originalLeader);
            try {
                waitUntil(() -> {
                    QuorumInfo info = describeQuorumOrNull(admin);
                    return info != null && info.leaderId() != originalLeader
                            && Set.of(1, 2, 3).contains(info.leaderId());
                }, Duration.ofSeconds(60));

                int newLeader = admin.describeMetadataQuorum().quorumInfo().get().leaderId();
                assertNotEquals(originalLeader, newLeader,
                        "a different controller must have taken over as the active controller");
            } finally {
                cluster.reviveController(originalLeader);
                waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(60));
            }
        }
    }

    @Test
    void oneControllerLossPreservesQuorumForMetadataOperations() throws Exception {
        try (Admin admin = newAdmin()) {
            waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(30));
            int leader = admin.describeMetadataQuorum().quorumInfo().get().leaderId();
            int aFollower = Set.of(1, 2, 3).stream().filter(id -> id != leader).findFirst().orElseThrow();

            cluster.killController(aFollower);
            try {
                // With 2 of 3 controllers still up, quorum majority (2) is
                // intact -- a metadata mutation (creating a topic) must
                // still succeed, and within a normal timeout, not just
                // "eventually."
                String topic = uniqueTopic("two-of-three");
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all().get(15, TimeUnit.SECONDS);

                Set<String> topics = admin.listTopics().names().get();
                assertTrue(topics.contains(topic), "topic creation must succeed with 2 of 3 controllers alive");
            } finally {
                cluster.reviveController(aFollower);
                waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(60));
            }
        }
    }

    @Test
    void quorumLossBlocksMetadataMutations() throws Exception {
        try (Admin admin = newAdmin()) {
            waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(30));

            // Kill 2 of the 3 controllers -- only 1 voter remains, below
            // the majority (2) required to commit new metadata.
            cluster.killController(1);
            cluster.killController(2);
            try {
                String topic = uniqueTopic("no-quorum");
                // Without a controller-quorum majority, nothing can ever
                // process this request -- real, observed behavior is that
                // the future simply never completes, so the CLIENT-SIDE
                // Future.get(timeout, unit) call itself times out (a bare
                // java.util.concurrent.TimeoutException, NOT wrapped in
                // ExecutionException the way a server-returned error would
                // be). This is itself the finding: quorum loss doesn't
                // produce a fast, clean rejection -- it produces silence.
                assertThrows(TimeoutException.class,
                        () -> admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                                .all().get(10, TimeUnit.SECONDS));
            } finally {
                cluster.reviveController(1);
                cluster.reviveController(2);
                waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(90));
            }
        }
    }

    @Test
    void restoringQuorumAllowsMetadataOperationsAgain() throws Exception {
        try (Admin admin = newAdmin()) {
            waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(30));

            cluster.killController(1);
            cluster.killController(2);
            waitUntil(() -> {
                QuorumInfo info = describeQuorumOrNull(admin);
                // With only one voter reachable, describeMetadataQuorum()
                // itself may time out (the request needs the leader to
                // respond, and the leader may be one of the killed
                // voters) -- either a null result or a stale voter list
                // both indicate quorum is impaired, which is all this
                // wait is confirming before we test recovery.
                return info == null || info.voters().size() < 3;
            }, Duration.ofSeconds(30));

            cluster.reviveController(1);
            cluster.reviveController(2);
            waitUntil(() -> allVotersPresent(admin, 3), Duration.ofSeconds(90));

            String topic = uniqueTopic("quorum-restored");
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
            Set<String> topics = admin.listTopics().names().get();
            assertTrue(topics.contains(topic), "topic creation must succeed again once quorum majority is restored");
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private static Admin newAdmin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
        return Admin.create(props);
    }

    private static QuorumInfo describeQuorumOrNull(Admin admin) {
        try {
            return admin.describeMetadataQuorum().quorumInfo().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean allVotersPresent(Admin admin, int expectedCount) {
        QuorumInfo info = describeQuorumOrNull(admin);
        return info != null && info.voters().size() == expectedCount;
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
