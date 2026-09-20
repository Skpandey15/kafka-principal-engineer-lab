package com.kafkalab.controllerquorum.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.QuorumInfo;

import java.util.Properties;

/**
 * Prints the metadata quorum's status via {@code Admin.describeMetadataQuorum()}
 * -- the Java-API equivalent of {@code kafka-metadata-quorum.sh describe
 * --status}, verified directly against the real, source-inspected
 * {@code kafka-clients:4.3.1} {@link QuorumInfo} class rather than
 * assumed from an older version's shape. Bootstraps via an ordinary
 * broker connection (any broker forwards this request to the current
 * controller leader); see {@code ControllerQuorumInspector} for the
 * programmatic version this lab's tests use.
 */
public final class DescribeQuorumApp {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        try (Admin admin = Admin.create(props)) {
            QuorumInfo quorum = admin.describeMetadataQuorum().quorumInfo().get();
            System.out.println("LeaderId:        " + quorum.leaderId());
            System.out.println("LeaderEpoch:     " + quorum.leaderEpoch());
            System.out.println("HighWatermark:   " + quorum.highWatermark());
            System.out.println("Voters:");
            for (QuorumInfo.ReplicaState voter : quorum.voters()) {
                System.out.printf("  id=%d logEndOffset=%d lastFetchTimestamp=%s lastCaughtUpTimestamp=%s%n",
                        voter.replicaId(), voter.logEndOffset(), voter.lastFetchTimestamp(), voter.lastCaughtUpTimestamp());
            }
            System.out.println("Observers:");
            for (QuorumInfo.ReplicaState observer : quorum.observers()) {
                System.out.printf("  id=%d logEndOffset=%d%n", observer.replicaId(), observer.logEndOffset());
            }
        }
    }
}
