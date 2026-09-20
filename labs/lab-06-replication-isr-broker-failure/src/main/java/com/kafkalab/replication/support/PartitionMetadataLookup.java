package com.kafkalab.replication.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * A small {@code AdminClient} wrapper used to answer "who is the leader of
 * this partition right now" as a genuinely separate query from the
 * producer/consumer data path.
 *
 * <p>This distinction matters for this lab specifically: neither
 * {@code RecordMetadata} (returned to a producer's send callback) nor a
 * {@code ConsumerRecord} exposes which broker actually served that
 * particular request. Logging a broker/leader identity next to a send or
 * poll result would therefore be a fabrication dressed up as observed
 * fact. This class exists so this lab's continuous producer/consumer apps
 * can honestly log "the leader was X as of this separate admin query taken
 * around this time" instead -- see {@code ContinuousProducerApp}'s
 * Javadoc for exactly how that distinction is preserved in its log output.
 */
public final class PartitionMetadataLookup implements AutoCloseable {

    private final Admin admin;

    public PartitionMetadataLookup(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = Admin.create(props);
    }

    /** Returns the current leader's broker id for one partition, or -1 if there is none (offline partition). */
    public int currentLeaderId(String topic, int partition) throws ExecutionException, InterruptedException {
        TopicDescription description = admin.describeTopics(java.util.List.of(topic))
                .topicNameValues().get(topic).get();
        for (TopicPartitionInfo p : description.partitions()) {
            if (p.partition() == partition) {
                return p.leader() == null ? -1 : p.leader().id();
            }
        }
        throw new IllegalArgumentException("No such partition: " + topic + "-" + partition);
    }

    /** Returns a compact "leader=X replicas=[..] isr=[..]" description for one partition. */
    public String describePartition(String topic, int partition) throws ExecutionException, InterruptedException {
        TopicDescription description = admin.describeTopics(java.util.List.of(topic))
                .topicNameValues().get(topic).get();
        for (TopicPartitionInfo p : description.partitions()) {
            if (p.partition() == partition) {
                var replicaIds = p.replicas().stream().map(n -> n.id()).toList();
                var isrIds = p.isr().stream().map(n -> n.id()).toList();
                String leaderId = p.leader() == null ? "none (offline)" : String.valueOf(p.leader().id());
                return "leader=" + leaderId + " replicas=" + replicaIds + " isr=" + isrIds;
            }
        }
        throw new IllegalArgumentException("No such partition: " + topic + "-" + partition);
    }

    @Override
    public void close() {
        admin.close();
    }
}
