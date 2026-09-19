package com.kafkalab.partitioning.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.List;
import java.util.Properties;

/**
 * A tiny read-only wrapper around {@link Admin} for the one thing every
 * distribution experiment in this lab needs before it can build a
 * complete report: how many partitions does the topic actually have right
 * now. Topic creation and partition-count changes stay a CLI concern in
 * this lab (see the README's Setup) -- this class only ever reads.
 */
public final class TopicInspector {

    private TopicInspector() {
    }

    public static int partitionCount(String bootstrapServers, String topic) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(adminProps)) {
            return admin.describeTopics(List.of(topic))
                    .allTopicNames().get()
                    .get(topic)
                    .partitions().size();
        }
    }
}
