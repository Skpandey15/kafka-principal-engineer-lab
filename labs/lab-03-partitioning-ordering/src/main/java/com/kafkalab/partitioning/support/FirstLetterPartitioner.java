package com.kafkalab.partitioning.support;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * A deliberately small, purely educational custom {@link Partitioner}.
 * It routes a record to one of two halves of the topic's partitions based
 * on whether the record's key starts with a letter in the first or second
 * half of the alphabet -- a routing rule Kafka's built-in key hashing has
 * no way to express, since {@code murmur2(keyBytes) % partitions} knows
 * nothing about what the key logically means.
 *
 * <p>See {@code CustomPartitionerDemoApp} and
 * docs/partitioning/PARTITIONING_AND_ORDERING.md for why this capability
 * is powerful but not something to reach for casually: every producer
 * application that writes to this topic must load and agree on this exact
 * class (or an API-compatible equivalent) to get consistent routing, this
 * class's own routing logic becomes a long-lived contract the moment any
 * data depends on it, and it has the same "existing data doesn't move"
 * behavior under a partition-count change that the built-in partitioner
 * has -- this class does not solve that, it just makes the routing rule
 * explicit instead of implicit.
 */
public final class FirstLetterPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed for this educational example. A real
        // custom partitioner with tunable behavior would read its own
        // settings from `configs` here.
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();
        if (numPartitions < 2 || key == null) {
            return 0;
        }
        char firstChar = Character.toUpperCase(key.toString().charAt(0));
        boolean firstHalfOfAlphabet = firstChar <= 'M';
        int halfSize = Math.max(1, numPartitions / 2);
        return firstHalfOfAlphabet ? 0 : halfSize;
    }

    @Override
    public void close() {
        // Nothing to release for this educational example. A real custom
        // partitioner holding resources (a metrics client, a cache) would
        // release them here.
    }
}
