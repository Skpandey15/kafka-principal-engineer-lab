package com.kafkalab.performance;

import com.kafkalab.performance.capacity.CapacityCalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The "worked capacity workbook" half of this WP, as real, deterministic
 * arithmetic -- no cluster needed, since capacity planning is a
 * calculation over MEASURED numbers (produced by
 * {@code PerformanceIntegrationTest}'s own real benchmark), not a
 * property of any specific running cluster itself.
 */
class CapacityCalculatorTest {

    @Test
    void estimateStorageAccountsForEveryReplicaSeparately() {
        // 1,000 msgs/sec * 1,000 bytes * 86,400 sec/day = ~82.4 GB for ONE
        // replica per day; RF=3 means the cluster stores that 3 times over,
        // not once -- the native mechanism WP-07 covers (each replica is
        // an independent, full on-disk copy).
        var estimate = CapacityCalculator.estimateStorage(1_000, 1_000, 86_400, 3, 3);

        long oneReplicaBytes = Math.round(1_000.0 * 1_000 * 86_400);
        assertEquals(oneReplicaBytes * 3, estimate.totalBytesAcrossAllReplicas());
        assertEquals(Math.round(oneReplicaBytes * 3 / 3.0), estimate.averageBytesPerBroker());
    }

    @Test
    void requiredPartitionsForThroughputRoundsUpNotDown() {
        // A real per-partition ceiling of 3.0 MB/s, a target of 10 MB/s:
        // 4 partitions, not 3 -- 3 partitions' worth of headroom (9 MB/s)
        // would fall short of the actual target.
        int partitions = CapacityCalculator.requiredPartitionsForThroughput(10.0, 3.0);
        assertEquals(4, partitions);
    }

    @Test
    void requiredPartitionsForConsumerParallelismMatchesDesiredInstanceCount() {
        // The OTHER, independent reason to add partitions: even with
        // throughput headroom to spare, a partition can only be consumed
        // by ONE group member at a time (WP-04/WP-05's own mechanic) --
        // 12 desired parallel consumers need at least 12 partitions,
        // full stop, regardless of per-partition throughput.
        assertEquals(12, CapacityCalculator.requiredPartitionsForConsumerParallelism(12));
    }
}
