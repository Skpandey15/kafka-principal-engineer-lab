package com.kafkalab.performance.capacity;

/**
 * The "worked capacity workbook" half of this WP, as real, testable
 * arithmetic rather than a static spreadsheet -- deliberately built to
 * take a MEASURED per-partition throughput ceiling (from
 * {@link com.kafkalab.performance.bench.ProducerBenchmark}, run against
 * THIS repository's own pinned Kafka version) as an input, not an
 * assumed industry-average number, so tuning (measure what one
 * partition can really do) and sizing (how many of those do you need)
 * stay connected instead of taught as separate tracks.
 */
public final class CapacityCalculator {

    private CapacityCalculator() {
    }

    public record StorageEstimate(long totalBytesAcrossAllReplicas, long averageBytesPerBroker) {
    }

    /**
     * Total on-disk storage a topic needs across the WHOLE cluster --
     * every replica counted separately, since each replica is a full,
     * independent on-disk copy (the native mechanism WP-07 covers).
     */
    public static StorageEstimate estimateStorage(double messagesPerSecond, double avgMessageSizeBytes,
                                                    long retentionSeconds, int replicationFactor, int brokerCount) {
        if (brokerCount <= 0) {
            throw new IllegalArgumentException("brokerCount must be positive");
        }
        double bytesPerSecondOneReplica = messagesPerSecond * avgMessageSizeBytes;
        double totalBytesOneReplica = bytesPerSecondOneReplica * retentionSeconds;
        long totalBytesAllReplicas = Math.round(totalBytesOneReplica * replicationFactor);
        long averageBytesPerBroker = Math.round(totalBytesAllReplicas / (double) brokerCount);
        return new StorageEstimate(totalBytesAllReplicas, averageBytesPerBroker);
    }

    /**
     * How many partitions a topic needs to sustain a target aggregate
     * throughput, given a REAL measured per-partition ceiling -- not a
     * rule of thumb. Rounds up: a partial partition's worth of headroom
     * still needs a whole additional partition.
     */
    public static int requiredPartitionsForThroughput(double targetAggregateBytesPerSecond, double measuredPerPartitionBytesPerSecond) {
        if (measuredPerPartitionBytesPerSecond <= 0) {
            throw new IllegalArgumentException("measuredPerPartitionBytesPerSecond must be positive");
        }
        return (int) Math.ceil(targetAggregateBytesPerSecond / measuredPerPartitionBytesPerSecond);
    }

    /**
     * The OTHER, independent reason a topic might need more partitions
     * than its throughput alone would require: consumer parallelism.
     * The native mechanic (WP-04/WP-05): a partition can be consumed by
     * at most one member of a consumer group at a time, so partition
     * count is a hard ceiling on how many consumer instances can do
     * useful work in parallel, regardless of how much throughput
     * headroom any one partition has left.
     */
    public static int requiredPartitionsForConsumerParallelism(int desiredParallelConsumerInstances) {
        if (desiredParallelConsumerInstances <= 0) {
            throw new IllegalArgumentException("desiredParallelConsumerInstances must be positive");
        }
        return desiredParallelConsumerInstances;
    }
}
