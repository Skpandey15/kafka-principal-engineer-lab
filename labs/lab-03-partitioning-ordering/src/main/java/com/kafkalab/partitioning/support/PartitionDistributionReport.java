package com.kafkalab.partitioning.support;

import java.util.Map;
import java.util.TreeMap;

/**
 * A small, lab-oriented reporting utility -- not a production metrics
 * framework. Every number it prints comes from records this lab's
 * experiments actually sent and had acknowledged; nothing here is
 * simulated or hard-coded.
 *
 * <p>Deliberately prints every partition from {@code 0} to
 * {@code totalPartitionCount - 1}, including ones with zero records --
 * a partition that received nothing is exactly the fact the
 * low-cardinality-key experiment exists to make visible, and a report
 * that only listed partitions with traffic would hide it.
 */
public final class PartitionDistributionReport {

    private PartitionDistributionReport() {
    }

    public static void print(String title, Map<Integer, Long> countsByPartition, int totalPartitionCount) {
        Map<Integer, Long> complete = new TreeMap<>();
        for (int p = 0; p < totalPartitionCount; p++) {
            complete.put(p, countsByPartition.getOrDefault(p, 0L));
        }

        long total = complete.values().stream().mapToLong(Long::longValue).sum();
        long min = complete.values().stream().mapToLong(Long::longValue).min().orElse(0);
        long max = complete.values().stream().mapToLong(Long::longValue).max().orElse(0);
        double average = totalPartitionCount == 0 ? 0.0 : (double) total / totalPartitionCount;

        System.out.println();
        System.out.println(title);
        System.out.println();
        System.out.printf("%-10s %-10s %s%n", "Partition", "Records", "Percentage");
        complete.forEach((partition, count) -> {
            double pct = total == 0 ? 0.0 : (100.0 * count / total);
            System.out.printf("%-10d %-10d %6.2f%%%n", partition, count, pct);
        });
        System.out.println();
        System.out.printf("Total       %d%n", total);
        System.out.printf("Average     %.2f%n", average);
        System.out.printf("Min         %d%n", min);
        System.out.printf("Max         %d%n", max);
        // The skew indicator this lab uses: max partition / average partition.
        // ~1.0 means the hottest partition in THIS sample is close to the
        // sample average; higher means more skew. This is a teaching metric
        // for this lab, not a production alerting threshold -- see the
        // README/docs for why no universal threshold is given.
        double maxOverAvg = average == 0 ? 0.0 : max / average;
        System.out.printf("Max/Avg     %.2f%n", maxOverAvg);
        long emptyPartitions = complete.values().stream().filter(c -> c == 0).count();
        if (emptyPartitions > 0) {
            System.out.printf("Empty partitions (0 records): %d of %d%n", emptyPartitions, totalPartitionCount);
        }
    }
}
