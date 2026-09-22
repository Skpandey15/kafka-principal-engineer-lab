package com.kafkalab.performance.bench;

import com.kafkalab.performance.support.LabConfig;
import com.kafkalab.performance.support.PayloadGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

/**
 * Runs {@link ProducerBenchmark} against the reused WP-07 cluster with
 * a config profile picked via {@code -Pprofile=}, printing real,
 * measured results -- not simulated numbers.
 */
public final class BenchmarkApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "perf-bench");
        int recordCount = LabConfig.getInt("recordCount", 20_000);
        int payloadSize = LabConfig.getInt("payloadSize", 512);
        String profile = LabConfig.get("profile", "batched");

        Map<String, Object> config = switch (profile) {
            case "unbatched" -> Map.of(
                    ProducerConfig.LINGER_MS_CONFIG, 0,
                    ProducerConfig.BATCH_SIZE_CONFIG, 1,
                    ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
            case "batched" -> Map.of(
                    ProducerConfig.LINGER_MS_CONFIG, 20,
                    ProducerConfig.BATCH_SIZE_CONFIG, 65536,
                    ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
            case "batched-lz4" -> Map.of(
                    ProducerConfig.LINGER_MS_CONFIG, 20,
                    ProducerConfig.BATCH_SIZE_CONFIG, 65536,
                    ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            default -> throw new IllegalArgumentException("unknown profile: " + profile);
        };

        System.out.println("[bench] profile=" + profile + " recordCount=" + recordCount + " payloadSize=" + payloadSize);
        ProducerBenchmark.Result result = ProducerBenchmark.run(
                bootstrapServers, topic, config, recordCount, PayloadGenerator.compressible(payloadSize));

        System.out.println("[bench] elapsedMs=" + result.elapsedMs()
                + " recordsPerSec=" + String.format("%.1f", result.recordsPerSec())
                + " mbPerSec=" + String.format("%.2f", result.mbPerSec())
                + " p50Ms=" + result.p50LatencyMs()
                + " p95Ms=" + result.p95LatencyMs()
                + " p99Ms=" + result.p99LatencyMs()
                + " compressionRateAvg=" + result.compressionRateAvg());
    }
}
