package com.kafkalab.performance.bench;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A real, hand-built producer benchmark -- not {@code kafka-producer-perf-test.sh}
 * hidden behind a shell script, so every knob's effect on the REAL
 * {@link KafkaProducer} is directly visible and directly attributable
 * to the config that caused it.
 */
public final class ProducerBenchmark {

    public record Result(int recordCount, int payloadSize, long elapsedMs,
                          double recordsPerSec, double mbPerSec,
                          long p50LatencyMs, long p95LatencyMs, long p99LatencyMs,
                          double compressionRateAvg) {
    }

    public static Result run(String bootstrapServers, String topic, Map<String, Object> extraProducerConfig,
                              int recordCount, byte[] payload) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putAll(extraProducerConfig);

        AtomicLongArray latenciesNanos = new AtomicLongArray(recordCount);
        AtomicLong nextIndex = new AtomicLong();
        CountDownLatch done = new CountDownLatch(recordCount);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            long start = System.nanoTime();
            for (int i = 0; i < recordCount; i++) {
                long sendStart = System.nanoTime();
                int index = (int) nextIndex.getAndIncrement();
                producer.send(new ProducerRecord<>(topic, "k" + i, payload), (metadata, exception) -> {
                    latenciesNanos.set(index, System.nanoTime() - sendStart);
                    done.countDown();
                });
            }
            producer.flush();
            done.await();
            long elapsedNanos = System.nanoTime() - start;

            double compressionRateAvg = readCompressionRateAvg(producer);

            long[] sortedMs = new long[recordCount];
            for (int i = 0; i < recordCount; i++) {
                sortedMs[i] = latenciesNanos.get(i) / 1_000_000;
            }
            Arrays.sort(sortedMs);

            long elapsedMs = Math.max(1, elapsedNanos / 1_000_000);
            double totalMb = (recordCount * (long) payload.length) / (1024.0 * 1024.0);

            return new Result(
                    recordCount,
                    payload.length,
                    elapsedMs,
                    recordCount / (elapsedMs / 1000.0),
                    totalMb / (elapsedMs / 1000.0),
                    percentile(sortedMs, 0.50),
                    percentile(sortedMs, 0.95),
                    percentile(sortedMs, 0.99),
                    compressionRateAvg);
        }
    }

    private static double readCompressionRateAvg(KafkaProducer<?, ?> producer) {
        for (Map.Entry<MetricName, ? extends org.apache.kafka.common.Metric> entry : producer.metrics().entrySet()) {
            if ("compression-rate-avg".equals(entry.getKey().name()) && "producer-metrics".equals(entry.getKey().group())) {
                Object value = entry.getValue().metricValue();
                if (value instanceof Double d && !d.isNaN()) {
                    return d;
                }
            }
        }
        return Double.NaN;
    }

    private static long percentile(long[] sortedMs, double p) {
        int index = (int) Math.ceil(p * sortedMs.length) - 1;
        return sortedMs[Math.max(0, Math.min(index, sortedMs.length - 1))];
    }
}
