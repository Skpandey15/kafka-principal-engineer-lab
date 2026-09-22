package com.kafkalab.performance.support;

import java.util.Random;

/**
 * Generates records of a specific, controlled size -- benchmarking
 * needs a known payload size to compute real bytes/sec, not just
 * records/sec. A REPEATING byte pattern (not random) deliberately --
 * highly compressible input is what makes compression's real effect
 * (Section on compression in the conceptual doc) actually visible;
 * random bytes would compress poorly regardless of algorithm, hiding
 * the real difference between codecs.
 */
public final class PayloadGenerator {

    private PayloadGenerator() {
    }

    public static byte[] compressible(int sizeBytes) {
        byte[] payload = new byte[sizeBytes];
        String pattern = "kafka-performance-lab-payload-";
        byte[] patternBytes = pattern.getBytes();
        for (int i = 0; i < sizeBytes; i++) {
            payload[i] = patternBytes[i % patternBytes.length];
        }
        return payload;
    }

    /** High-entropy payload -- for the rare case a test needs to show compression does NOT help. */
    public static byte[] random(int sizeBytes, long seed) {
        byte[] payload = new byte[sizeBytes];
        new Random(seed).nextBytes(payload);
        return payload;
    }
}
