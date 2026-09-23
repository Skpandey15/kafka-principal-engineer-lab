package com.kafkalab.failureeng.disk;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ONE failure-matrix row explicitly assigned to this WP: "Disk
 * fills on a broker," simulated SAFELY -- a real, size-capped tmpfs
 * the broker can genuinely exhaust, never the host's real disk. A real
 * `apache/kafka:4.3.1` broker, a real filesystem-full condition, a
 * real observed failure -- not a mocked exception.
 */
class DiskPressureTest {

    @Test
    void writesFailOnceTheBrokersLogDirectoryGenuinelyFillsUp() throws Exception {
        int hostPort = 19_705;
        // 48MB -- small enough to fill in seconds with modest records,
        // comfortably larger than KRaft's own __cluster_metadata
        // overhead so the failure is from THIS test's own writes, not
        // an immediate startup failure.
        try (DiskConstrainedKafkaContainer kafka = new DiskConstrainedKafkaContainer(hostPort, "48m")) {
            kafka.start();
            String bootstrapServers = kafka.bootstrapServers(hostPort);

            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                admin.createTopics(List.of(new NewTopic("disk-pressure", 1, (short) 1))).all().get();
            }

            byte[] payload = new byte[512 * 1024];
            new Random(42).nextBytes(payload);

            Map<String, Object> producerProps = Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                    // Real production tuning would retry generously; this
                    // test wants to OBSERVE the failure quickly, not mask
                    // it behind retries.
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

            Exception realFailure = null;
            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
                // Comfortably more data than the 48MB tmpfs can hold --
                // real evidence the failure isn't a timing fluke.
                for (int i = 0; i < 300 && realFailure == null; i++) {
                    try {
                        producer.send(new ProducerRecord<>("disk-pressure", "k" + i, payload))
                                .get(10, TimeUnit.SECONDS);
                    } catch (ExecutionException | TimeoutException e) {
                        realFailure = e;
                    }
                }
            }

            assertTrue(realFailure != null,
                    "producing ~150MB of records into a real, 48MB-capped filesystem must eventually surface a "
                            + "real write failure -- Kafka does not preemptively throttle as the disk fills, exactly "
                            + "as the failure-matrix row for this scenario says");
        }
    }
}
