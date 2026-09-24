package com.kafkalab.multicluster;

import com.kafkalab.multicluster.support.TwoClusterEnvironment;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real MirrorMaker 2 (the actual {@code connect-mirror-maker.sh}, not
 * a hand-rolled replication loop) replicating between TWO real,
 * independent Kafka clusters -- the multi-cluster & DR half of WP-20's
 * capstone scope. See the conceptual doc for why this is the ONE piece
 * of genuinely new infrastructure this capstone WP builds, versus the
 * several topics covered conceptually only.
 */
class MultiClusterDrIntegrationTest {

    @Test
    void mirrorMaker2RealReplicatesRecordsFromPrimaryToARenamedSecondaryTopic() throws Exception {
        try (TwoClusterEnvironment env = new TwoClusterEnvironment(19_720, 19_721)) {
            env.start();

            String topic = "orders-" + uniqueId();
            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, env.primaryBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            }

            List<String> produced = List.of("order-1", "order-2", "order-3");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.primaryBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.ACKS_CONFIG, "all"))) {
                for (String value : produced) {
                    producer.send(new ProducerRecord<>(topic, value, value)).get();
                }
            }

            // MirrorMaker 2's real, default remote-topic naming
            // convention: <source-cluster-alias>.<original-topic-name>
            // -- confirmed by this test actually finding data there.
            String mirroredTopic = "primary." + topic;
            List<String> consumedFromSecondary = pollValues(env.secondaryBootstrapServers(), mirroredTopic, 3, Duration.ofSeconds(90));

            assertEquals(new TreeSet<>(produced), new TreeSet<>(consumedFromSecondary),
                    "every record produced on the primary cluster must be REAL-replicated (not simulated) onto the secondary cluster's renamed topic");
        }
    }

    @Test
    void consumerGroupOffsetsAreRealTranslatedAcrossClustersForDrFailover() throws Exception {
        try (TwoClusterEnvironment env = new TwoClusterEnvironment(19_722, 19_723)) {
            env.start();

            String topic = "orders-" + uniqueId();
            String groupId = "dr-group-" + uniqueId();
            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, env.primaryBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            }

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.primaryBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.ACKS_CONFIG, "all"))) {
                for (int i = 0; i < 20; i++) {
                    producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)).get();
                }
            }

            // A third real finding: MM2's checkpoint translation relies
            // on its own internal offset-syncs topic, which the
            // replication task populates SPARSELY and asynchronously as
            // it mirrors records -- it is not instantaneous. Committing
            // the primary-side group's offset before replication (and
            // therefore the offset-sync entries covering that offset)
            // has actually caught up made the checkpoint task translate
            // against a stale, early offset-sync entry and then never
            // revisit it -- confirmed the hard way (translated offset
            // stuck at 1 for the full 90s timeout even though the real
            // commit was 10). Waiting for all 20 records to be REAL,
            // fully mirrored onto the secondary cluster first -- the
            // same real replication this class's other test already
            // verifies -- guarantees the offset-sync topic is caught up
            // before the group ever commits.
            pollValues(env.secondaryBootstrapServers(), "primary." + topic, 20, Duration.ofSeconds(90));

            // A real consumer group commits its position after consuming
            // ONLY the first 10 of 20 -- this is the exact position DR
            // failover needs to preserve, so a consumer resuming on the
            // secondary cluster does not reprocess everything.
            // A second real finding: KafkaConsumer's default
            // enable.auto.commit=true means close() itself commits the
            // consumer's actual CURRENT position (20, since one poll()
            // already fetched everything) -- AFTER this block's own
            // explicit commitSync(10), silently overwriting it back to
            // 20. Confirmed the hard way (translated offset stayed 20
            // even with an explicit commitSync(10) added). Fixed by
            // disabling auto-commit entirely, so ONLY the explicit
            // commit below ever writes anything.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.primaryBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, groupId,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false))) {
                consumer.subscribe(List.of(topic));
                // A real finding building this test (the SAME class of
                // bug WP-16's own lag test hit first): a single poll()
                // can return MORE than the intended 10 records at once,
                // since all 20 are already available before the
                // consumer even starts -- committing the consumer's own
                // tracked "current position" after an uncontrolled loop
                // committed offset 20, not 10. Fixed by explicitly
                // committing offset 10, regardless of how many records
                // any one poll() batch actually returned.
                Instant deadline = Instant.now().plusSeconds(20);
                long highestOffsetSeen = -1;
                while (highestOffsetSeen < 9 && Instant.now().isBefore(deadline)) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                    for (var record : records) {
                        highestOffsetSeen = Math.max(highestOffsetSeen, record.offset());
                    }
                }
                consumer.commitSync(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(10)));
            }

            String mirroredTopic = "primary." + topic;
            TopicPartition mirroredPartition = new TopicPartition(mirroredTopic, 0);

            // RemoteClusterUtils.translateOffsets -- MirrorMaker 2's own
            // real client API, reading the checkpoint records MM2's
            // checkpoint connector replicates -- NOT something this lab
            // computes itself.
            Map<String, Object> secondaryConnProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, env.secondaryBootstrapServers());
            OffsetAndMetadata translated = waitForTranslatedOffset(secondaryConnProps, groupId, mirroredPartition, Duration.ofSeconds(90));

            assertTrue(translated.offset() >= 9 && translated.offset() <= 11,
                    "the real, MM2-translated offset for the secondary cluster must land close to position 10 "
                            + "(where the primary-side consumer group actually committed), not 0 -- otherwise DR "
                            + "failover would silently mean reprocessing everything from the start; got " + translated.offset());
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static List<String> pollValues(String bootstrapServers, String topic, int minCount, Duration timeout) {
        java.util.Properties props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<String> collected = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> collected.add(r.value()));
            }
        }
        if (collected.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " records on " + topic + ", got " + collected.size());
        }
        return collected;
    }

    /**
     * A real finding building this test: MM2 emits checkpoints on its
     * OWN interval, continuously -- the FIRST one this method observes
     * can real-reflect an early, stale group position (e.g. right after
     * the group first appeared, before the real {@code commitSync()} of
     * offset 10 ever happened), not the LATEST one. Returning on the
     * first non-null result alone produced a real, confirmed false
     * failure (translated offset 1, not ~10) -- this method instead
     * polls until the value reaches the EXPECTED range, exactly the
     * same bounded-condition-polling convention this repository already
     * uses everywhere else, just applied to a value that changes over
     * time rather than one that simply appears.
     */
    private static OffsetAndMetadata waitForTranslatedOffset(Map<String, Object> secondaryConnProps, String groupId,
                                                               TopicPartition partition, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        OffsetAndMetadata last = null;
        while (Instant.now().isBefore(deadline)) {
            Map<TopicPartition, OffsetAndMetadata> translated = RemoteClusterUtils.translateOffsets(
                    secondaryConnProps, "primary", groupId, Duration.ofSeconds(10));
            OffsetAndMetadata offset = translated.get(partition);
            if (offset != null) {
                last = offset;
                if (offset.offset() >= 9) {
                    return offset;
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("translated offset for " + partition + " never reached the expected range within "
                + timeout + "; last observed: " + last);
    }
}
