package com.kafkalab.deliverysemantics.consumer;

import com.kafkalab.deliverysemantics.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Section 1 of the WP-06 spec, made runnable: the difference between a
 * record's own offset, this consumer's in-memory <em>position</em>, and
 * the group's durably <em>committed</em> offset -- and why restarting a
 * consumer replays exactly the gap between those last two, no more and
 * no less.
 *
 * <p>Three phases, run in one process so the narrative reads top to
 * bottom in the console:
 * <ol>
 *   <li><b>First run</b> -- a fresh consumer in a fresh group fetches a
 *       batch, but only commits up to the second-to-last record on
 *       purpose. This leaves {@code position > committed offset}: the
 *       consumer has already moved past records it never told the group
 *       it finished.</li>
 *   <li><b>Restart</b> -- a brand-new {@code KafkaConsumer} instance
 *       (same {@code group.id}) subscribes. It resumes from the last
 *       *committed* offset, not from where phase 1's position happened
 *       to be -- so the uncommitted tail record from phase 1 is fetched
 *       again. That is a replay, and it is correct: nothing told Kafka
 *       that record was ever finished.</li>
 *   <li><b>{@code auto.offset.reset}</b> -- a consumer in a never-before-seen
 *       group (no committed offset exists at all) subscribes with the
 *       configured {@code autoOffsetReset} policy, and this app prints
 *       exactly where it starts: {@code earliest} starts at the low
 *       watermark (replays full retained history); {@code latest} starts
 *       at the high watermark (sees only records produced from now on).</li>
 * </ol>
 */
public final class OffsetLifecycleApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        String groupId = LabConfig.get("groupId", "offset-lifecycle-demo");
        String clientId = LabConfig.get("clientId", "consumer-1");
        String autoOffsetReset = LabConfig.get("autoOffsetReset", "earliest");
        int maxRecords = LabConfig.getInt("maxRecords", 5);

        System.out.println("=== Phase 1: first run -- record offset vs. consumer position vs. committed offset ===");
        Map<TopicPartition, OffsetAndMetadata> lastCommitted = phase1FirstRun(topic, groupId, clientId, autoOffsetReset, maxRecords);

        System.out.println();
        System.out.println("=== Phase 2: restart -- a new consumer instance resumes from the committed offset, replaying the uncommitted tail ===");
        phase2Restart(topic, groupId, clientId + "-restarted", autoOffsetReset, lastCommitted);

        System.out.println();
        System.out.println("=== Phase 3: auto.offset.reset=" + autoOffsetReset + " on a never-before-seen group ===");
        phase3AutoOffsetReset(topic, groupId + "-reset-demo-" + System.currentTimeMillis(), clientId, autoOffsetReset);
    }

    private static Map<TopicPartition, OffsetAndMetadata> phase1FirstRun(
            String topic, String groupId, String clientId, String autoOffsetReset, int maxRecords) {
        try (KafkaConsumer<String, String> consumer = newConsumer(groupId, clientId, autoOffsetReset)) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> batch = pollUntil(consumer, maxRecords);

            if (batch.isEmpty()) {
                System.out.println("No records available -- seed the topic first with runProducer.");
                return Map.of();
            }

            for (ConsumerRecord<String, String> record : batch) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                System.out.printf("  fetched record: partition=%d recordOffset=%d key=%s value=%s%n",
                        record.partition(), record.offset(), record.key(), record.value());
                System.out.printf("    -> consumer position for partition=%d is now %d (next offset to fetch)%n",
                        record.partition(), consumer.position(tp));
            }

            Set<TopicPartition> assigned = consumer.assignment();
            System.out.println("  committed offsets before any commit (expected: none for a fresh group):");
            for (TopicPartition tp : assigned) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                System.out.printf("    partition=%d committed=%s%n", tp.partition(), committed);
            }

            // Deliberately commit only up to the second-to-last record, leaving the
            // last record's partition uncommitted -- this is the gap phase 2 replays.
            ConsumerRecord<String, String> lastRecord = batch.get(batch.size() - 1);
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            for (ConsumerRecord<String, String> record : batch) {
                if (record == lastRecord) {
                    continue;
                }
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                toCommit.merge(tp, new OffsetAndMetadata(record.offset() + 1),
                        (existing, candidate) -> candidate.offset() > existing.offset() ? candidate : existing);
            }
            if (!toCommit.isEmpty()) {
                consumer.commitSync(toCommit);
            }

            System.out.println("  committed offsets after committing everything except the last fetched record:");
            for (TopicPartition tp : assigned) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                long position = consumer.position(tp);
                System.out.printf("    partition=%d committed=%s position=%d%s%n",
                        tp.partition(), committed, position,
                        (committed == null || committed.offset() != position) ? "  <-- position != committed offset" : "");
            }

            return toCommit;
        }
    }

    private static void phase2Restart(
            String topic, String groupId, String clientId, String autoOffsetReset,
            Map<TopicPartition, OffsetAndMetadata> phase1Committed) {
        try (KafkaConsumer<String, String> consumer = newConsumer(groupId, clientId, autoOffsetReset)) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> batch = pollUntil(consumer, 1);

            if (batch.isEmpty()) {
                System.out.println("  No replayed records fetched -- either everything was already committed, or no uncommitted tail existed.");
                return;
            }

            for (ConsumerRecord<String, String> record : batch) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata previouslyCommitted = phase1Committed.get(tp);
                System.out.printf("  replayed record: partition=%d recordOffset=%d key=%s value=%s%n",
                        record.partition(), record.offset(), record.key(), record.value());
                System.out.printf("    this restart resumed at the last committed offset%s -- not at phase 1's in-memory position%n",
                        previouslyCommitted == null ? "" : " (" + previouslyCommitted.offset() + ")");
            }

            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            for (ConsumerRecord<String, String> record : batch) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                toCommit.merge(tp, new OffsetAndMetadata(record.offset() + 1),
                        (existing, candidate) -> candidate.offset() > existing.offset() ? candidate : existing);
            }
            consumer.commitSync(toCommit);
            System.out.println("  committed the replayed tail. This group now has no uncommitted records left.");
        }
    }

    private static void phase3AutoOffsetReset(String topic, String groupId, String clientId, String autoOffsetReset) {
        try (KafkaConsumer<String, String> consumer = newConsumer(groupId, clientId, autoOffsetReset)) {
            consumer.subscribe(List.of(topic));
            // A single short poll is enough to force partition assignment and a
            // reset decision; we only need to observe *where* position lands.
            consumer.poll(Duration.ofSeconds(2));
            Set<TopicPartition> assigned = consumer.assignment();
            if (assigned.isEmpty()) {
                System.out.println("  No partitions assigned yet (rebalance still settling) -- rerun to observe.");
                return;
            }
            for (TopicPartition tp : assigned) {
                long position = consumer.position(tp);
                System.out.printf("  partition=%d starting position=%d (autoOffsetReset=%s)%n",
                        tp.partition(), position, autoOffsetReset);
            }
            System.out.println("  'earliest' starts at each partition's low watermark (full retained history is replayed).");
            System.out.println("  'latest' starts at each partition's high watermark (only records produced from now on are seen).");
        }
    }

    private static List<ConsumerRecord<String, String>> pollUntil(KafkaConsumer<String, String> consumer, int maxRecords) {
        java.util.ArrayList<ConsumerRecord<String, String>> collected = new java.util.ArrayList<>();
        for (int attempt = 0; attempt < 10 && collected.size() < maxRecords; attempt++) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                collected.add(record);
                if (collected.size() >= maxRecords) {
                    break;
                }
            }
        }
        return collected;
    }

    private static KafkaConsumer<String, String> newConsumer(String groupId, String clientId, String autoOffsetReset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }
}
