package com.kafkalab.deliverysemantics.consumer;

import com.kafkalab.deliverysemantics.support.FailurePoint;
import com.kafkalab.deliverysemantics.support.LabConfig;
import com.kafkalab.deliverysemantics.support.OrderEvent;
import com.kafkalab.deliverysemantics.support.ProcessedEventStore;
import com.kafkalab.deliverysemantics.support.SimulatedCrashException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The single, configurable poll -&gt; process -&gt; commit loop that drives
 * nearly every experiment in the WP-06 spec: at-most-once and
 * at-least-once failure windows, {@code commitSync} vs. {@code commitAsync},
 * auto-commit vs. manual commit, batch-commit boundaries, per-partition
 * offsets, idempotent processing, and rebalance-vs-commit interaction.
 *
 * <p>One configurable loop rather than eight near-duplicate classes,
 * because these concerns genuinely interact within one real consumer --
 * see the lab README, "Why one app instead of many," for the full
 * rationale.
 *
 * <h2>The loop, per record</h2>
 * <pre>
 * if commitTiming == BEFORE_PROCESS:
 *     commit(offset + 1)
 *     maybeCrash(AFTER_COMMIT_BEFORE_PROCESS)
 * maybeCrash(BEFORE_PROCESS)
 * if idempotent and already processed:
 *     status = DUPLICATE_SKIPPED
 * else if this eventId is configured to fail:
 *     status = FAILED; halt this batch
 * else:
 *     apply the (simulated) business side effect
 *     status = SUCCESS
 * maybeCrash(AFTER_PROCESS_BEFORE_COMMIT)
 * if commitTiming == AFTER_PROCESS:
 *     commit(offset + 1)
 * else if commitTiming == AFTER_BATCH:
 *     remember offset + 1 for a single commit after the whole poll batch
 * </pre>
 *
 * <p>{@link #run} is the reusable core -- both {@link #main} (the CLI
 * entry point, which catches {@link SimulatedCrashException} at the top
 * level and exits abruptly, exactly like a real crash would) and this
 * lab's integration tests (which catch it directly and construct a fresh
 * {@code KafkaConsumer} to simulate a deterministic, in-process restart)
 * call it directly, so every experiment is reproducible on demand
 * instead of depending on randomly killing a process.
 */
public final class DeliverySemanticsApp {

    public enum CommitTiming { BEFORE_PROCESS, AFTER_PROCESS, AFTER_BATCH }

    public enum CommitMode { SYNC, ASYNC }

    public record Config(
            String bootstrapServers,
            String topic,
            String groupId,
            String clientId,
            CommitTiming commitTiming,
            CommitMode commitMode,
            FailurePoint failurePoint,
            String crashAtEventId,
            String failAtEventId,
            boolean idempotent,
            long processingDelayMs,
            int maxRecords,
            boolean autoCommit) {

        public static Config fromSystemProperties() {
            return new Config(
                    LabConfig.bootstrapServers(),
                    LabConfig.topic(),
                    LabConfig.get("groupId", "delivery-semantics-demo"),
                    LabConfig.get("clientId", "consumer-1"),
                    CommitTiming.valueOf(LabConfig.get("commitTiming", "AFTER_PROCESS")),
                    CommitMode.valueOf(LabConfig.get("commitMode", "SYNC")),
                    FailurePoint.valueOf(LabConfig.get("failurePoint", "NONE")),
                    LabConfig.get("crashAtEventId", ""),
                    LabConfig.get("failAtEventId", ""),
                    LabConfig.getBoolean("idempotent", false),
                    LabConfig.getLong("processingDelayMs", 0),
                    LabConfig.getInt("maxRecords", 0),
                    LabConfig.getBoolean("autoCommit", false));
        }
    }

    public static void main(String[] args) {
        Config config = Config.fromSystemProperties();
        KafkaConsumer<String, String> consumer = newConsumer(config);
        ProcessedEventStore store = config.idempotent() ? ProcessedEventStore.forGroup(config.groupId()) : null;
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            consumer.wakeup();
        }));

        System.out.printf("Starting %s | group=%s | topic=%s | commitTiming=%s | commitMode=%s | failurePoint=%s | autoCommit=%s | idempotent=%s%n",
                config.clientId(), config.groupId(), config.topic(), config.commitTiming(), config.commitMode(),
                config.failurePoint(), config.autoCommit(), config.idempotent());
        System.out.println("Press Ctrl+C for a graceful shutdown. Configure -PfailurePoint/-PcrashAtEventId for a deterministic simulated crash instead.");

        try {
            run(config, consumer, store, shuttingDown);
            consumer.close();
            System.out.printf("%s | closed.%n", config.clientId());
        } catch (SimulatedCrashException e) {
            System.out.printf("%s | SIMULATED_CRASH | %s%n", config.clientId(), e.getMessage());
            System.out.printf("%s | exiting abruptly WITHOUT consumer.close() -- no LeaveGroup is sent, and any offsets not already committed above are lost from the group's perspective.%n", config.clientId());
            System.exit(1);
        } catch (WakeupException e) {
            if (!shuttingDown.get()) {
                throw e;
            }
            System.out.printf("%s | wakeup() received -- shutting down gracefully.%n", config.clientId());
            consumer.close();
        }
    }

    /**
     * The reusable core loop. Runs until {@code shuttingDown} is set, a
     * configured {@link FailurePoint} throws a {@link SimulatedCrashException},
     * a configured business failure halts a batch, or (when
     * {@code maxRecords > 0}) that many records have been successfully
     * handled -- whichever comes first.
     *
     * <p>Note on the "attempt" field this loop logs: it counts how many
     * times THIS process has seen a given eventId during THIS run. It is
     * an in-memory readability aid, not a durable cross-restart counter --
     * a genuine cross-restart duplicate is what {@code status=DUPLICATE_SKIPPED}
     * reports, backed by {@link ProcessedEventStore}, which IS durable.
     */
    public static void run(Config config, KafkaConsumer<String, String> consumer, ProcessedEventStore store, AtomicBoolean shuttingDown) {
        Map<String, Integer> attemptCounts = new HashMap<>();
        consumer.subscribe(List.of(config.topic()), new CommitOnRevokeListener(config, consumer));

        int handled = 0;
        outer:
        while (!shuttingDown.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                continue;
            }

            Map<TopicPartition, OffsetAndMetadata> batchOffsets = new LinkedHashMap<>();
            boolean batchFailed = false;

            for (ConsumerRecord<String, String> record : records) {
                OrderEvent event = OrderEvent.parse(record.value());
                String eventId = event.eventId();
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                int attempt = attemptCounts.merge(eventId, 1, Integer::sum);

                if (config.commitTiming() == CommitTiming.BEFORE_PROCESS && !config.autoCommit()) {
                    commitOne(consumer, config, tp, record.offset() + 1, eventId);
                    maybeCrash(config, FailurePoint.AFTER_COMMIT_BEFORE_PROCESS, eventId);
                }
                maybeCrash(config, FailurePoint.BEFORE_PROCESS, eventId);

                boolean duplicate = store != null && store.isProcessed(eventId);
                if (duplicate) {
                    log(config, record, eventId, attempt, "DUPLICATE_SKIPPED");
                } else if (eventId.equals(config.failAtEventId())) {
                    log(config, record, eventId, attempt, "FAILED");
                    System.out.printf("%s | BATCH_HALTED | processing failure at eventId=%s stopped this poll batch before its remaining records were handled.%n",
                            config.clientId(), eventId);
                    batchFailed = true;
                    break;
                } else {
                    if (config.processingDelayMs() > 0) {
                        try {
                            Thread.sleep(config.processingDelayMs());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break outer;
                        }
                    }
                    if (store != null) {
                        store.markProcessed(eventId);
                    }
                    log(config, record, eventId, attempt, "SUCCESS");
                    handled++;
                }

                maybeCrash(config, FailurePoint.AFTER_PROCESS_BEFORE_COMMIT, eventId);

                if (config.commitTiming() == CommitTiming.AFTER_PROCESS && !config.autoCommit()) {
                    // Committed even on a duplicate: a skip is still a
                    // handled outcome. Without this, offset never advances
                    // past a duplicate and the same record is refetched
                    // forever instead of being skipped once and moved past.
                    commitOne(consumer, config, tp, record.offset() + 1, eventId);
                } else if (config.commitTiming() == CommitTiming.AFTER_BATCH && !config.autoCommit()) {
                    batchOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
                }

                if (config.maxRecords() > 0 && handled >= config.maxRecords()) {
                    if (config.commitTiming() == CommitTiming.AFTER_BATCH && !config.autoCommit() && !batchOffsets.isEmpty()) {
                        commitBatch(consumer, config, batchOffsets);
                    }
                    break outer;
                }
            }

            if (config.commitTiming() == CommitTiming.AFTER_BATCH && !config.autoCommit()) {
                if (batchFailed) {
                    System.out.printf("%s | BATCH_NOT_COMMITTED | whole-batch-commit semantics: because this batch failed partway through, none of its offsets were committed -- even the records that succeeded before the failure will be reprocessed on restart.%n",
                            config.clientId());
                } else if (!batchOffsets.isEmpty()) {
                    commitBatch(consumer, config, batchOffsets);
                }
            }

            if (batchFailed) {
                break;
            }
        }
    }

    private static void maybeCrash(Config config, FailurePoint point, String eventId) {
        boolean eventMatches = config.crashAtEventId().isEmpty() || config.crashAtEventId().equals(eventId);
        if (config.failurePoint() == point && eventMatches) {
            throw new SimulatedCrashException(point, eventId);
        }
    }

    private static void commitOne(KafkaConsumer<String, String> consumer, Config config, TopicPartition tp, long offset, String eventId) {
        commit(consumer, config, Map.of(tp, new OffsetAndMetadata(offset)), eventId);
    }

    private static void commitBatch(KafkaConsumer<String, String> consumer, Config config, Map<TopicPartition, OffsetAndMetadata> offsets) {
        commit(consumer, config, offsets, "(batch of " + offsets.size() + " partitions)");
    }

    private static void commit(KafkaConsumer<String, String> consumer, Config config, Map<TopicPartition, OffsetAndMetadata> offsets, String eventId) {
        if (config.commitMode() == CommitMode.SYNC) {
            consumer.commitSync(offsets);
            logCommit(config, offsets, eventId, "commitSync", "COMMITTED");
        } else {
            logCommit(config, offsets, eventId, "commitAsync", "SUBMITTED");
            consumer.commitAsync(offsets, (committed, exception) -> {
                if (exception != null) {
                    System.out.printf("%s | eventId=%s | commitAsync -> COMMIT_FAILED: %s%n", config.clientId(), eventId, exception);
                } else {
                    logCommit(config, committed, eventId, "commitAsync", "COMMITTED");
                }
            });
        }
    }

    private static void logCommit(Config config, Map<TopicPartition, OffsetAndMetadata> offsets, String eventId, String method, String status) {
        offsets.forEach((tp, oam) -> System.out.printf(
                "%s | partition=%d | eventId=%s | %s -> %s committedOffset=%d%n",
                config.clientId(), tp.partition(), eventId, method, status, oam.offset()));
    }

    private static void log(Config config, ConsumerRecord<String, String> record, String eventId, int attempt, String status) {
        System.out.printf(
                "%s | partition=%d | offset=%d | eventId=%s | attempt=%d | status=%s%n",
                config.clientId(), record.partition(), record.offset(), eventId, attempt, status);
    }

    private static KafkaConsumer<String, String> newConsumer(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.clientId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.autoCommit());
        // Shortened from Kafka's defaults (45000 / 300000) for the same
        // lab-observability reason lab-04's ConsumerGroupMemberApp shortens
        // them: the rebalance-vs-commit experiment (README Experiment F)
        // needs a reassignment to become visible within seconds, not
        // minutes. Not a production recommendation on its own.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20_000);
        return new KafkaConsumer<>(props);
    }

    /**
     * Ties this lab's commit strategy back to WP-05's rebalance mechanics
     * by making every rebalance this consumer participates in visible.
     *
     * <p><b>This class deliberately does NOT commit anything on revoke,
     * and an earlier version of this lab that did was wrong in a way
     * worth keeping on record.</b> The tempting design is "commit
     * {@code consumer.position(tp)} synchronously in
     * {@code onPartitionsRevoked}, so in-flight progress isn't silently
     * handed to the next owner." That is wrong here because
     * {@code consumer.position(tp)} reflects how far the client has
     * *fetched* into a partition, not how far this application has
     * decided it is *safe to commit* -- and those two numbers are not the
     * same thing the moment {@code commitTiming=AFTER_BATCH} is combined
     * with a mid-batch failure (or, more subtly, whenever a single
     * {@code poll()} call returns more records than {@code maxRecords}
     * allows this loop to actually handle before stopping). Committing
     * blindly on revoke can advance the group's committed offset PAST
     * records this run's own batch logic explicitly decided to withhold
     * -- silently erasing the exact at-least-once safety this lab's
     * {@code AFTER_BATCH} experiment exists to demonstrate.
     *
     * <p>This was not a theoretical concern: building this lab's batch-
     * boundary experiment, exactly that happened. A consumer processing
     * offsets 10-14 failed at offset 13 and correctly declined to commit
     * anything past offset 10 from its own loop -- but {@code poll()} had
     * already fetched all 5 records into that single call, so
     * {@code position()} was already 15. The old {@code onPartitionsRevoked}
     * commit fired on {@code consumer.close()} and committed offset 15
     * anyway, silently "consuming" the very 3 records (11, 12, and the
     * intentionally-uncommitted 13) the batch-boundary experiment exists
     * to preserve for replay. Real evidence: the next consumer joining
     * that group found nothing left to fetch and blocked in
     * {@code poll()} indefinitely -- confirmed with a thread dump showing
     * the main thread parked in {@code ClassicKafkaConsumer.poll}.
     *
     * <p>The deeper lesson (see the README's rebalance section): for
     * this lab's synchronous, single-threaded poll-process-commit loop,
     * a rebalance callback can only ever fire at the top of the next
     * {@code poll()} call -- by which point the previous batch's commit
     * decision has already been fully made by the loop itself. There is
     * no genuine "processed but not yet committed, and about to be lost
     * to a revoke" window to rescue in this design. That window is real
     * for architectures where record processing happens on a separate
     * thread from the poll loop (so a revoke can land mid-processing) --
     * this lab does not use that design, and bolting on a revoke-time
     * commit anyway only reintroduced the exact bug above.
     */
    private record CommitOnRevokeListener(Config config, KafkaConsumer<String, String> consumer) implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log("PARTITIONS_REVOKED", partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log("PARTITIONS_ASSIGNED", partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            // Unlike onPartitionsRevoked, these partitions are already
            // gone -- this consumer was considered dead before it could
            // react. Committing here would race the new owner and is not
            // attempted.
            log("PARTITIONS_LOST", partitions);
        }

        private void log(String eventName, Collection<TopicPartition> partitions) {
            System.out.printf("%s | %-19s | timestamp=%s | partitions=%s%n",
                    config.clientId(), eventName, Instant.now(), partitions);
        }
    }
}
