package com.kafkalab.nativeclient.consumer;

import com.kafkalab.nativeclient.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Experiment: the native consumer poll loop — subscribe, poll, process,
 * (auto-)commit — plus graceful shutdown via {@code wakeup()}.
 *
 * <p>Run several instances of this class with different {@code -PgroupId}
 * and {@code -PclientId} values (see the lab README's consumer-group
 * experiment) to observe partition ownership across a group, independent
 * consumption across groups, and replay behavior when a new group starts
 * with no committed offsets.
 *
 * <p>This class uses auto-commit, deliberately, to keep the first consumer
 * in this lab focused on the poll loop itself rather than manual offset
 * management. Read the comments in the poll loop below (and the lab
 * README's "Auto-commit" section) before assuming auto-commit means
 * anything stronger than what it actually does.
 */
public final class ConsumerApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        String groupId = LabConfig.get("groupId", "java-orders-group");
        String clientId = LabConfig.get("clientId", "consumer-a");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // auto.offset.reset only matters the FIRST time this group.id has
        // no committed offset for a partition (a brand-new group, or one
        // whose committed offsets have expired/been reset). "earliest"
        // makes every new group id you try in this lab replay the topic
        // from the start, which is what makes the replay experiment
        // reproducible on demand -- it does NOT mean "always start from
        // the beginning"; an EXISTING group with a committed offset
        // resumes from that committed offset regardless of this setting.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // enable.auto.commit defaults to true; set explicitly here so the
        // choice is visible rather than implicit. See the poll loop below
        // for exactly what this does and does not guarantee.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        // The native graceful-shutdown pattern for a Kafka consumer: call
        // wakeup() from another thread (here, the JVM shutdown hook fired
        // by Ctrl+C / SIGTERM). wakeup() causes the consumer's blocking
        // poll() call to throw WakeupException instead of blocking
        // indefinitely -- it is the only KafkaConsumer method that is safe
        // to call from a different thread than the one driving the poll
        // loop. Simply killing the process instead skips this: any record
        // this loop had already fetched and started processing, but not
        // yet committed, is abandoned mid-flight with no chance to commit
        // or log where it stopped.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            consumer.wakeup();
        }));

        System.out.printf("Starting consumer: clientId=%s groupId=%s topic=%s%n", clientId, groupId, topic);
        System.out.println("Press Ctrl+C for a graceful shutdown.");
        System.out.println();

        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                // poll() is not merely "fetch records." While blocked in
                // (or between) calls to poll(), the consumer also drives
                // group membership: joining/rebalancing when needed, and
                // -- depending on the active consumer-group protocol --
                // either sending heartbeats itself or relying on a
                // background thread to do so. Calling poll() too
                // infrequently (slow per-record processing in this loop)
                // is exactly the failure scenario documented in
                // docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md under
                // "Consumer processing takes longer than the configured
                // poll/session liveness allowance" -- this lab does not
                // reproduce that failure yet; WP-05 owns it in depth.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    // record.offset() is this ONE record's fixed position
                    // in its partition -- it never changes. It is not the
                    // same thing as the consumer's current position (the
                    // next offset it will fetch) or the group's committed
                    // offset (what the broker remembers as "done" for this
                    // group) -- see docs/architecture/KAFKA_MENTAL_MODEL.md
                    // for why conflating these is a real source of bugs.
                    System.out.printf(
                            "clientId=%s topic=%s partition=%d offset=%d key=%s value=%s timestamp=%d%n",
                            clientId, record.topic(), record.partition(), record.offset(),
                            record.key(), record.value(), record.timestamp()
                    );

                    // This line is the application processing step. In
                    // this lab it is just a print statement, but in a real
                    // consumer this is where a business side effect (a
                    // database write, an email, a charge) would happen --
                    // and, per the mental model, that side effect
                    // completing is a DIFFERENT event from this record's
                    // offset later being committed. Auto-commit below does
                    // not know or care whether this line succeeded.
                }

                // With auto-commit enabled, the client periodically commits
                // the current position on your behalf, on its own
                // schedule (auto.commit.interval.ms), NOT immediately
                // after each poll() batch finishes processing. That means:
                // (a) a crash between "processed" and "the next scheduled
                // auto-commit" replays this batch on restart, and (b) even
                // without a crash, auto-commit is not synchronized with
                // your processing loop's success or failure at all -- it
                // does not know whether the loop above threw partway
                // through a batch. "Auto-commit commits after successful
                // processing" is not an accurate description of what this
                // does. Manual, deliberate offset commits (tied explicitly
                // to confirmed processing) are a later-lab topic.
            }
        } catch (WakeupException e) {
            if (!shuttingDown.get()) {
                throw e; // a real wakeup() we didn't ask for -- don't swallow it
            }
            System.out.println();
            System.out.println("wakeup() received -- shutting down gracefully.");
        } finally {
            printCommittedOffsets(consumer, topic, groupId);
            consumer.close();
            System.out.println("Consumer closed.");
        }
    }

    private static void printCommittedOffsets(KafkaConsumer<String, String> consumer, String topic, String groupId) {
        try {
            Set<TopicPartition> assigned = consumer.assignment();
            Map<TopicPartition, OffsetAndMetadata> committedByPartition = consumer.committed(assigned);
            for (TopicPartition partition : assigned) {
                OffsetAndMetadata committed = committedByPartition.get(partition);
                long committedOffset = committed == null ? -1 : committed.offset();
                long position = consumer.position(partition);
                System.out.printf(
                        "group=%s partition=%s committedOffset=%d currentPosition=%d%n",
                        groupId, partition, committedOffset, position
                );
            }
        } catch (RuntimeException e) {
            // Best-effort teaching output only -- if the consumer already
            // lost its assignment during shutdown, that's fine; this
            // summary is not load-bearing for the experiment itself.
            System.out.println("(could not read final offsets during shutdown: " + e.getMessage() + ")");
        }
    }
}
