package com.kafkalab.consumergroups.consumer;

import com.kafkalab.consumergroups.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One named member of a consumer group. Run several instances of this
 * class, same {@code -PgroupId}, different {@code -PclientId}, to
 * reproduce every experiment in this lab's README: partition ownership
 * and scaling, independent groups, joins, graceful departure, abrupt
 * failure, and rebalance visibility.
 *
 * <p>This class does not re-teach the poll loop or auto-commit -- WP-03's
 * {@code lab-02} {@code ConsumerApp} already does, thoroughly, and this
 * class follows the exact same {@code wakeup()}-based graceful-shutdown
 * pattern. What's new here is the explicit
 * {@link ConsumerRebalanceListener}, which makes every rebalance this
 * consumer participates in visible with a timestamp, rather than
 * something you have to infer from log noise.
 *
 * <h2>Configuration this class deliberately shortens from Kafka's
 * defaults, and why</h2>
 * <ul>
 *   <li>{@code session.timeout.ms}: 10000 (Kafka's own default is 45000).
 *       This is the client-requested upper bound on how long the group
 *       coordinator waits without a heartbeat before considering this
 *       member dead. Shortened so Experiment C (abrupt failure) produces
 *       an observable reassignment within roughly ten seconds instead of
 *       up to forty-five -- a lab-observability adjustment, not a
 *       production recommendation; see the README's production-notes
 *       section for what actually drives this choice in a real
 *       deployment.</li>
 *   <li>{@code heartbeat.interval.ms}: left at Kafka's own default, 3000
 *       -- already well inside the conventional "under 1/3 of the session
 *       timeout" guidance for the shortened value above.</li>
 *   <li>{@code max.poll.interval.ms}: 20000 (Kafka's own default is
 *       300000 / five minutes). Shortened for the same reason: a real
 *       "this consumer is processing too slowly" experiment via
 *       {@code -PprocessingDelayMs} needs to be observable in a lab
 *       session, not require waiting five minutes.</li>
 * </ul>
 * All three defaults were verified directly against the actual
 * {@code kafka-clients:4.3.1} {@code ConsumerConfig} source before being
 * cited here -- not assumed from memory or an older version's
 * documentation.
 */
public final class ConsumerGroupMemberApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        String groupId = LabConfig.get("groupId", "order-processing-service");
        String clientId = LabConfig.get("clientId", "consumer-1");
        long processingDelayMs = Long.parseLong(LabConfig.get("processingDelayMs", "0"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20_000);

        // Static membership (group.instance.id) is intentionally NOT set
        // here -- this lab runs with ordinary dynamic membership by
        // default, which is what makes Experiments A-C (join, graceful
        // departure, failure) trigger a real rebalance every time. See
        // docs/consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md for what
        // changes, and why, if group.instance.id is set instead.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            consumer.wakeup();
        }));

        System.out.printf("Starting %s | group=%s | topic=%s%n", clientId, groupId, topic);
        System.out.println("Press Ctrl+C for a graceful shutdown (sends LeaveGroup -- see the README's");
        System.out.println("graceful-departure vs. abrupt-failure experiment).");

        try {
            consumer.subscribe(List.of(topic), new LoggingRebalanceListener(clientId));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "%s | group=%s | partition=%d | offset=%d | key=%s | value=%s%n",
                            clientId, groupId, record.partition(), record.offset(), record.key(), record.value()
                    );

                    if (processingDelayMs > 0) {
                        // Simulates a slow record / slow downstream
                        // dependency. See the README's "Rebalance impact
                        // experiment" and
                        // docs/consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md's
                        // "Slow consumers, poison records" section for
                        // what this is standing in for, and why a single
                        // slow record can stall every record behind it on
                        // the same partition.
                        try {
                            Thread.sleep(processingDelayMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            if (!shuttingDown.get()) {
                throw e;
            }
            System.out.printf("%s | wakeup() received -- shutting down gracefully.%n", clientId);
        } finally {
            consumer.close();
            System.out.printf("%s | closed.%n", clientId);
        }
    }

    /**
     * Makes every rebalance this consumer participates in visible, with a
     * timestamp, instead of something you have to infer from coordinator
     * log noise. Real production code would also use
     * {@code onPartitionsRevoked}/{@code onPartitionsLost} to commit or
     * checkpoint state before losing a partition -- this lab only logs,
     * deliberately, so the rebalance events themselves stay the focus.
     */
    private record LoggingRebalanceListener(String clientId) implements ConsumerRebalanceListener {

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
            // Distinct from onPartitionsRevoked: this fires when
            // partitions are taken away WITHOUT this consumer having had
            // a chance to react first (e.g., this member was already
            // considered dead by the coordinator before it could
            // gracefully revoke) -- see the README's failure experiment.
            log("PARTITIONS_LOST", partitions);
        }

        private void log(String event, Collection<TopicPartition> partitions) {
            System.out.printf(
                    "%s | %-19s | timestamp=%s | partitions=%s%n",
                    clientId, event, Instant.now(), partitions
            );
        }
    }
}
