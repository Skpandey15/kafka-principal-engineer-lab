package com.kafkalab.partitioning.consumer;

import com.kafkalab.partitioning.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Experiment: partition count as a ceiling on useful consumer-group
 * parallelism.
 *
 * <p>Run several instances of this class, same {@code groupId}, different
 * {@code clientId}, against a topic with fewer partitions than instances
 * (see the README's Setup for the exact numbers this experiment expects).
 * Every instance prints its own partition assignment on startup and after
 * any rebalance -- watch how many instances end up with an EMPTY
 * assignment.
 *
 * <p>This class deliberately does not re-teach the poll loop, auto-commit,
 * or graceful shutdown in depth -- lab-02's {@code ConsumerApp}
 * (WP-03) already does, thoroughly, and this class follows the exact same
 * {@code wakeup()}-based shutdown pattern. What's new here is watching
 * {@code consumer.assignment()} directly, which is the fact this
 * experiment is actually about. Consumer-group rebalance mechanics
 * themselves -- exactly how and when that assignment is decided -- are
 * WP-05's job, not this one; see
 * docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md.
 */
public final class ConsumerParallelismApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        String groupId = LabConfig.get("groupId", "partitioning-lab-group");
        String clientId = LabConfig.get("clientId", "consumer-a");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            consumer.wakeup();
        }));

        System.out.printf("Starting consumer: clientId=%s groupId=%s topic=%s%n", clientId, groupId, topic);
        System.out.println("Press Ctrl+C for a graceful shutdown.");

        int lastPrintedAssignmentSize = -1;
        try {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                int assignmentSize = consumer.assignment().size();
                if (assignmentSize != lastPrintedAssignmentSize) {
                    if (assignmentSize == 0) {
                        System.out.printf("clientId=%s: assignment is EMPTY -- idle, no partitions owned.%n", clientId);
                    } else {
                        System.out.printf("clientId=%s: assigned partitions %s%n", clientId, consumer.assignment());
                    }
                    lastPrintedAssignmentSize = assignmentSize;
                }

                for (var record : records) {
                    System.out.printf(
                            "clientId=%s partition=%d offset=%d key=%s%n",
                            clientId, record.partition(), record.offset(), record.key()
                    );
                }
            }
        } catch (WakeupException e) {
            if (!shuttingDown.get()) {
                throw e;
            }
            System.out.println("wakeup() received -- shutting down gracefully.");
        } finally {
            consumer.close();
            System.out.println("Consumer closed.");
        }
    }
}
