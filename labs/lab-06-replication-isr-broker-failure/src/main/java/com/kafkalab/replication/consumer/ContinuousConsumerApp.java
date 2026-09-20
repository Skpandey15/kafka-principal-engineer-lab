package com.kafkalab.replication.consumer;

import com.kafkalab.replication.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes continuously so that killing and recovering the current
 * partition leader mid-run (README Experiment 14) shows real consumer
 * behavior: a brief interruption while the client's fetch to the dead
 * leader times out and metadata refreshes to the new leader, then
 * consumption resuming -- entirely independent of consumer-group
 * rebalancing, which this app deliberately runs alone (a group of one)
 * so a broker-leader failure is never confused with a
 * {@code PARTITIONS_REVOKED}/{@code PARTITIONS_ASSIGNED} rebalance event.
 * See the README's Experiment 14 for the explicit distinction between the
 * two mechanisms.
 */
public final class ContinuousConsumerApp {

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        String groupId = LabConfig.get("groupId", "replication-lab-consumer");
        String clientId = LabConfig.get("clientId", "consumer-1");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            consumer.wakeup();
        }));

        System.out.printf("Starting %s | group=%s | topic=%s%n", clientId, groupId, topic);
        System.out.println("Press Ctrl+C for a graceful shutdown. Kill/restart a broker in another terminal while this runs.");

        try {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("%s | %s | partition=%d offset=%d key=%s value=%s%n",
                            Instant.now(), clientId, record.partition(), record.offset(), record.key(), record.value());
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
}
