package com.kafkalab.transactions.consumer;

import com.kafkalab.transactions.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Section 9's experiment: a consumer whose {@code isolation.level} is
 * configurable, so the SAME topic can be read twice -- once as
 * {@code Consumer-A} ({@code read_uncommitted}), once as {@code Consumer-B}
 * ({@code read_committed}) -- to compare what each one actually sees
 * against {@link com.kafkalab.transactions.producer.TransactionalProducerApp}'s
 * commit and abort runs.
 *
 * <p>Verified directly against the pinned {@code kafka-clients:4.3.1}
 * {@code ConsumerConfig} defaults: {@code isolation.level} defaults to
 * {@code read_uncommitted} -- an ordinary {@code KafkaConsumer}, with no
 * configuration at all, WILL observe records from a transaction that later
 * aborts, unless {@code read_committed} is set explicitly. This surprises
 * people who assume "transactional producer" alone is enough.
 */
public final class IsolationLevelConsumerApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "txn-lab-orders");
        String isolationLevel = LabConfig.get("isolationLevel", "read_committed");
        String groupId = LabConfig.get("groupId", "txn-lab-isolation-consumer-" + isolationLevel);
        long runForMs = LabConfig.getLong("runForMs", 15000);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        System.out.printf("IsolationLevelConsumerApp | topic=%s | isolation.level=%s | groupId=%s%n", topic, isolationLevel, groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + runForMs;
            int total = 0;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    total++;
                    System.out.printf("isolation.level=%s | partition=%d | offset=%d | key=%s | value=%s%n",
                            isolationLevel, record.partition(), record.offset(), record.key(), record.value());
                }
            }
            System.out.printf("Done. isolation.level=%s consumed %d record(s) from %s in this window.%n", isolationLevel, total, topic);
        }
    }
}
