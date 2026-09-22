package com.kafkalab.retrydlq.consumer;

import com.kafkalab.retrydlq.support.IdempotencyStore;
import com.kafkalab.retrydlq.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * A real consumer loop wired to {@link RetryingRecordProcessor}: for
 * every record, claim/retry/complete-or-DLQ, then always commit the
 * offset (whether the record was processed, skipped as a duplicate, or
 * routed to the DLQ) -- a poison or exhausted-retry record must never
 * block the partition forever, which is the entire point of having a
 * DLQ path at all.
 */
public final class RetryDlqConsumerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String jdbcUrl = LabConfig.jdbcUrl();
        String topic = LabConfig.get("topic", "retry-dlq-orders");
        String dlqTopic = LabConfig.get("dlqTopic", topic + ".DLQ");
        String groupId = LabConfig.get("groupId", "retry-dlq-consumer");
        int maxAttempts = LabConfig.getInt("maxAttempts", 3);
        long runForMs = LabConfig.getInt("runForMs", 20_000);

        // A real, environment-specific finding also hit in WP-11
        // (OrdersJdbcSeedApp): the PostgreSQL JDBC driver sends the
        // JVM's default timezone to the server, and an old zoneinfo
        // alias (e.g. "Asia/Calcutta") is rejected outright.
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
             Connection dbConnection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")) {

            IdempotencyStore store = new IdempotencyStore(dbConnection, groupId);
            RetryingRecordProcessor processor = new RetryingRecordProcessor(
                    store, producer, dlqTopic, maxAttempts, Duration.ofMillis(200), Duration.ofSeconds(30));

            consumer.subscribe(java.util.List.of(topic));
            System.out.println("[consumer] listening on " + topic + " (DLQ: " + dlqTopic + ") for " + runForMs + "ms...");
            java.time.Instant deadline = java.time.Instant.now().plusMillis(runForMs);
            while (java.time.Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    RetryingRecordProcessor.Outcome outcome = processor.process(record, event ->
                            System.out.println("[consumer] applied business logic for " + event.eventId()));
                    System.out.println("[consumer] offset=" + record.offset() + " outcome=" + outcome);
                }
                consumer.commitSync();
            }
        }
    }
}
