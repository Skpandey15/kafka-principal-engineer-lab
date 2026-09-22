package com.kafkalab.outbox.naive;

import com.kafkalab.outbox.support.LabConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.TimeZone;

/**
 * The dual-write problem, demonstrated directly: writing to the database
 * and publishing to Kafka are TWO INDEPENDENT operations here, with
 * nothing that makes them atomic. With {@code -PcrashAfterCommit=true},
 * this app throws right after the DB commit and before ever calling the
 * Kafka producer -- simulating a process crash in that exact window --
 * leaving a real, committed business row with NO corresponding Kafka
 * event. See {@link com.kafkalab.outbox.outbox.OutboxWriterApp} for the
 * pattern that fixes this.
 */
public final class NaiveDualWriteApp {

    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        String jdbcUrl = LabConfig.jdbcUrl();
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "naive-order-events");
        String orderId = LabConfig.get("orderId", "O-NAIVE-1");
        String customerId = LabConfig.get("customerId", "C-1");
        String amount = LabConfig.get("amount", "10.00");
        boolean crashAfterCommit = LabConfig.getBoolean("crashAfterCommit", false);

        System.out.println("[naive] writing business row order_id=" + orderId
                + " -- DB write and Kafka publish are two SEPARATE operations, not one atomic unit.");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")) {
            connection.setAutoCommit(true);
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO outbox_demo_orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
                statement.setString(1, orderId);
                statement.setString(2, customerId);
                statement.setBigDecimal(3, new BigDecimal(amount));
                statement.executeUpdate();
            }
        }
        System.out.println("[naive] business row committed to Postgres.");

        if (crashAfterCommit) {
            throw new IllegalStateException("[naive] simulated crash AFTER the DB commit but BEFORE the Kafka publish -- "
                    + "order " + orderId + " now exists in the database with NO corresponding Kafka event. "
                    + "This is the dual-write problem.");
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            String value = "{\"orderId\":\"" + orderId + "\",\"customerId\":\"" + customerId + "\",\"amount\":\"" + amount + "\"}";
            producer.send(new ProducerRecord<>(topic, orderId, value)).get();
            System.out.println("[naive] Kafka event published for " + orderId + " on topic " + topic + ".");
        }
    }
}
