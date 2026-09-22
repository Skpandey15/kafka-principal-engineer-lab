package com.kafkalab.outbox.outbox;

import com.kafkalab.outbox.support.LabConfig;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.TimeZone;

/**
 * The transactional outbox pattern's entire premise, in one method: write
 * the business row AND an "outbox" row describing the event to publish,
 * in ONE database transaction. Either both are committed together, or
 * neither is -- there is no window where one exists without the other,
 * unlike {@link com.kafkalab.outbox.naive.NaiveDualWriteApp}. Publishing
 * to Kafka itself is a SEPARATE concern, handled entirely by Debezium's
 * CDC pipeline reading the committed {@code outbox_event} row (see
 * {@link OutboxConnectorRegistrationApp}) -- this app never talks to
 * Kafka directly.
 */
public final class OutboxWriterApp {

    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        String jdbcUrl = LabConfig.jdbcUrl();
        String orderId = LabConfig.get("orderId", "O-OUTBOX-1");
        String customerId = LabConfig.get("customerId", "C-1");
        String amount = LabConfig.get("amount", "10.00");
        String aggregateType = LabConfig.get("aggregateType", "Order");
        String eventType = LabConfig.get("eventType", "OrderCreated");
        boolean failBeforeCommit = LabConfig.getBoolean("failBeforeCommit", false);

        String payload = "{\"orderId\":\"" + orderId + "\",\"customerId\":\"" + customerId + "\",\"amount\":\"" + amount + "\"}";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO outbox_demo_orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
                    statement.setString(1, orderId);
                    statement.setString(2, customerId);
                    statement.setBigDecimal(3, new BigDecimal(amount));
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO outbox_event (aggregatetype, aggregateid, type, payload) VALUES (?, ?, ?, ?::jsonb)")) {
                    statement.setString(1, aggregateType);
                    statement.setString(2, orderId);
                    statement.setString(3, eventType);
                    statement.setString(4, payload);
                    statement.executeUpdate();
                }

                if (failBeforeCommit) {
                    throw new IllegalStateException("[outbox] simulated failure BEFORE commit -- "
                            + "both inserts are staged but neither is durable yet.");
                }

                connection.commit();
                System.out.println("[outbox] business row and outbox row for " + orderId
                        + " committed together, in one transaction. No direct Kafka call was made -- "
                        + "Debezium's CDC pipeline will pick up the outbox_event row from here.");
            } catch (Exception e) {
                connection.rollback();
                System.out.println("[outbox] transaction rolled back -- neither the business row nor the outbox row for "
                        + orderId + " exists. Reason: " + e.getMessage());
                throw e;
            }
        }
    }
}
