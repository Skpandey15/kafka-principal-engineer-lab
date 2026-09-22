package com.kafkalab.connectcdc.cdc;

import com.kafkalab.connectcdc.support.LabConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * Plain JDBC (no ORM -- this lab's point is Debezium's CDC capture, not a
 * persistence framework) insert/update/delete against the source
 * database's {@code orders} table, to drive real CDC events for the
 * consumer/experiments to observe.
 */
public final class OrdersJdbcSeedApp {

    public static void main(String[] args) throws Exception {
        // See the note below on why this is set BEFORE opening any
        // connection, not passed as a connection property -- the
        // PostgreSQL JDBC driver derives the session TimeZone it sends
        // to the server from the JVM's default TimeZone, not from a
        // Properties key.
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));

        String jdbcUrl = LabConfig.jdbcUrl();
        String action = LabConfig.get("action", "insert");
        String orderId = LabConfig.get("orderId", "O-DEMO-1");
        String customerId = LabConfig.get("customerId", "C-1");
        String amount = LabConfig.get("amount", "10.00");

        Properties props = new Properties();
        props.setProperty("user", LabConfig.get("dbUser", "postgres"));
        props.setProperty("password", LabConfig.get("dbPassword", "postgres"));

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            switch (action) {
                case "insert" -> {
                    try (PreparedStatement statement = connection.prepareStatement(
                            "INSERT INTO orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
                        statement.setString(1, orderId);
                        statement.setString(2, customerId);
                        statement.setBigDecimal(3, new java.math.BigDecimal(amount));
                        int rows = statement.executeUpdate();
                        System.out.printf("INSERT orderId=%s -> %d row(s)%n", orderId, rows);
                    }
                }
                case "update" -> {
                    try (PreparedStatement statement = connection.prepareStatement(
                            "UPDATE orders SET amount = ? WHERE order_id = ?")) {
                        statement.setBigDecimal(1, new java.math.BigDecimal(amount));
                        statement.setString(2, orderId);
                        int rows = statement.executeUpdate();
                        System.out.printf("UPDATE orderId=%s amount=%s -> %d row(s)%n", orderId, amount, rows);
                    }
                }
                case "delete" -> {
                    try (PreparedStatement statement = connection.prepareStatement(
                            "DELETE FROM orders WHERE order_id = ?")) {
                        statement.setString(1, orderId);
                        int rows = statement.executeUpdate();
                        System.out.printf("DELETE orderId=%s -> %d row(s)%n", orderId, rows);
                    }
                }
                default -> throw new IllegalArgumentException("Unknown -Paction: " + action + " (expected insert|update|delete)");
            }
        }
    }
}
