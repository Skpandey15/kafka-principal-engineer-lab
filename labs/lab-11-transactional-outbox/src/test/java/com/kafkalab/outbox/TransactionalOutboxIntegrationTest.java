package com.kafkalab.outbox;

import com.kafkalab.outbox.support.ConnectRestClient;
import com.kafkalab.outbox.support.OutboxTestCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kafka + real PostgreSQL + a real Kafka Connect distributed-mode
 * worker running Debezium's PostgreSQL connector with the real
 * {@code io.debezium.transforms.outbox.EventRouter} single message
 * transform -- no mocked connector, no mocked transform, no simulated
 * envelope.
 *
 * <p>Every assertion is reached through a bounded condition-polling loop
 * with an overall deadline, per this repository's established test
 * convention.
 */
class TransactionalOutboxIntegrationTest {

    private static OutboxTestCluster cluster;
    private static ConnectRestClient connect;

    @BeforeAll
    static void startCluster() {
        cluster = new OutboxTestCluster(19_701);
        cluster.start();
        connect = new ConnectRestClient(cluster.connectUrl());
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Test
    void naiveDualWriteLosesTheKafkaEventWhenTheProcessCrashesAfterTheDbCommit() throws Exception {
        // Models NaiveDualWriteApp's -PcrashAfterCommit=true path directly:
        // the DB write commits on its own, and the process is treated as
        // having crashed before it ever creates a Kafka producer -- the
        // two operations were never atomic to begin with.
        String orderId = "O-NAIVE-" + uniqueId();
        String naiveTopic = "naive-order-events-" + uniqueId();

        insertBusinessRowOnly(orderId, "C-1", "10.00");

        assertTrue(businessRowExists(orderId), "the business row must exist -- its own commit succeeded independently");

        List<RawRecord> events = pollRawRecords(naiveTopic, Duration.ofSeconds(5));
        assertTrue(events.isEmpty(),
                "no Kafka event should exist for " + orderId + " -- the app never even attempted to publish one. "
                        + "This is the dual-write problem: a committed business row with no corresponding event.");
    }

    @Test
    void outboxRowAndBusinessRowCommitOrRollbackTogetherInOneTransaction() throws Exception {
        String failedOrderId = "O-ATOMIC-FAIL-" + uniqueId();
        assertTrue(writeOutboxTransaction(failedOrderId, "C-1", "10.00", "Order", "OrderCreated", true),
                "rollback should have been triggered");
        assertFalse(businessRowExists(failedOrderId), "business row must NOT exist after rollback");
        assertFalse(outboxRowExists(failedOrderId), "outbox row must NOT exist after rollback");

        String okOrderId = "O-ATOMIC-OK-" + uniqueId();
        writeOutboxTransaction(okOrderId, "C-1", "10.00", "Order", "OrderCreated", false);
        assertTrue(businessRowExists(okOrderId), "business row must exist after a successful commit");
        assertTrue(outboxRowExists(okOrderId), "outbox row must exist after a successful commit");
    }

    @Test
    void debeziumRoutesOutboxEventsToATopicNamedByAggregateTypeWithTheOriginalPayload() throws Exception {
        String id = uniqueId();
        registerOutboxConnector("dbz-outbox-route-" + id, "outboxcdc-route-" + id, "slot_route_" + id.replace("-", "_"), "no_data");

        String orderId = "O-ROUTE-" + id;
        String customerAggId = "C-ROUTE-" + id;
        writeOutboxTransaction(orderId, "C-1", "42.50", "Order", "OrderCreated", false);
        writeOutboxTransaction(customerAggId, customerAggId, "0.00", "Customer", "CustomerTouched", false);

        List<RawRecord> orderEvents = pollUntilMatching("outbox.event.Order", r -> containsValue(r, orderId), 1, Duration.ofSeconds(25));
        List<RawRecord> customerEvents = pollUntilMatching("outbox.event.Customer", r -> containsValue(r, customerAggId), 1, Duration.ofSeconds(25));

        // Parsed structurally, not by raw substring -- a real finding
        // building this test: PostgreSQL's jsonb text serialization
        // inserts a space after every ':' and ',' (`::jsonb` round-trips
        // `{"a":"b"}` as `{"a": "b"}`), so a naive `"orderId":"..."`
        // substring check against the routed payload fails even though
        // the field and value are both present and correct.
        com.fasterxml.jackson.databind.JsonNode orderPayload = parsePayload(orderEvents.get(0));
        assertEquals(orderId, orderPayload.path("orderId").asText());
        assertEquals("42.50", orderPayload.path("amount").asText());
        com.fasterxml.jackson.databind.JsonNode customerPayload = parsePayload(customerEvents.get(0));
        assertEquals(customerAggId, customerPayload.path("orderId").asText());
    }

    private static com.fasterxml.jackson.databind.JsonNode parsePayload(RawRecord record) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper().readTree(payloadValue(record));
    }

    @Test
    void outboxEventIsCapturedEvenWhenTheWritingProcessNeverTalksToKafka() throws Exception {
        // The core value proposition of CDC-based outbox publishing: an
        // outbox row committed by a process that crashes immediately
        // afterward (and NEVER creates a Kafka producer, never even
        // attempts to publish) still gets published -- because
        // publication is driven by Debezium reading the WAL, entirely
        // independent of the writing process's own liveness. Modeled
        // here by writing the row and registering the connector
        // AFTERWARD, with snapshot.mode=initial so the already-committed
        // row gets picked up.
        String id = uniqueId();
        String orderId = "O-DECOUPLED-" + id;

        try (Connection connection = newJdbcConnection(); var statement = connection.createStatement()) {
            statement.execute("DELETE FROM outbox_event");
        }
        writeOutboxTransaction(orderId, "C-1", "77.00", "Order", "OrderCreated", false);

        registerOutboxConnector("dbz-outbox-decoupled-" + id, "outboxcdc-decoupled-" + id, "slot_decoupled_" + id.replace("-", "_"), "initial");

        List<RawRecord> events = pollUntilMatching("outbox.event.Order", r -> containsValue(r, orderId), 1, Duration.ofSeconds(30));
        assertTrue(payloadValue(events.get(0)).contains(orderId),
                "the outbox row committed BEFORE the connector even existed must still be captured and routed");
    }

    @Test
    void deletingAnAlreadyCapturedOutboxRowProducesNoFurtherEventOnTheRoutedTopic() throws Exception {
        // A real outbox-pattern operational concern: the outbox table
        // needs periodic cleanup (it otherwise grows unbounded), but
        // deleting an already-captured row must NOT produce a duplicate
        // or tombstone event on the routed topic -- tombstones.on.delete
        // is explicitly set to false on this connector for exactly this
        // reason.
        String id = uniqueId();
        registerOutboxConnector("dbz-outbox-cleanup-" + id, "outboxcdc-cleanup-" + id, "slot_cleanup_" + id.replace("-", "_"), "no_data");

        String orderId = "O-CLEANUP-" + id;
        writeOutboxTransaction(orderId, "C-1", "5.00", "Order", "OrderCreated", false);
        pollUntilMatching("outbox.event.Order", r -> containsValue(r, orderId), 1, Duration.ofSeconds(25));

        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement("DELETE FROM outbox_event WHERE aggregateid = ?")) {
            statement.setString(1, orderId);
            statement.executeUpdate();
        }

        // Re-poll from the beginning after a bounded wait: the total
        // count of records matching this orderId must STILL be exactly
        // 1 -- the DELETE produced nothing new.
        Thread.sleep(5000);
        List<RawRecord> all = pollRawRecordsMatching("outbox.event.Order", r -> containsValue(r, orderId), Duration.ofSeconds(10));
        assertEquals(1, all.size(), "deleting an already-captured outbox row must not produce a second event: " + all);
    }

    @Test
    void outboxDemoOrdersTableChangesAreNeverCapturedOnlyOutboxEventIs() throws Exception {
        String id = uniqueId();
        registerOutboxConnector("dbz-outbox-notcaptured-" + id, "outboxcdc-notcaptured-" + id, "slot_notcaptured_" + id.replace("-", "_"), "no_data");

        String orderId = "O-BUSINESSONLY-" + id;
        insertBusinessRowOnly(orderId, "C-1", "9.99");

        List<RawRecord> events = pollRawRecordsMatching("outbox.event.Order", r -> containsValue(r, orderId), Duration.ofSeconds(8));
        assertTrue(events.isEmpty(),
                "a write to outbox_demo_orders alone (bypassing the outbox table entirely) must never appear on any outbox.event.* topic");
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static Connection newJdbcConnection() throws Exception {
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        return DriverManager.getConnection(cluster.jdbcUrl(), props);
    }

    private static void insertBusinessRowOnly(String orderId, String customerId, String amount) throws Exception {
        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO outbox_demo_orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
            statement.setString(1, orderId);
            statement.setString(2, customerId);
            statement.setBigDecimal(3, new BigDecimal(amount));
            statement.executeUpdate();
        }
    }

    /** Writes the business row AND the outbox row in ONE transaction; if forceFailure, throws before commit and rolls back. Returns true if a rollback happened. */
    private static boolean writeOutboxTransaction(String aggregateId, String customerId, String amount,
                                                   String aggregateType, String eventType, boolean forceFailure) throws Exception {
        String payload = "{\"orderId\":\"" + aggregateId + "\",\"customerId\":\"" + customerId + "\",\"amount\":\"" + amount + "\"}";
        try (Connection connection = newJdbcConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO outbox_demo_orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
                    statement.setString(1, aggregateId);
                    statement.setString(2, customerId);
                    statement.setBigDecimal(3, new BigDecimal(amount));
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO outbox_event (aggregatetype, aggregateid, type, payload) VALUES (?, ?, ?, ?::jsonb)")) {
                    statement.setString(1, aggregateType);
                    statement.setString(2, aggregateId);
                    statement.setString(3, eventType);
                    statement.setString(4, payload);
                    statement.executeUpdate();
                }
                if (forceFailure) {
                    throw new IllegalStateException("forced failure before commit");
                }
                connection.commit();
                return false;
            } catch (Exception e) {
                connection.rollback();
                return true;
            }
        }
    }

    private static boolean businessRowExists(String orderId) throws Exception {
        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM outbox_demo_orders WHERE order_id = ?")) {
            statement.setString(1, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static boolean outboxRowExists(String aggregateId) throws Exception {
        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM outbox_event WHERE aggregateid = ?")) {
            statement.setString(1, aggregateId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static void registerOutboxConnector(String connectorName, String topicPrefix, String slotName, String snapshotMode) throws Exception {
        connect.registerConnector(connectorName, """
                {"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
                 "database.hostname":"it-postgres","database.port":"5432",
                 "database.user":"postgres","database.password":"postgres","database.dbname":"inventory",
                 "topic.prefix":"%s","table.include.list":"public.outbox_event",
                 "publication.name":"dbz_outbox_publication","publication.autocreate.mode":"disabled",
                 "slot.name":"%s","plugin.name":"pgoutput","snapshot.mode":"%s",
                 "tombstones.on.delete":"false",
                 "transforms":"outbox",
                 "transforms.outbox.type":"io.debezium.transforms.outbox.EventRouter",
                 "transforms.outbox.route.by.field":"aggregatetype",
                 "transforms.outbox.route.topic.replacement":"outbox.event.${routedByValue}",
                 "transforms.outbox.table.field.event.id":"id",
                 "transforms.outbox.table.field.event.key":"aggregateid",
                 "transforms.outbox.table.field.event.payload":"payload"}
                """.formatted(topicPrefix, slotName, snapshotMode));
        connect.waitForState(connectorName, "RUNNING", Duration.ofSeconds(30));
    }

    private record RawRecord(String key, String value) {
    }

    private static JsonConverter newValueConverter() {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName(), "schemas.enable", "true"));
        return converter;
    }

    private static boolean containsValue(RawRecord record, String needle) {
        String decoded = payloadValue(record);
        return decoded != null && decoded.contains(needle);
    }

    /** The routed outbox topic's value is a plain JSON STRING (EventRouter's default, unexpanded payload), not a Struct -- decoded via JsonConverter the same way, just with a different resulting Java type. */
    private static String payloadValue(RawRecord record) {
        if (record.value() == null) {
            return null;
        }
        Object value = newValueConverter().toConnectData("t", record.value().getBytes()).value();
        return String.valueOf(value);
    }

    private static List<RawRecord> pollRawRecords(String topic, Duration timeout) {
        return pollRawRecordsMatching(topic, r -> true, timeout);
    }

    private static List<RawRecord> pollRawRecordsMatching(String topic, Predicate<RawRecord> filter, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-outbox-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<RawRecord> collected = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    RawRecord rr = new RawRecord(
                            record.key() == null ? null : new String(record.key()),
                            record.value() == null ? null : new String(record.value()));
                    if (filter.test(rr)) {
                        collected.add(rr);
                    }
                }
            }
        }
        return collected;
    }

    private static List<RawRecord> pollUntilMatching(String topic, Predicate<RawRecord> filter, int minCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-outbox-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<RawRecord> matching = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (matching.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    RawRecord rr = new RawRecord(
                            record.key() == null ? null : new String(record.key()),
                            record.value() == null ? null : new String(record.value()));
                    if (filter.test(rr)) {
                        matching.add(rr);
                    }
                }
            }
        }
        if (matching.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " matching record(s) on " + topic + ", got " + matching.size());
        }
        return matching;
    }
}
