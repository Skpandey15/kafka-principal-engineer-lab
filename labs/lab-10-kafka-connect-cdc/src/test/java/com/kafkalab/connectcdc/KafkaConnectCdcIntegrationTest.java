package com.kafkalab.connectcdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.kafkalab.connectcdc.support.ConnectRestClient;
import com.kafkalab.connectcdc.support.KafkaConnectCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kafka + real PostgreSQL + a real Kafka Connect distributed-mode
 * worker running BOTH the {@code connect-file} plugin and the real
 * Debezium PostgreSQL connector plugin (see {@link KafkaConnectCluster}
 * for why single-node is the right choice for THESE tests) -- no mocked
 * connector, no mocked REST API, no simulated CDC envelope.
 *
 * <p>Every assertion is reached through a bounded condition-polling loop
 * with an overall deadline, per this repository's established test
 * convention.
 */
class KafkaConnectCdcIntegrationTest {

    private static KafkaConnectCluster cluster;
    private static ConnectRestClient connect;

    @BeforeAll
    static void startCluster() {
        cluster = new KafkaConnectCluster(19_700);
        cluster.start();
        connect = new ConnectRestClient(cluster.connectUrl());
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Test
    void fileStreamAndDebeziumPluginsAreBothDiscoveredOnTheWorker() throws Exception {
        // A real, verified finding building this lab (see
        // platform/kafka-connect/fetch-plugins.sh): connect-file is NOT
        // on connect-distributed.sh's default classpath in
        // kafka-clients 4.3.1 -- it has to be added via plugin.path like
        // any external plugin. This asserts that fix actually works,
        // alongside the Debezium plugin.
        JsonNode plugins = connect.connectorPlugins();
        List<String> classNames = new ArrayList<>();
        plugins.forEach(p -> classNames.add(p.path("class").asText()));
        assertTrue(classNames.contains("org.apache.kafka.connect.file.FileStreamSourceConnector"));
        assertTrue(classNames.contains("org.apache.kafka.connect.file.FileStreamSinkConnector"));
        assertTrue(classNames.contains("io.debezium.connector.postgresql.PostgresConnector"));
    }

    @Test
    void fileStreamSourceAndSinkConnectorsMoveDataThroughKafkaEndToEnd() throws Exception {
        String id = uniqueId();
        String topic = "file-topic-" + id;
        String sourceName = "file-source-" + id;
        String sinkName = "file-sink-" + id;
        String sourcePath = "/tmp/source-" + id + ".txt";
        String sinkPath = "/tmp/sink-" + id + ".txt";

        cluster.execInConnectWorker("sh", "-c", "printf 'eventId=Order-1|customerId=C-1|amount=1.00\\n' > " + sourcePath);

        connect.registerConnector(sourceName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
                 "tasks.max":"1","file":"%s","topic":"%s"}
                """.formatted(sourcePath, topic));
        connect.waitForState(sourceName, "RUNNING", Duration.ofSeconds(20));

        connect.registerConnector(sinkName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector",
                 "tasks.max":"1","file":"%s","topics":"%s"}
                """.formatted(sinkPath, topic));
        connect.waitForState(sinkName, "RUNNING", Duration.ofSeconds(20));

        waitUntil(() -> {
            try {
                String contents = cluster.execInConnectWorker("cat", sinkPath).trim();
                return contents.equals("eventId=Order-1|customerId=C-1|amount=1.00");
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(20));
    }

    @Test
    void connectSourceOffsetIsARealFilePositionNotAKafkaOffset() throws Exception {
        String id = uniqueId();
        String topic = "offset-topic-" + id;
        String sourceName = "offset-source-" + id;
        String sourcePath = "/tmp/offset-src-" + id + ".txt";
        String line = "eventId=Order-1|customerId=C-1|amount=1.00\n";

        cluster.execInConnectWorker("sh", "-c", "printf '" + line.replace("\n", "\\n") + "' > " + sourcePath);

        connect.registerConnector(sourceName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
                 "tasks.max":"1","file":"%s","topic":"%s"}
                """.formatted(sourcePath, topic));
        connect.waitForState(sourceName, "RUNNING", Duration.ofSeconds(20));
        consumeRawRecords(topic, 1, Duration.ofSeconds(20));

        // The REAL offset record: key = [connectorName, {"filename": sourcePath}], value = {"position": N}.
        List<RawRecord> offsetRecords = consumeRawStringRecords("connect-offsets", 1, Duration.ofSeconds(20),
                r -> r.contains(sourceName));
        assertTrue(offsetRecords.get(0).value().contains("\"position\""),
                "a FileStreamSourceConnector's own offset representation must be a byte position, not a Kafka offset: " + offsetRecords.get(0).value());
    }

    @Test
    void debeziumConnectorReachesRunningState() throws Exception {
        String id = uniqueId();
        String connectorName = "dbz-" + id;
        registerDebezium(connectorName, "cdc" + id, "slot_" + id.replace("-", "_"));
        var status = connect.waitForState(connectorName, "RUNNING", Duration.ofSeconds(30));
        assertEquals("RUNNING", status.path("connector").path("state").asText());
    }

    @Test
    void insertProducesACreateEventWithNullBeforeAndPopulatedAfter() throws Exception {
        String id = uniqueId();
        String topicPrefix = "cdc" + id;
        registerDebezium("dbz-insert-" + id, topicPrefix, "slot_ins_" + id.replace("-", "_"));
        connect.waitForState("dbz-insert-" + id, "RUNNING", Duration.ofSeconds(30));

        String orderId = "O-" + id;
        insertOrder(orderId, "C-1", "12.34");

        List<CdcEvent> events = consumeCdcEvents(topicPrefix + ".public.orders", 1, Duration.ofSeconds(20));
        CdcEvent event = events.get(0);
        assertEquals("c", event.op());
        assertNull(event.before());
        assertNotNull(event.after());
        assertEquals(orderId, event.after().get("order_id"));
        assertEquals(new BigDecimal("12.34"), event.after().get("amount"));
    }

    @Test
    void updateProducesAnEventWithFullBeforeAndAfterDueToReplicaIdentityFull() throws Exception {
        String id = uniqueId();
        String topicPrefix = "cdc" + id;
        registerDebezium("dbz-update-" + id, topicPrefix, "slot_upd_" + id.replace("-", "_"));
        connect.waitForState("dbz-update-" + id, "RUNNING", Duration.ofSeconds(30));

        String orderId = "O-" + id;
        insertOrder(orderId, "C-1", "10.00");
        updateOrderAmount(orderId, "20.00");

        List<CdcEvent> events = consumeCdcEvents(topicPrefix + ".public.orders", 2, Duration.ofSeconds(20));
        CdcEvent update = events.get(1);
        assertEquals("u", update.op());
        assertNotNull(update.before(), "REPLICA IDENTITY FULL means the update's 'before' must be fully populated, not just the key");
        assertEquals(new BigDecimal("10.00"), update.before().get("amount"));
        assertEquals(new BigDecimal("20.00"), update.after().get("amount"));
    }

    @Test
    void deleteProducesAnEventThenATombstone() throws Exception {
        String id = uniqueId();
        String topicPrefix = "cdc" + id;
        registerDebezium("dbz-delete-" + id, topicPrefix, "slot_del_" + id.replace("-", "_"));
        connect.waitForState("dbz-delete-" + id, "RUNNING", Duration.ofSeconds(30));

        String orderId = "O-" + id;
        insertOrder(orderId, "C-1", "5.00");
        deleteOrder(orderId);

        List<RawRecord> raw = consumeRawRecords(topicPrefix + ".public.orders", 3, Duration.ofSeconds(20));
        assertEquals("c", parseOp(raw.get(0)));
        assertEquals("d", parseOp(raw.get(1)));
        assertNull(raw.get(2).value(), "Debezium must emit a null-value tombstone immediately after a delete, for log compaction");
    }

    @Test
    void preExistingRowsAreCapturedAsASnapshotWithFirstAndLastMarkers() throws Exception {
        String id = uniqueId();
        String topicPrefix = "cdc" + id;

        try (Connection connection = newJdbcConnection()) {
            try (var statement = connection.createStatement()) {
                statement.execute("DELETE FROM orders");
            }
        }
        insertOrder("O-SNAP-1-" + id, "C-1", "1.00");
        insertOrder("O-SNAP-2-" + id, "C-1", "2.00");

        registerDebezium("dbz-snapshot-" + id, topicPrefix, "slot_snap_" + id.replace("-", "_"), "initial");
        connect.waitForState("dbz-snapshot-" + id, "RUNNING", Duration.ofSeconds(30));

        List<CdcEvent> events = consumeCdcEvents(topicPrefix + ".public.orders", 2, Duration.ofSeconds(30));
        assertEquals("first", events.get(0).snapshot());
        assertEquals("last", events.get(1).snapshot());
    }

    @Test
    void deletingAConnectorDoesNotClearItsStoredOffsets() throws Exception {
        // A real, surprising finding building this lab (see
        // FileStreamDemoApp's Javadoc): Kafka Connect's offset storage
        // outlives a connector's own delete/recreate lifecycle. Verified
        // here deterministically: register, produce one line, delete the
        // connector, re-register under the SAME name/file, and confirm
        // the re-registered task's stored offset is still the OLD
        // (non-zero) position, not reset to zero.
        String id = uniqueId();
        String topic = "outlive-topic-" + id;
        String connectorName = "outlive-source-" + id;
        String sourcePath = "/tmp/outlive-" + id + ".txt";

        cluster.execInConnectWorker("sh", "-c", "printf 'line-one\\n' > " + sourcePath);
        connect.registerConnector(connectorName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
                 "tasks.max":"1","file":"%s","topic":"%s"}
                """.formatted(sourcePath, topic));
        connect.waitForState(connectorName, "RUNNING", Duration.ofSeconds(20));
        consumeRawRecords(topic, 1, Duration.ofSeconds(20));

        connect.delete(connectorName);
        waitUntil(() -> {
            try {
                return connect.status(connectorName).path("error_code").asInt() == 404;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(10));

        connect.registerConnector(connectorName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
                 "tasks.max":"1","file":"%s","topic":"%s"}
                """.formatted(sourcePath, topic));
        connect.waitForState(connectorName, "RUNNING", Duration.ofSeconds(20));

        // The re-registered task's stored position must already be the
        // FULL length of "line-one\n" (9 bytes) -- proving it resumed
        // from the OLD offset rather than starting over at 0.
        List<RawRecord> offsetRecords = consumeRawStringRecords("connect-offsets", 1, Duration.ofSeconds(20),
                r -> r.contains(connectorName));
        assertTrue(offsetRecords.get(offsetRecords.size() - 1).value().contains("\"position\":9"),
                "expected the re-registered connector to resume from the previously stored position (9), got: " + offsetRecords);
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static void registerDebezium(String connectorName, String topicPrefix, String slotName) throws Exception {
        registerDebezium(connectorName, topicPrefix, slotName, "no_data");
    }

    /**
     * {@code snapshot.mode} matters a great deal here: the {@code orders}
     * table is SHARED across every test in this class (Postgres has no
     * per-connector table scoping), so a connector using the default
     * {@code initial} mode would snapshot whatever rows EARLIER tests
     * left behind as {@code "r"} (read) events before this test's own
     * fresh insert/update/delete ever appears -- a real test-isolation
     * bug this exact scenario surfaced while building this suite
     * ({@code expected: <c> but was: <r>}). {@code no_data} captures the
     * table's schema at start but explicitly skips snapshotting existing
     * ROWS, so a test using it only ever sees the changes IT makes,
     * going forward -- exactly what the insert/update/delete tests need.
     * {@link #preExistingRowsAreCapturedAsASnapshotWithFirstAndLastMarkers}
     * is the one test that deliberately wants the default snapshot
     * behavior, and calls the 4-argument overload with {@code "initial"}
     * explicitly instead.
     */
    private static void registerDebezium(String connectorName, String topicPrefix, String slotName, String snapshotMode) throws Exception {
        connect.registerConnector(connectorName, """
                {"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
                 "database.hostname":"it-postgres","database.port":"5432",
                 "database.user":"postgres","database.password":"postgres","database.dbname":"inventory",
                 "topic.prefix":"%s","table.include.list":"public.orders",
                 "publication.name":"dbz_publication","publication.autocreate.mode":"disabled",
                 "slot.name":"%s","plugin.name":"pgoutput","snapshot.mode":"%s"}
                """.formatted(topicPrefix, slotName, snapshotMode));
    }

    private static Connection newJdbcConnection() throws Exception {
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        return DriverManager.getConnection(cluster.jdbcUrl(), props);
    }

    private static void insertOrder(String orderId, String customerId, String amount) throws Exception {
        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
            statement.setString(1, orderId);
            statement.setString(2, customerId);
            statement.setBigDecimal(3, new BigDecimal(amount));
            statement.executeUpdate();
        }
    }

    private static void updateOrderAmount(String orderId, String amount) throws Exception {
        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE orders SET amount = ? WHERE order_id = ?")) {
            statement.setBigDecimal(1, new BigDecimal(amount));
            statement.setString(2, orderId);
            statement.executeUpdate();
        }
    }

    private static void deleteOrder(String orderId) throws Exception {
        try (Connection connection = newJdbcConnection();
             PreparedStatement statement = connection.prepareStatement("DELETE FROM orders WHERE order_id = ?")) {
            statement.setString(1, orderId);
            statement.executeUpdate();
        }
    }

    private record RawRecord(String key, String value) {
    }

    private static String parseOp(RawRecord record) {
        if (record.value() == null) {
            return null;
        }
        JsonConverter valueConverter = newJsonConverter();
        Struct envelope = (Struct) valueConverter.toConnectData("t", record.value().getBytes()).value();
        return envelope.getString("op");
    }

    private static JsonConverter newJsonConverter() {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName(), "schemas.enable", "true"));
        return converter;
    }

    private static List<RawRecord> consumeRawRecords(String topic, int minCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-raw-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<RawRecord> collected = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    collected.add(new RawRecord(
                            record.key() == null ? null : new String(record.key()),
                            record.value() == null ? null : new String(record.value())));
                }
            }
        }
        if (collected.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " records on " + topic + ", got " + collected.size());
        }
        return collected;
    }

    /** Like {@link #consumeRawRecords}, but returns String key/value pairs directly (for connect-offsets, whose key/value are plain JSON text, not Connect-enveloped). */
    private static List<RawRecord> consumeRawStringRecords(String topic, int minMatching, Duration timeout, java.util.function.Predicate<String> keyFilter) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-offsets-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<RawRecord> matching = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (matching.size() < minMatching && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.key() != null && keyFilter.test(record.key())) {
                        matching.add(new RawRecord(record.key(), record.value()));
                    }
                }
            }
        }
        if (matching.size() < minMatching) {
            throw new AssertionError("expected at least " + minMatching + " matching record(s) on " + topic + ", got " + matching.size());
        }
        return matching;
    }

    private static List<CdcEvent> consumeCdcEvents(String topic, int minCount, Duration timeout) {
        JsonConverter valueConverter = newJsonConverter();
        List<RawRecord> raw = consumeRawRecords(topic, minCount, timeout);
        List<CdcEvent> events = new ArrayList<>();
        for (RawRecord record : raw) {
            if (record.value() == null) {
                continue;
            }
            Struct envelope = (Struct) valueConverter.toConnectData(topic, record.value().getBytes()).value();
            String op = envelope.getString("op");
            String snapshot = envelope.getStruct("source").getString("snapshot");
            events.add(new CdcEvent(op, snapshot, structToMap(safeStruct(envelope, "before")), structToMap(safeStruct(envelope, "after"))));
        }
        return events;
    }

    private static Struct safeStruct(Struct envelope, String field) {
        try {
            return envelope.getStruct(field);
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<String, Object> structToMap(Struct struct) {
        if (struct == null) {
            return null;
        }
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        struct.schema().fields().forEach(f -> map.put(f.name(), struct.get(f)));
        return map;
    }

    private record CdcEvent(String op, String snapshot, Map<String, Object> before, Map<String, Object> after) {
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
