package com.kafkalab.connectcdc.cdc;

import com.kafkalab.connectcdc.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consumes {@code cdc.public.orders} and decodes Debezium's REAL envelope
 * -- {@code op}/{@code before}/{@code after}/{@code source} -- using
 * Kafka Connect's own {@link JsonConverter} and {@link Struct} API,
 * exactly the way a real downstream Connect-aware consumer would (not
 * hand-parsed JSON field access, which would have to reimplement the
 * Decimal logical-type decoding itself).
 *
 * <p>{@code op} values, real and verified against this exact pinned
 * Debezium version: {@code "c"} (create), {@code "u"} (update),
 * {@code "d"} (delete), {@code "r"} (read -- a snapshot-phase record).
 */
public final class CdcEventConsumerApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "cdc.public.orders");
        long runForMs = LabConfig.getLong("runForMs", 15000);

        JsonConverter valueConverter = new JsonConverter();
        valueConverter.configure(Map.of(
                ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName(),
                "schemas.enable", "true"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        System.out.printf("CdcEventConsumerApp | topic=%s%n", topic);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + runForMs;
            int count = 0;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.value() == null) {
                        System.out.printf("partition=%d offset=%d | TOMBSTONE (Debezium's post-delete marker for log compaction)%n",
                                record.partition(), record.offset());
                        continue;
                    }
                    count++;
                    Struct envelope = (Struct) valueConverter.toConnectData(topic, record.value()).value();
                    String op = envelope.getString("op");
                    Struct before = safeStruct(envelope, "before");
                    Struct after = safeStruct(envelope, "after");
                    Struct source = envelope.getStruct("source");
                    System.out.printf("partition=%d offset=%d | op=%s | snapshot=%s | lsn=%s | before=%s | after=%s%n",
                            record.partition(), record.offset(), describeOp(op),
                            source.getString("snapshot"), source.get("lsn"),
                            describeRow(before), describeRow(after));
                }
            }
            System.out.printf("Done. %d real CDC event(s) decoded.%n", count);
        }
    }

    private static Struct safeStruct(Struct envelope, String field) {
        try {
            return envelope.getStruct(field);
        } catch (Exception e) {
            return null;
        }
    }

    private static String describeOp(String op) {
        return switch (op) {
            case "c" -> "c (create/insert)";
            case "u" -> "u (update)";
            case "d" -> "d (delete)";
            case "r" -> "r (read -- snapshot phase)";
            default -> op;
        };
    }

    private static String describeRow(Struct row) {
        if (row == null) {
            return "null";
        }
        Object amount = row.get("amount");
        return String.format("{order_id=%s, customer_id=%s, amount=%s}",
                row.get("order_id"), row.get("customer_id"), amount);
    }
}
