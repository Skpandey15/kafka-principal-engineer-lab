package com.kafkalab.outbox.outbox;

import com.kafkalab.outbox.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consumes a routed outbox topic (e.g. {@code outbox.event.Order}) and
 * decodes it. Unlike WP-11's {@code CdcEventConsumerApp}, the record's
 * value here is NOT a full Debezium envelope with op/before/after/source
 * -- the EventRouter transform already unwrapped it down to just the
 * outbox row's own {@code payload} column content (a plain JSON string),
 * because {@code table.expand.json.payload} was left at its default
 * ({@code false}). The worker's own JsonConverter (schemas.enable=true)
 * still wraps THAT string in the usual {"schema":...,"payload":...}
 * envelope on the wire, so {@link JsonConverter#toConnectData} is still
 * the correct way to unwrap it -- it just returns a plain
 * {@code String}, not a {@code Struct}, this time.
 */
public final class OutboxEventConsumerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "outbox.event.Order");
        long runForMs = Long.parseLong(LabConfig.get("runForMs", "15000"));

        JsonConverter keyConverter = new JsonConverter();
        keyConverter.configure(Map.of(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName(), "schemas.enable", "true"));
        JsonConverter valueConverter = new JsonConverter();
        valueConverter.configure(Map.of(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName(), "schemas.enable", "true"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-consumer-demo");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            System.out.println("[outbox-consumer] listening on " + topic + " for " + runForMs + "ms...");
            Instant deadline = Instant.now().plusMillis(runForMs);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.value() == null) {
                        System.out.println("[outbox-consumer] tombstone, key=" + decode(keyConverter, record.key()));
                        continue;
                    }
                    Object key = decode(keyConverter, record.key());
                    Object value = valueConverter.toConnectData(topic, record.value()).value();
                    System.out.println("[outbox-consumer] key=" + key + " payload=" + value);
                }
            }
        }
    }

    private static Object decode(JsonConverter converter, byte[] bytes) {
        return bytes == null ? null : converter.toConnectData("k", bytes).value();
    }
}
