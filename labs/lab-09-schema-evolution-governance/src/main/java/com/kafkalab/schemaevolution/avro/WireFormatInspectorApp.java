package com.kafkalab.schemaevolution.avro;

import com.kafkalab.schemaevolution.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Section 10's experiment: read a topic's records as RAW {@code byte[]}
 * (bypassing the Avro deserializer entirely) and manually decode the
 * Confluent wire format's own framing:
 *
 * <pre>
 * [magic byte = 0x0][4-byte big-endian schema ID][Avro binary payload]
 * </pre>
 *
 * This is Confluent's own serializer's wire format specifically -- it is
 * NOT a generic "every Schema Registry implementation does this" fact.
 * Apicurio Registry, for example, supports multiple ID-encoding strategies
 * and is not guaranteed to use the same 1-byte-magic + 4-byte-ID framing.
 * This app labels every claim below as Confluent-specific for that reason.
 */
public final class WireFormatInspectorApp {

    private static final byte CONFLUENT_MAGIC_BYTE = 0x0;

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "avro-orders");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wire-format-inspector-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 8000;
            int shown = 0;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, byte[]> record : records) {
                    shown++;
                    describe(record);
                }
            }
            if (shown == 0) {
                System.out.println("No records found on " + topic + " -- produce one first (runAvroProducer).");
            }
        }
    }

    private static void describe(ConsumerRecord<String, byte[]> record) {
        byte[] bytes = record.value();
        System.out.printf("partition=%d | offset=%d | total bytes=%d%n", record.partition(), record.offset(), bytes.length);
        if (bytes.length < 5) {
            System.out.println("  Too short to contain the Confluent framing (magic byte + 4-byte schema ID) -- not produced by KafkaAvroSerializer?");
            return;
        }
        byte magic = bytes[0];
        ByteBuffer idBuffer = ByteBuffer.wrap(bytes, 1, 4);
        int schemaId = idBuffer.getInt();
        int payloadLength = bytes.length - 5;
        System.out.printf("  [Confluent-specific framing] magicByte=0x%x (expected 0x%x) | schemaId=%d | avroPayloadBytes=%d%n",
                magic, CONFLUENT_MAGIC_BYTE, schemaId, payloadLength);
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < Math.min(bytes.length, 16); i++) {
            hex.append(String.format("%02x ", bytes[i]));
        }
        System.out.println("  first bytes (hex): " + hex + (bytes.length > 16 ? "..." : ""));
    }
}
