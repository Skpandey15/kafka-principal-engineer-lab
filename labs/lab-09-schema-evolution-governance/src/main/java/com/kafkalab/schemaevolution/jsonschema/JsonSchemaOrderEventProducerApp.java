package com.kafkalab.schemaevolution.jsonschema;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Section 20's experiment: {@code plain JSON} vs. {@code JSON governed by
 * JSON Schema} -- the point this app exists to make concrete is that
 * "we use JSON" (Section 3's {@code RawBytesDemoApp}, a plain
 * {@code StringSerializer} moving hand-built JSON text with ZERO
 * validation) and "we use JSON Schema" (this app: the SAME kind of JSON
 * payload, but with a real schema registered, versioned, and
 * compatibility-checked exactly like the Avro and Protobuf subjects in
 * this lab) are completely different levels of governance, even though
 * both eventually put JSON bytes on the wire.
 */
public final class JsonSchemaOrderEventProducerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String topic = LabConfig.get("topic", "jsonschema-orders");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        OrderEventJson event = new OrderEventJson("O-JSON-1", "C-501", "55.00");

        try (KafkaProducer<String, OrderEventJson> producer = new KafkaProducer<>(props)) {
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, event.orderId, event)).get();
            System.out.printf("Produced JSON Schema-governed OrderEvent | topic=%s | partition=%d | offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }

        System.out.println("A REAL schema (reflected from OrderEventJson's own shape) was registered under subject '" + topic + "-value' --");
        System.out.println("this is the difference from Section 3's RawBytesDemoApp, whose hand-built JSON has NO registered schema, NO subject, and NO compatibility check at all.");
    }
}
