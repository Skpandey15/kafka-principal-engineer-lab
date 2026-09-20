package com.kafkalab.schemaevolution.avro;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Sections 5-8, 18's experiment: a real Avro consumer against the real
 * Schema Registry. This app deliberately does NOT pin a reader schema --
 * {@code KafkaAvroDeserializer} without {@code specific.avro.reader} set
 * resolves each record using the WRITER schema the registry hands back for
 * that record's embedded schema ID, producing a {@link GenericRecord}
 * shaped however that writer schema says. Section 18's reader-vs-writer
 * discussion is demonstrated concretely by this app being able to read
 * records produced under DIFFERENT schema versions in the same run, each
 * one deserialized against its OWN writer schema.
 */
public final class AvroOrderEventConsumerApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String topic = LabConfig.get("topic", "avro-orders");
        String groupId = LabConfig.get("groupId", "avro-orders-consumer");
        long runForMs = LabConfig.getLong("runForMs", 10000);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        // GenericRecord, not a generated SpecificRecord class -- see the
        // producer's Javadoc, "Why GenericRecord."
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        System.out.printf("AvroOrderEventConsumerApp | topic=%s | groupId=%s%n", topic, groupId);

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + runForMs;
            int count = 0;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    count++;
                    System.out.printf("partition=%d | offset=%d | writerSchemaFields=%s | value=%s%n",
                            record.partition(), record.offset(), record.value().getSchema().getFields(), record.value());
                }
            }
            System.out.printf("Done. Consumed %d record(s).%n", count);
        }
    }
}
