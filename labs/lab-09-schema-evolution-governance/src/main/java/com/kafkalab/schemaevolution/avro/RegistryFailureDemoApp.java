package com.kafkalab.schemaevolution.avro;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Sections 30-31's experiment, spanning a REAL Schema Registry outage
 * within one running process -- matching the pause-for-operator-action
 * pattern WP-09's {@code FencingDemoApp} established.
 *
 * <pre>
 *   Phase 1 (registry UP): produce + consume one record each, warming
 *                          this process's own in-memory ID/schema caches.
 *   -- operator runs `docker compose stop schema-registry`, then presses Enter --
 *   Phase 2 (registry DOWN): the SAME producer/consumer instances send/poll
 *                          AGAIN, reusing their warm caches; a BRAND NEW
 *                          SchemaRegistryClient (empty cache) then attempts
 *                          a lookup that has never been cached anywhere in
 *                          this process, to observe the failure directly.
 * </pre>
 */
public final class RegistryFailureDemoApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String topic = LabConfig.get("topic", "avro-orders");
        Schema v1 = new Schema.Parser().parse(new File("src/main/avro/order-event-v1.avsc"));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        producerProps.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "registry-failure-demo-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProps.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps);
             KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps)) {

            System.out.println("=== PHASE 1: registry UP ===");
            send(producer, topic, v1, "O-REGFAIL-1");
            consumer.subscribe(List.of(topic));
            pollOnce(consumer, "warm-up poll (also warms this consumer's schema cache)");

            System.out.println();
            System.out.println("Now run in another terminal:  docker compose -f platform/schema-registry/docker-compose.yml stop schema-registry");
            System.out.println("Then press Enter here.");
            new BufferedReader(new InputStreamReader(System.in)).readLine();

            System.out.println();
            System.out.println("=== PHASE 2: registry DOWN ===");
            System.out.println("--- Q: can this SAME producer instance continue, reusing its cached schema->ID mapping? ---");
            try {
                send(producer, topic, v1, "O-REGFAIL-2");
                System.out.println("YES -- send succeeded with no registry round-trip needed (schema/ID already cached in this producer instance).");
            } catch (Exception e) {
                System.out.println("NO -- send failed: " + e);
            }

            System.out.println("--- Q: can this SAME consumer instance continue reading ALREADY-CACHED schema IDs? ---");
            try {
                pollOnce(consumer, "post-outage poll of records with already-cached schema IDs");
                System.out.println("YES -- deserialization succeeded with no registry round-trip needed.");
            } catch (Exception e) {
                System.out.println("NO -- poll/deserialize failed: " + e);
            }

            System.out.println("--- Q: what happens with a BRAND NEW client that has never cached anything, hitting a never-before-seen lookup? ---");
            try (SchemaRegistryClient freshClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)) {
                freshClient.getLatestSchemaMetadata(topic + "-value");
                System.out.println("UNEXPECTED: succeeded (registry must actually be reachable).");
            } catch (Exception e) {
                System.out.println("FAILS, as expected -- real exception: " + e);
            }
        }
    }

    private static void send(KafkaProducer<String, GenericRecord> producer, String topic, Schema schema, String eventId) throws Exception {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("orderId", eventId);
        record.put("customerId", "C-501");
        record.put("amount", new BigDecimal("1.00"));
        var metadata = producer.send(new ProducerRecord<>(topic, eventId, (GenericRecord) record)).get();
        System.out.printf("  sent eventId=%s | partition=%d | offset=%d%n", eventId, metadata.partition(), metadata.offset());
    }

    private static void pollOnce(KafkaConsumer<String, GenericRecord> consumer, String label) {
        System.out.println("  " + label + "...");
        long deadline = System.currentTimeMillis() + 5000;
        int total = 0;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(300));
            total += records.count();
            if (!records.isEmpty()) {
                break;
            }
        }
        System.out.println("  consumed " + total + " record(s).");
    }
}
