package com.kafkalab.schemaevolution.raw;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.schemaevolution.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Section 3's experiment: the raw serialization pipeline, with NO schema
 * governance at all.
 *
 * <pre>
 * Java Object -&gt; Serializer -&gt; byte[] -&gt; Kafka -&gt; byte[] -&gt; Deserializer -&gt; Java Object
 * </pre>
 *
 * A plain {@code StringSerializer} moves hand-built JSON text as bytes --
 * Kafka stores and transports those bytes with zero understanding of
 * "OrderEvent," "orderId," or any field's type. This app then demonstrates
 * WHY that is dangerous the moment two services evolve independently: it
 * writes one record in the ORIGINAL shape and one record in a SILENTLY
 * changed shape (a field renamed), then reads both back with logic that
 * only knows the original shape. Nothing in this pipeline prevents the
 * shape change, checks it, or even notices it happened -- the failure is
 * discovered only by inspecting the consumer's actual output, which is
 * precisely the point.
 */
public final class RawBytesDemoApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "raw-orders");
        ObjectMapper mapper = new ObjectMapper();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            String original = mapper.writeValueAsString(
                    java.util.Map.of("orderId", "O-101", "customerId", "C-501", "amount", 1250.00));
            System.out.println("Producing ORIGINAL shape:  " + original);
            producer.send(new ProducerRecord<>(topic, "O-101", original)).get();

            // Nothing stopped this: a field silently renamed, with no
            // schema, no registry, and no compatibility check of any kind
            // in the path between this producer and Kafka.
            String changed = mapper.writeValueAsString(
                    java.util.Map.of("orderId", "O-102", "custId", "C-502", "amount", 75.00));
            System.out.println("Producing SILENTLY CHANGED shape: " + changed);
            producer.send(new ProducerRecord<>(topic, "O-102", changed)).get();
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-bytes-demo-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 8000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    JsonNode node = mapper.readTree(record.value());
                    // This consumer's business logic only ever knew to look
                    // for "customerId" -- it has no way to know the field
                    // was renamed upstream. Kafka delivered the bytes
                    // perfectly; the CONTRACT broke, silently.
                    JsonNode customerIdNode = node.get("customerId");
                    String customerId = customerIdNode == null ? null : customerIdNode.asText();
                    System.out.printf("Consumed key=%s | raw=%s | this consumer's customerId lookup -> %s%n",
                            record.key(), record.value(), customerId == null ? "NULL (field missing -- SILENT CORRUPTION, not a crash)" : customerId);
                }
            }
        }
    }
}
