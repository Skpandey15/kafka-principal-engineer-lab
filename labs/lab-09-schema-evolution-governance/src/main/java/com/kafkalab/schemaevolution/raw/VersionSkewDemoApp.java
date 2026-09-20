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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Section 4's experiment: run independently-versioned producer/consumer
 * "reading logic" against each other, still with no schema governance --
 * this is the version-skew problem BEFORE any of this WP's Avro/registry
 * machinery exists to help.
 *
 * <pre>
 * OrderEvent v1: orderId, customerId, amount
 * OrderEvent v2: orderId, customerId, amount, currency
 * </pre>
 *
 * Run with {@code -PproducerVersion=v2 -PconsumerVersion=v1} (new producer,
 * old consumer) and {@code -PproducerVersion=v1 -PconsumerVersion=v2} (old
 * producer, new consumer) to see both directions for real.
 */
public final class VersionSkewDemoApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "version-skew-orders");
        String producerVersion = LabConfig.get("producerVersion", "v2");
        String consumerVersion = LabConfig.get("consumerVersion", "v1");
        ObjectMapper mapper = new ObjectMapper();

        System.out.printf("Producer=%s -> Kafka -> Consumer=%s%n", producerVersion, consumerVersion);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("orderId", "O-" + System.currentTimeMillis());
            fields.put("customerId", "C-501");
            fields.put("amount", 1250.00);
            if ("v2".equals(producerVersion)) {
                fields.put("currency", "EUR");
            }
            String json = mapper.writeValueAsString(fields);
            System.out.println("Producer " + producerVersion + " writes: " + json);
            producer.send(new ProducerRecord<>(topic, (String) fields.get("orderId"), json)).get();
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "version-skew-" + producerVersion + "-" + consumerVersion + "-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 8000;
            boolean sawOne = false;
            while (System.currentTimeMillis() < deadline && !sawOne) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    sawOne = true;
                    JsonNode node = mapper.readTree(record.value());
                    if ("v1".equals(consumerVersion)) {
                        // v1 reading logic only ever looks for orderId/customerId/amount.
                        // If the producer was v2, the extra "currency" field is simply
                        // never looked at -- present on the wire, invisible to this logic.
                        System.out.printf("Consumer v1 reads: orderId=%s customerId=%s amount=%s (currency field, if present on the wire, is never read by v1 logic at all)%n",
                                node.get("orderId").asText(), node.get("customerId").asText(), node.get("amount").asText());
                    } else {
                        JsonNode currencyNode = node.get("currency");
                        String currency = currencyNode == null ? null : currencyNode.asText();
                        String resolvedCurrency = currency != null ? currency
                                : "USD (v2 logic's OWN hardcoded fallback -- there is no schema here to supply this default; a real v1 producer never wrote a currency field at all)";
                        System.out.printf("Consumer v2 reads: orderId=%s customerId=%s amount=%s currency=%s%n",
                                node.get("orderId").asText(), node.get("customerId").asText(), node.get("amount").asText(), resolvedCurrency);
                    }
                }
            }
            if (!sawOne) {
                System.out.println("No record consumed within the wait window.");
            }
        }
    }
}
