package com.kafkalab.retrydlq.producer;

import com.kafkalab.retrydlq.support.LabConfig;
import com.kafkalab.retrydlq.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * Produces one record to the lab's input topic. With
 * {@code -Ppoison=true}, sends deliberately malformed wire-format text
 * instead of a real OrderEvent -- this lab's "poison message" case,
 * something the retry processor can never successfully parse no matter
 * how many times it's retried.
 */
public final class OrderEventProducerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "retry-dlq-orders");
        String eventId = LabConfig.get("eventId", "ORDER-1");
        String customerId = LabConfig.get("customerId", "C-1");
        double amount = Double.parseDouble(LabConfig.get("amount", "10.00"));
        boolean poison = Boolean.parseBoolean(LabConfig.get("poison", "false"));

        String value = poison
                ? "this-is-not-a-valid-order-event-wire-format"
                : new OrderEvent(eventId, customerId, amount).toWireFormat();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>(topic, eventId, value)).get();
            System.out.println("[producer] sent " + (poison ? "POISON " : "") + "record: " + value);
        }
    }
}
