package com.kafkalab.deliverysemantics.producer;

import com.kafkalab.deliverysemantics.support.LabConfig;
import com.kafkalab.deliverysemantics.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Seeds the lab topic with {@code eventId}-tagged order events, keyed by
 * {@code customerId} so a multi-partition topic gets a real spread across
 * partitions (needed for the per-partition-offsets and rebalance
 * experiments). {@code eventId}s are sequential and human-readable
 * ({@code ORDER-1001}, {@code ORDER-1002}, ...) so log lines and the
 * README's captured output stay easy to follow by eye.
 */
public final class OrderEventProducerApp {

    private static final int DISTINCT_CUSTOMERS = 4;

    public static void main(String[] args) {
        String topic = LabConfig.topic();
        int count = LabConfig.getInt("count", 10);
        int startEventNumber = LabConfig.getInt("startEventNumber", 1);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing " + count + " order events to " + topic
                    + " starting at ORDER-" + (1000 + startEventNumber));
            for (int i = 0; i < count; i++) {
                int eventNumber = startEventNumber + i;
                String eventId = "ORDER-" + (1000 + eventNumber);
                String customerId = "CUSTOMER-" + (100 + (eventNumber % DISTINCT_CUSTOMERS));
                double amount = 10.00 + (eventNumber % 25) * 3.5;
                OrderEvent event = new OrderEvent(eventId, customerId, amount);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, customerId, event.toWireFormat());
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("send failed for eventId=" + eventId + ": " + exception);
                    } else {
                        System.out.printf("sent eventId=%-12s customerId=%-12s -> partition=%d offset=%d%n",
                                eventId, customerId, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
            System.out.println("Done.");
        }
    }
}
