package com.kafkalab.connectcdc.connect;

import com.kafkalab.connectcdc.support.LabConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Section 4's experiment: dump the REAL {@code connect-offsets} internal
 * topic. A source connector's "offset" is whatever ITS OWN
 * {@code SourceTask} implementation says it is -- for
 * {@code FileStreamSourceConnector}, real captured evidence is a byte
 * POSITION within the source file:
 *
 * <pre>
 * key:   ["file-source-orders",{"filename":"/data/source-orders.txt"}]
 * value: {"position":92}
 * </pre>
 *
 * For Debezium's PostgreSQL connector, the equivalent offset is a WAL LSN
 * (log sequence number) instead -- a completely different kind of
 * "position," specific to that connector's own source system. This is
 * exactly the point: Kafka Connect's offset storage is a generic
 * key-value mechanism; what a "position" MEANS is entirely up to each
 * connector.
 */
public final class ConnectOffsetsInspectorApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("offsetsTopic", "connect-offsets");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offsets-inspector-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15000;
            int count = 0;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.printf("key=%s | value=%s%n", record.key(), record.value());
                }
            }
            System.out.printf("Done. %d real offset record(s) found on %s.%n", count, topic);
        }
    }
}
