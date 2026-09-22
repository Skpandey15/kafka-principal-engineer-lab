package com.kafkalab.springkafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@code AckMode.MANUAL} (configured on {@code manualAckContainerFactory}):
 * the container commits ONLY the offsets this listener explicitly
 * acknowledges, one record at a time -- the native mechanism this wraps
 * is WP-06's own manual {@code commitSync()} per record, not the
 * automatic per-batch commit Spring Kafka's default {@code AckMode.BATCH}
 * gives you. A record whose key is {@code "skip-ack"} deliberately
 * never calls {@link Acknowledgment#acknowledge()}, modeling a listener
 * that decides NOT to consider a specific record done yet.
 */
@Component
public class ManualAckListener {

    private final List<String> processedKeys = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "manual-ack-in", containerFactory = "manualAckContainerFactory", groupId = "lab14-manual-ack-group")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        processedKeys.add(record.key());
        if (!"skip-ack".equals(record.key())) {
            acknowledgment.acknowledge();
        }
    }

    public List<String> processedKeys() {
        return Collections.unmodifiableList(processedKeys);
    }
}
