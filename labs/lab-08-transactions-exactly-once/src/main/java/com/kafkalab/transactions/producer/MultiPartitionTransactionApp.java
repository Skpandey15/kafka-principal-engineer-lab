package com.kafkalab.transactions.producer;

import com.kafkalab.transactions.support.LabConfig;
import com.kafkalab.transactions.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Section 10's experiment: ONE transaction writing to THREE different
 * topics (standing in for "multiple partitions and/or multiple topics" --
 * separate topics is the stronger, more visibly distinct case) --
 * {@code txn-lab-orders}, {@code txn-lab-payments}, {@code txn-lab-audit}
 * -- then committing or aborting all three writes together.
 *
 * <h2>What "atomic" means here, precisely</h2>
 * It does NOT mean these three writes land at the same offset, the same
 * timestamp, or even on the same broker (they don't -- each topic's
 * partition-0 leader can be a different broker entirely). It means: no
 * {@code read_committed} consumer of ANY of these three topics can ever
 * observe the commit outcome of one of these records without also, from
 * that point on, being able to observe the other two -- the transaction
 * coordinator writes markers to every partition this transaction touched
 * only once the commit decision itself is durable, and a
 * {@code read_committed} consumer withholds each partition's records until
 * ITS marker arrives. See the lab README's Experiment 6 for real evidence
 * from all three topics after both a commit and an abort run.
 */
public final class MultiPartitionTransactionApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String transactionalId = LabConfig.get("transactionalId", "txn-lab-multi-partition-producer");
        String action = LabConfig.get("action", "commit");
        String eventId = LabConfig.get("eventId", "Order-500");

        String ordersTopic = LabConfig.get("ordersTopic", "txn-lab-orders");
        String paymentsTopic = LabConfig.get("paymentsTopic", "txn-lab-payments");
        String auditTopic = LabConfig.get("auditTopic", "txn-lab-audit");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        System.out.printf("MultiPartitionTransactionApp | transactionalId=%s | eventId=%s | action=%s%n", transactionalId, eventId, action);
        System.out.printf("Writing to 3 topics in ONE transaction: %s, %s, %s%n", ordersTopic, paymentsTopic, auditTopic);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();
            producer.beginTransaction();

            OrderEvent orderEvent = new OrderEvent(eventId, "CUSTOMER-MULTI", 250.00);
            RecordMetadata orderMeta = producer.send(new ProducerRecord<>(ordersTopic, eventId, orderEvent.toWireFormat())).get();
            System.out.printf("  sent | topic=%s | partition=%d | offset=%d%n", ordersTopic, orderMeta.partition(), orderMeta.offset());

            OrderEvent paymentEvent = new OrderEvent(eventId + "-PAYMENT", "CUSTOMER-MULTI", 250.00);
            RecordMetadata paymentMeta = producer.send(new ProducerRecord<>(paymentsTopic, eventId, paymentEvent.toWireFormat())).get();
            System.out.printf("  sent | topic=%s | partition=%d | offset=%d%n", paymentsTopic, paymentMeta.partition(), paymentMeta.offset());

            OrderEvent auditEvent = new OrderEvent(eventId + "-AUDIT", "CUSTOMER-MULTI", 250.00);
            RecordMetadata auditMeta = producer.send(new ProducerRecord<>(auditTopic, eventId, auditEvent.toWireFormat())).get();
            System.out.printf("  sent | topic=%s | partition=%d | offset=%d%n", auditTopic, auditMeta.partition(), auditMeta.offset());

            if ("abort".equalsIgnoreCase(action)) {
                System.out.println("abortTransaction() -- ALL THREE writes above become invisible to read_committed consumers, across all three topics, together.");
                producer.abortTransaction();
            } else {
                System.out.println("commitTransaction() -- ALL THREE writes above become visible to read_committed consumers, across all three topics, together.");
                producer.commitTransaction();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("Done.");
    }
}
