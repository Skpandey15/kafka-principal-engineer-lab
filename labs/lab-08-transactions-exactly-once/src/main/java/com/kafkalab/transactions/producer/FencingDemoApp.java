package com.kafkalab.transactions.producer;

import com.kafkalab.transactions.support.LabConfig;
import com.kafkalab.transactions.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Section 12's experiment: producer fencing. Run this app TWICE, from two
 * separate terminals, with the SAME {@code -PtransactionalId}:
 *
 * <pre>
 *   Terminal 1: ./gradlew runFencingDemo -PinstanceName=instance-A
 *   (it sends one record, then pauses waiting for Enter)
 *   Terminal 2: ./gradlew runFencingDemo -PinstanceName=instance-B
 *   (it runs to completion immediately -- its OWN initTransactions() call
 *    fences instance A, deterministically, the moment it runs)
 *   Terminal 1: press Enter -- its next send/commit fails with a real,
 *   fencing-related exception
 * </pre>
 *
 * <p>Unlike Section 2's duplicate-risk experiment, this one is fully
 * deterministic: {@code initTransactions()} for a given
 * {@code transactional.id} ALWAYS bumps the producer epoch at the
 * transaction coordinator and fences whatever epoch the previous instance
 * was using, regardless of timing. The only thing timing affects is WHICH
 * of instance A's calls discovers the fencing (its second send, or its
 * commit) -- not WHETHER it gets fenced.
 *
 * <h2>The exception this repository actually observed -- not the one most
 * tutorials describe</h2>
 * Running this exact scenario against the pinned {@code kafka-clients:4.3.1}
 * client, the fenced producer's next {@code send(...).get()} throws an
 * {@link ExecutionException} wrapping
 * {@link InvalidProducerEpochException}, and its next
 * {@code commitTransaction()} throws {@link InvalidProducerEpochException}
 * directly (unwrapped) -- NOT {@link ProducerFencedException}, which a lot
 * of older Kafka tutorials name as "the" fencing exception. Both classes
 * exist in this client version and both extend the same
 * {@code ApplicationRecoverableException} base -- they are siblings, not a
 * subtype relationship, so catching one does not catch the other. This app
 * catches both, and reports honestly which one it actually received.
 */
public final class FencingDemoApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "txn-lab-fencing");
        String transactionalId = LabConfig.get("transactionalId", "txn-lab-fencing-demo");
        String instanceName = LabConfig.get("instanceName", "instance-A");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        System.out.printf("FencingDemoApp | instanceName=%s | transactionalId=%s%n", instanceName, transactionalId);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("[%s] initTransactions() -- if an earlier instance is holding this transactional.id, its producer epoch is fenced NOW.%n", instanceName);
            producer.initTransactions();

            producer.beginTransaction();
            OrderEvent first = new OrderEvent(instanceName + "-record-1", "CUSTOMER-FENCE", 1.0);
            RecordMetadata firstMeta = producer.send(new ProducerRecord<>(topic, instanceName, first.toWireFormat())).get();
            System.out.printf("[%s] sent record 1 | partition=%d | offset=%d%n", instanceName, firstMeta.partition(), firstMeta.offset());

            System.out.printf("[%s] Waiting for Enter. If you are instance A, start instance B now (same transactionalId), THEN press Enter here.%n", instanceName);
            new BufferedReader(new InputStreamReader(System.in)).readLine();

            System.out.printf("[%s] attempting a second send + commitTransaction()...%n", instanceName);
            OrderEvent second = new OrderEvent(instanceName + "-record-2", "CUSTOMER-FENCE", 2.0);
            try {
                RecordMetadata secondMeta = producer.send(new ProducerRecord<>(topic, instanceName, second.toWireFormat())).get();
                System.out.printf("[%s] sent record 2 | partition=%d | offset=%d%n", instanceName, secondMeta.partition(), secondMeta.offset());
                producer.commitTransaction();
                System.out.printf("[%s] commitTransaction() SUCCEEDED -- this instance was never fenced.%n", instanceName);
            } catch (ExecutionException e) {
                reportIfFencing(instanceName, transactionalId, e.getCause() != null ? e.getCause() : e);
            } catch (InvalidProducerEpochException | ProducerFencedException e) {
                reportIfFencing(instanceName, transactionalId, e);
            }
        } catch (Exception e) {
            System.out.printf("[%s] Unexpected exception (not fencing): %s%n", instanceName, e);
            throw e;
        }
    }

    private static void reportIfFencing(String instanceName, String transactionalId, Throwable cause) {
        if (cause instanceof InvalidProducerEpochException || cause instanceof ProducerFencedException) {
            System.out.printf("[%s] FENCED: %s: %s%n", instanceName, cause.getClass().getName(), cause.getMessage());
            System.out.printf("[%s] This producer instance's epoch is no longer authoritative for transactionalId=%s. Per the Kafka client contract, this producer is now unusable and MUST be closed (never retried) -- see the lab README's Experiment 7 for why.%n",
                    instanceName, transactionalId);
        } else {
            System.out.printf("[%s] Unexpected exception (not fencing): %s%n", instanceName, cause);
            throw new RuntimeException(cause);
        }
    }
}
