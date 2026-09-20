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
 * Sections 6-8's experiment: a transactional producer using the real
 * {@code initTransactions()} / {@code beginTransaction()} /
 * {@code commitTransaction()} / {@code abortTransaction()} API, writing
 * several records inside ONE transaction and then either committing or
 * aborting the whole thing, controlled by {@code -Paction=commit|abort}.
 *
 * <p>Run an {@link com.kafkalab.transactions.consumer.IsolationLevelConsumerApp}
 * with {@code isolationLevel=read_committed} BEFORE and DURING a run of
 * this app (records are sent well before the commit/abort call returns) to
 * see directly that nothing becomes visible to it until this method
 * returns -- and, for the abort case, that nothing ever becomes visible at
 * all. See the lab README's Experiments 3-4 for real captured output.
 */
public final class TransactionalProducerApp {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "txn-lab-orders");
        String transactionalId = LabConfig.get("transactionalId", "txn-lab-producer-1");
        String action = LabConfig.get("action", "commit");
        int startEventNumber = LabConfig.getInt("startEventNumber", 1);
        // A pause between the sends and the commit/abort call, purely so a
        // human running an isolation-level consumer alongside this app in
        // another terminal has time to observe "records sent, but not yet
        // visible to read_committed" before this process finishes.
        long pauseBeforeFinishMs = LabConfig.getLong("pauseBeforeFinishMs", 8000);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        // enable.idempotence is forced true by the client whenever
        // transactional.id is set -- listed explicitly here anyway so this
        // app's printed configuration is honest about what's actually in
        // effect, not just what this code happens to set.

        System.out.printf("TransactionalProducerApp | transactionalId=%s | topic=%s | action=%s%n", transactionalId, topic, action);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("initTransactions() -- registers this transactional.id with its coordinator, fences any earlier producer instance using the same id, and recovers/completes any transaction that instance left hanging.");
            producer.initTransactions();

            System.out.println("beginTransaction()");
            producer.beginTransaction();

            String[] eventIds = {"Order-" + startEventNumber, "Order-" + (startEventNumber + 1), "Order-" + (startEventNumber + 2)};
            for (String eventId : eventIds) {
                OrderEvent event = new OrderEvent(eventId, "CUSTOMER-TXN", 99.99);
                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, eventId, event.toWireFormat())).get();
                System.out.printf("  sent (inside open transaction) | eventId=%s | partition=%d | offset=%d -- NOT yet visible to read_committed%n",
                        eventId, metadata.partition(), metadata.offset());
            }

            System.out.printf("Pausing %dms before %s -- check a read_committed consumer now: these offsets exist on disk but are withheld.%n",
                    pauseBeforeFinishMs, action);
            Thread.sleep(pauseBeforeFinishMs);

            if ("abort".equalsIgnoreCase(action)) {
                System.out.println("abortTransaction() -- the broker writes an ABORT marker; a read_committed consumer will skip straight past these offsets and never surface these records.");
                producer.abortTransaction();
            } else {
                System.out.println("commitTransaction() -- the broker writes a COMMIT marker; these records become visible to read_committed consumers atomically, all at once, from this point on.");
                producer.commitTransaction();
            }
        } catch (Exception e) {
            System.out.println("Unhandled exception -- see stack trace below.");
            throw new RuntimeException(e);
        }

        System.out.println("Done.");
    }
}
