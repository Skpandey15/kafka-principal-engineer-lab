package com.kafkalab.transactions.producer;

import com.kafkalab.transactions.support.LabConfig;
import com.kafkalab.transactions.support.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Sections 3-4's experiment: an idempotent producer burst, whose real
 * effect on the broker (Producer ID, producer epoch, and the per-partition
 * sequence number this app's sends occupy) is inspected AFTER this app
 * exits, using the broker's own tool:
 * <pre>
 *   docker exec kafka-broker-1 /opt/kafka/bin/kafka-transactions.sh \
 *     --bootstrap-server kafka-broker-1:19092 \
 *     describe-producers --topic &lt;topic&gt; --partition &lt;n&gt;
 * </pre>
 * There is no public {@code KafkaProducer} API to read back its own PID or
 * sequence numbers -- those are protocol-level details the client library
 * manages internally and never exposes to application code. This app
 * therefore does not print them itself; the lab README's Experiment 2
 * captures real {@code describe-producers} output taken right after a run,
 * which is the only place this evidence honestly comes from.
 *
 * <p>All idempotence-relevant configuration is printed explicitly below,
 * using the values this repository verified directly against the pinned
 * {@code kafka-clients:4.3.1} jar's {@code ProducerConfig} defaults (not
 * copied from documentation written for an older client version) -- see
 * the lab README's "Modern client defaults" note. Notably,
 * {@code enable.idempotence} already defaults to {@code true} in this
 * client version; the {@code -Pidempotence=false} override exists so this
 * app can also demonstrate the OFF state on request.
 */
public final class IdempotentProducerApp {

    public static void main(String[] args) {
        String bootstrapServers = LabConfig.bootstrapServers();
        String topic = LabConfig.get("topic", "txn-lab-idempotent");
        int count = LabConfig.getInt("count", 10);
        boolean idempotence = LabConfig.getBoolean("idempotence", true);
        // Every record is sent to the SAME partition (an explicit key that
        // hashes consistently, in practice fixed here to partition 0
        // directly) so a single `describe-producers --partition 0` call
        // shows the full, unbroken sequence-number run this burst produced.
        int partition = 0;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);

        System.out.println("IdempotentProducerApp -- resolved configuration relevant to idempotence:");
        System.out.println("  enable.idempotence            = " + idempotence + " (client default in kafka-clients 4.3.1: true)");
        System.out.println("  acks                           = all (forced when idempotence is on; also the client's own default either way in 4.3.1)");
        System.out.println("  retries                        = 2147483647 / Integer.MAX_VALUE (the client default -- bounded in practice by delivery.timeout.ms, not a retry count)");
        System.out.println("  max.in.flight.requests.per.connection = 5 (the client default; idempotence requires <= 5, NOT <= 1 as older documentation for pre-2.5 Kafka often states)");
        System.out.println("  transactional.id               = (unset) -- idempotence alone does not require one; see TransactionalProducerApp for the transactional case");
        System.out.println();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 1; i <= count; i++) {
                String eventId = "Order-" + (1000 + i);
                OrderEvent event = new OrderEvent(eventId, "CUSTOMER-" + i, 10.0 * i);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, eventId, event.toWireFormat());
                futures.add(producer.send(record));
            }
            for (int i = 0; i < futures.size(); i++) {
                try {
                    RecordMetadata metadata = futures.get(i).get();
                    System.out.printf("eventId=Order-%d | topic=%s | partition=%d | offset=%d | key=Order-%d%n",
                            1000 + i + 1, metadata.topic(), metadata.partition(), metadata.offset(), 1000 + i + 1);
                } catch (Exception e) {
                    System.out.printf("eventId=Order-%d | SEND_FAILED | %s%n", 1000 + i + 1, e);
                }
            }
        }

        System.out.println();
        System.out.printf("Done. Inspect this burst's PID/epoch/sequence with:%n  docker exec kafka-broker-1 /opt/kafka/bin/kafka-transactions.sh --bootstrap-server kafka-broker-1:19092 describe-producers --topic %s --partition %d%n",
                topic, partition);
    }
}
