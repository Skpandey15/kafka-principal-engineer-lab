package com.kafkalab.transactions.eos;

import com.kafkalab.transactions.support.FailurePoint;
import com.kafkalab.transactions.support.LabConfig;
import com.kafkalab.transactions.support.OrderEvent;
import com.kafkalab.transactions.support.SimulatedCrashException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sections 14-16's experiment: the atomic version of
 * {@link NonTransactionalConsumeTransformProduceApp} -- one Kafka
 * transaction per poll batch, wrapping BOTH the output record(s) AND the
 * input offset(s) as a single atomic unit via
 * {@code sendOffsetsToTransaction()}.
 *
 * <h2>The loop, per non-empty poll batch</h2>
 * <pre>
 * beginTransaction()
 * for each record in the batch:
 *     transform and send() to the output topic (still inside the open transaction)
 * sendOffsetsToTransaction(inputOffsets, consumer.groupMetadata())
 * maybeCrash(BEFORE_COMMIT)
 * commitTransaction()
 * maybeCrash(AFTER_COMMIT)
 * </pre>
 *
 * <p>Two crash points make Sections 15-16's experiments deterministic (see
 * {@link FailurePoint}):
 * <ul>
 *   <li>{@code BEFORE_COMMIT} -- the transaction is left open/hanging on the
 *       coordinator. On restart, THIS SAME {@code transactional.id}'s next
 *       {@code initTransactions()} call aborts that hanging transaction
 *       automatically (this is part of what {@code initTransactions()}
 *       does, not extra code this app has to write) before bumping the
 *       epoch and proceeding -- so the output never becomes visible to a
 *       {@code read_committed} consumer, and the input offsets were never
 *       committed either, so the same input batch is reprocessed. This is
 *       correct, not a bug.</li>
 *   <li>{@code AFTER_COMMIT} -- everything (output + input offsets) is
 *       already durably committed together by the time this throws. On
 *       restart, the consumer resumes strictly after the committed offset
 *       -- this batch is never reprocessed.</li>
 * </ul>
 */
public final class TransactionalConsumeTransformProduceApp {

    public record Config(
            String bootstrapServers,
            String inputTopic,
            String outputTopic,
            String groupId,
            String transactionalId,
            FailurePoint failurePoint) {

        public static Config fromSystemProperties() {
            return new Config(
                    LabConfig.bootstrapServers(),
                    LabConfig.get("inputTopic", "txn-lab-pipeline-input"),
                    LabConfig.get("outputTopic", "txn-lab-pipeline-output"),
                    LabConfig.get("groupId", "txn-lab-pipeline-transactional"),
                    LabConfig.get("transactionalId", "txn-lab-pipeline-processor-1"),
                    FailurePoint.valueOf(LabConfig.get("failurePoint", "NONE")));
        }
    }

    public static void main(String[] args) {
        Config config = Config.fromSystemProperties();
        KafkaConsumer<String, String> consumer = newConsumer(config);
        KafkaProducer<String, String> producer = newProducer(config);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        System.out.printf("TransactionalConsumeTransformProduceApp | input=%s | output=%s | group=%s | transactionalId=%s | failurePoint=%s%n",
                config.inputTopic(), config.outputTopic(), config.groupId(), config.transactionalId(), config.failurePoint());

        try {
            producer.initTransactions();
            run(config, consumer, producer, shuttingDown, 0);
            consumer.close();
            producer.close();
        } catch (SimulatedCrashException e) {
            System.out.printf("SIMULATED_CRASH at %s | %s%n", e.failurePoint(), e.getMessage());
            System.out.println("Exiting abruptly WITHOUT closing the consumer/producer.");
            System.exit(1);
        }
    }

    /**
     * The reusable core loop. {@code producer.initTransactions()} MUST have
     * already been called before this method runs (callers control that so
     * a restart-simulating test can call it exactly once per fresh
     * producer instance, matching real restart semantics). Returns the
     * number of input records fully committed (output + offset, atomically)
     * before stopping.
     */
    public static int run(Config config, KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer,
            AtomicBoolean shuttingDown, int maxRecords) {
        consumer.subscribe(List.of(config.inputTopic()));
        int handled = 0;
        while (!shuttingDown.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                if (maxRecords > 0 && handled >= maxRecords) {
                    break;
                }
                continue;
            }

            producer.beginTransaction();
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new LinkedHashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                OrderEvent input = OrderEvent.parse(record.value());
                OrderEvent output = transform(input);
                RecordMetadata metadata = send(producer, config.outputTopic(), output);
                System.out.printf("  (in transaction) consumed inputOffset=%d eventId=%s -> produced outputPartition=%d outputOffset=%d%n",
                        record.offset(), input.eventId(), metadata.partition(), metadata.offset());
                offsetsToCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }

            producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());
            maybeCrashBatch(config, FailurePoint.BEFORE_COMMIT);

            producer.commitTransaction();
            System.out.printf("commitTransaction() -- committed %d output record(s) AND %d input offset(s) atomically.%n",
                    records.count(), offsetsToCommit.size());
            maybeCrashBatch(config, FailurePoint.AFTER_COMMIT);

            handled += records.count();
            if (maxRecords > 0 && handled >= maxRecords) {
                break;
            }
        }
        return handled;
    }

    private static void maybeCrashBatch(Config config, FailurePoint point) {
        if (config.failurePoint() == point) {
            throw new SimulatedCrashException(point, "transactionalId=" + config.transactionalId());
        }
    }

    private static OrderEvent transform(OrderEvent input) {
        return new OrderEvent("PROCESSED-" + input.eventId(), input.customerId(), input.amount() * 1.0);
    }

    private static RecordMetadata send(KafkaProducer<String, String> producer, String topic, OrderEvent event) {
        try {
            return producer.send(new ProducerRecord<>(topic, event.eventId(), event.toWireFormat())).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static KafkaConsumer<String, String> newConsumer(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Best practice for a transactional consume-transform-produce
        // stage: if the INPUT topic is itself ever written to
        // transactionally by something upstream, this stage should not
        // read uncommitted (possibly-aborted) input in the first place.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> newProducer(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, config.transactionalId());
        return new KafkaProducer<>(props);
    }
}
