package com.kafkalab.transactions.eos;

import com.kafkalab.transactions.support.LabConfig;
import com.kafkalab.transactions.support.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Section 13's experiment: the classic consume-transform-produce pipeline
 * WITHOUT Kafka transactions -- consume, transform, produce output,
 * COMMIT OFFSET, each step separate and each one capable of succeeding
 * while the next one fails.
 *
 * <p>This class's crash-injection point sits exactly where the real-world
 * bug lives: AFTER the output record has been durably produced, but BEFORE
 * the input offset is committed. On restart, the consumer resumes from the
 * last COMMITTED offset -- which is still before this record -- so the
 * same input is fetched and transformed again, producing a SECOND output
 * record for the same logical event. This is at-least-once delivery
 * doing exactly what it promises (no input is ever lost) at the cost it
 * always carries (a successfully-processed input can still be reprocessed).
 *
 * <p>This is the direct extension of WP-06's (lab-05's) own
 * {@code AFTER_PROCESS_BEFORE_COMMIT} failure window into a pipeline whose
 * "processing" is itself a Kafka produce -- see the lab README's
 * Experiment 8 for real captured evidence of the resulting duplicate.
 */
public final class NonTransactionalConsumeTransformProduceApp {

    public record Config(
            String bootstrapServers,
            String inputTopic,
            String outputTopic,
            String groupId,
            boolean crashAfterProduce,
            String crashAfterEventId) {

        public static Config fromSystemProperties() {
            return new Config(
                    LabConfig.bootstrapServers(),
                    LabConfig.get("inputTopic", "txn-lab-pipeline-input"),
                    LabConfig.get("outputTopic", "txn-lab-pipeline-output"),
                    LabConfig.get("groupId", "txn-lab-pipeline-non-transactional"),
                    LabConfig.getBoolean("crashAfterProduce", false),
                    LabConfig.get("crashAfterEventId", ""));
        }
    }

    public static void main(String[] args) {
        Config config = Config.fromSystemProperties();
        KafkaConsumer<String, String> consumer = newConsumer(config);
        KafkaProducer<String, String> producer = newProducer(config);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        System.out.printf("NonTransactionalConsumeTransformProduceApp | input=%s | output=%s | group=%s | crashAfterProduce=%s%n",
                config.inputTopic(), config.outputTopic(), config.groupId(), config.crashAfterProduce());

        try {
            run(config, consumer, producer, shuttingDown, 0);
            consumer.close();
            producer.close();
        } catch (CrashPoint e) {
            System.out.printf("SIMULATED_CRASH | %s%n", e.getMessage());
            System.out.println("Exiting abruptly WITHOUT committing the offset for the record just produced -- and WITHOUT closing the consumer/producer.");
            System.exit(1);
        }
    }

    /**
     * The reusable core loop. Returns the number of input records fully
     * processed (produced AND committed) before stopping. {@code maxRecords}
     * of 0 means "run until {@code shuttingDown}".
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
            for (ConsumerRecord<String, String> record : records) {
                OrderEvent input = OrderEvent.parse(record.value());
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                OrderEvent output = transform(input);
                var metadata = send(producer, config.outputTopic(), output);
                System.out.printf("consumed inputOffset=%d eventId=%s -> produced outputPartition=%d outputOffset=%d%n",
                        record.offset(), input.eventId(), metadata.partition(), metadata.offset());

                boolean shouldCrash = config.crashAfterProduce()
                        && (config.crashAfterEventId().isEmpty() || config.crashAfterEventId().equals(input.eventId()));
                if (shouldCrash) {
                    throw new CrashPoint("after producing output for eventId=" + input.eventId() + ", before committing input offset " + (record.offset() + 1));
                }

                consumer.commitSync(Map.of(tp, new OffsetAndMetadata(record.offset() + 1)));
                System.out.printf("committed inputOffset=%d for eventId=%s%n", record.offset() + 1, input.eventId());
                handled++;
                if (maxRecords > 0 && handled >= maxRecords) {
                    return handled;
                }
            }
        }
        return handled;
    }

    private static org.apache.kafka.clients.producer.RecordMetadata send(KafkaProducer<String, String> producer, String topic, OrderEvent event) {
        try {
            return producer.send(new ProducerRecord<>(topic, event.eventId(), event.toWireFormat())).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static OrderEvent transform(OrderEvent input) {
        return new OrderEvent("PROCESSED-" + input.eventId(), input.customerId(), input.amount() * 1.0);
    }

    private static KafkaConsumer<String, String> newConsumer(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> newProducer(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /** This pipeline's own crash-injection signal -- see the lab README's Experiment 8. */
    public static final class CrashPoint extends RuntimeException {
        public CrashPoint(String message) {
            super(message);
        }
    }
}
