package com.kafkalab.schemaevolution.avro;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.math.BigDecimal;
import java.util.Properties;

/**
 * Section 12's experiment: can one Kafka topic safely contain multiple
 * event types?
 *
 * <p>With the DEFAULT {@code TopicNameStrategy}, the subject for every
 * record's value on topic {@code t} is always {@code t-value} -- so
 * producing BOTH {@code OrderEvent} and the structurally unrelated
 * {@code PaymentEvent} to the same topic would force them to share ONE
 * subject's compatibility history, which makes no sense for two
 * genuinely different record types (compatibility checking one against
 * the other's schema history is meaningless, not just inconvenient).
 *
 * <p>With {@code RecordNameStrategy}, the subject is derived from the
 * Avro record's OWN fully-qualified name instead of the topic -- so
 * {@code OrderEvent} and {@code PaymentEvent} naturally get their OWN
 * independent subjects and compatibility histories, even while sharing
 * one physical topic. This app demonstrates that concretely: real
 * subjects registered, real evidence of two independent histories.
 *
 * <h2>The tradeoff, not an absolute rule</h2>
 * Multiple event types on one topic can be reasonable (e.g., a single
 * "domain events" topic for a service preserves cross-event-type
 * ordering within one partition) but costs consumers the ability to
 * subscribe to just one event type without filtering, and costs the
 * topic-level retention/partitioning tuning that a single, homogeneous
 * event type could be tuned around specifically. Neither choice is
 * universally correct.
 */
public final class NamingStrategyDemoApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String topic = LabConfig.get("topic", "multi-event-topic");

        Schema orderSchema = new Schema.Parser().parse(new File("src/main/avro/order-event-v1.avsc"));
        Schema paymentSchema = new Schema.Parser().parse(new File("src/main/avro/payment-event-v1.avsc"));

        // Subjects under RecordNameStrategy are named after the writer's
        // record full name -- registering here (rather than relying on
        // auto-register) so the resulting subject names are visible
        // immediately, before any send() happens.
        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        String orderSubject = orderSchema.getFullName();
        String paymentSubject = paymentSchema.getFullName();
        registryClient.register(orderSubject, orderSchema);
        registryClient.register(paymentSubject, paymentSchema);
        System.out.println("Registered subjects under RecordNameStrategy: " + orderSubject + ", " + paymentSubject);
        System.out.println("(compare to TopicNameStrategy, which would force BOTH under the single subject '" + topic + "-value')");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
        props.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        GenericData.Record orderRecord = new GenericData.Record(orderSchema);
        orderRecord.put("orderId", "O-NAMING-DEMO");
        orderRecord.put("customerId", "C-501");
        orderRecord.put("amount", new BigDecimal("10.00"));

        GenericData.Record paymentRecord = new GenericData.Record(paymentSchema);
        paymentRecord.put("paymentId", "P-NAMING-DEMO");
        paymentRecord.put("orderId", "O-NAMING-DEMO");
        paymentRecord.put("amount", new BigDecimal("10.00"));

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            var orderMeta = producer.send(new ProducerRecord<>(topic, "O-NAMING-DEMO", (GenericRecord) orderRecord)).get();
            System.out.printf("Produced OrderEvent  | topic=%s | partition=%d | offset=%d%n", topic, orderMeta.partition(), orderMeta.offset());
            var paymentMeta = producer.send(new ProducerRecord<>(topic, "O-NAMING-DEMO", (GenericRecord) paymentRecord)).get();
            System.out.printf("Produced PaymentEvent | topic=%s | partition=%d | offset=%d%n", topic, paymentMeta.partition(), paymentMeta.offset());
        }

        System.out.println("Both event types now live on the SAME topic (" + topic + "), each with its OWN independent subject/compatibility history.");
    }
}
