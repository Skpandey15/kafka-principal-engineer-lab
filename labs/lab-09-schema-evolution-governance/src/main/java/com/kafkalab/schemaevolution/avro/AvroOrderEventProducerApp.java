package com.kafkalab.schemaevolution.avro;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;

/**
 * Sections 5-7's experiment: a real Avro producer against the real,
 * running Confluent Schema Registry, using {@link GenericRecord} rather
 * than Avro-generated {@code SpecificRecord} classes -- deliberately, to
 * avoid the extra build-time code-generation step this lab's schema-
 * evolution focus does not need (see the README, "Why GenericRecord, not
 * SpecificRecord").
 *
 * <p>{@code -Pschema} selects which {@code .avsc} file under
 * {@code src/main/avro/} this run registers/uses -- {@code v1}, {@code v2}
 * (compatible), or one of the deliberately-incompatible variants for
 * Section 7's rejection experiments.
 *
 * <p>{@code auto.register.schemas} is explicitly set to {@code false}
 * (the client default is {@code true}) so schema registration in this lab
 * is always a DELIBERATE, observable step this app performs itself via
 * {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient#register}
 * before producing -- not something that happens silently on first
 * {@code send()}. This matches how a real, governed pipeline should
 * behave: registration is a reviewed, gated action (Section 23), not an
 * implicit side effect of running a producer.
 */
public final class AvroOrderEventProducerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String topic = LabConfig.get("topic", "avro-orders");
        String schemaName = LabConfig.get("schema", "v1");

        Schema schema = SchemaFiles.load(schemaName);
        System.out.println("Using schema file: " + SchemaFiles.fileFor(schemaName).getPath());
        System.out.println("Schema: " + schema.toString(true));

        String subject = topic + "-value";
        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        // Explicit registration BEFORE producing -- matches the
        // Producer -> schema -> Schema Registry -> schema ID -> serialize
        // flow (Section 9) literally, rather than letting the serializer
        // register implicitly as a side effect of the first send().
        int schemaId = registryClient.register(subject, schema);
        System.out.printf("Registered against subject=%s -> schema ID=%d%n", subject, schemaId);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Registration already happened explicitly, above -- the
        // serializer only needs to look up the ID for this exact schema
        // (an exact match, since it is the same schema object just
        // registered), never register on its own.
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        // Lets GenericRecord carry BigDecimal for the `amount` field's
        // decimal logical type, instead of a raw ByteBuffer -- see the
        // README's "Why not double" note for why this matters for money.
        props.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        GenericRecord record = buildRecord(schema);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, (String) record.get("orderId"), record)).get();
            System.out.printf("Produced | topic=%s | partition=%d | offset=%d | schema=%s%n",
                    metadata.topic(), metadata.partition(), metadata.offset(), schemaName);
        }
    }

    private static GenericRecord buildRecord(Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            switch (field.name()) {
                case "orderId" -> record.put(field.name(),
                        field.schema().getType() == Schema.Type.INT ? (int) (System.currentTimeMillis() % 100000) : "O-" + System.currentTimeMillis());
                case "customerId" -> record.put(field.name(), "C-501");
                case "custId" -> record.put(field.name(), "C-501");
                case "amount" -> {
                    if (field.schema().getLogicalType() != null) {
                        record.put(field.name(), new BigDecimal("1250.00"));
                    } else {
                        record.put(field.name(), "1250.00");
                    }
                }
                case "currency" -> record.put(field.name(), "USD");
                default -> throw new IllegalStateException("Unhandled field in demo record builder: " + field.name());
            }
        }
        return record;
    }

    /** Resolves {@code -Pschema=<name>} to its .avsc file and parses it. */
    static final class SchemaFiles {
        private static final Map<String, String> FILES = Map.of(
                "v1", "order-event-v1.avsc",
                "v2", "order-event-v2.avsc",
                "v2-no-default", "order-event-v3-required-no-default.avsc",
                "v2-renamed", "order-event-v2-renamed-field.avsc",
                "v2-type-changed", "order-event-v2-type-changed.avsc",
                "v2-orderid-type-changed", "order-event-v2-orderid-type-changed.avsc",
                "v2-field-removed", "order-event-v2-field-removed.avsc",
                "v3-required-no-default", "order-event-v3-required-no-default.avsc");

        static File fileFor(String schemaName) {
            String fileName = FILES.get(schemaName);
            if (fileName == null) {
                throw new IllegalArgumentException("Unknown -Pschema value: " + schemaName + " (known: " + FILES.keySet() + ")");
            }
            return new File("src/main/avro/" + fileName);
        }

        static Schema load(String schemaName) throws Exception {
            return new Schema.Parser().parse(fileFor(schemaName));
        }
    }
}
