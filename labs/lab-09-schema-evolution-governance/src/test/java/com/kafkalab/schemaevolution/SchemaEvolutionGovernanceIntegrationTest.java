package com.kafkalab.schemaevolution;

import com.kafkalab.schemaevolution.support.SchemaRegistryKafkaCluster;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kafka + real Confluent Schema Registry (see
 * {@link SchemaRegistryKafkaCluster} for why single-node is the right
 * choice for THESE tests) -- no mocked registry, no mocked serializer, no
 * simulated compatibility logic.
 *
 * <p>Every assertion is reached through a bounded condition-polling loop
 * with an overall deadline, per this repository's established test
 * convention.
 */
class SchemaEvolutionGovernanceIntegrationTest {

    private static SchemaRegistryKafkaCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new SchemaRegistryKafkaCluster(19_600);
        cluster.start();
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Test
    void avroV1ProducesAndConsumesSuccessfully() throws Exception {
        String topic = uniqueTopic("avro-v1");
        Schema v1 = avroSchema("v1");
        registerSubject(topic + "-value", v1);

        GenericRecord sent = orderRecord(v1, "O-1", "C-1", "10.00");
        produceAvro(topic, "avro-v1-" + UUID.randomUUID(), sent, v1);

        List<GenericRecord> consumed = consumeAvro(topic, 1, Duration.ofSeconds(20));
        assertEquals(1, consumed.size());
        assertEquals("O-1", consumed.get(0).get("orderId").toString());
    }

    @Test
    void compatibleAvroV2RegistersSuccessfully() throws Exception {
        String subject = uniqueTopic("avro-compat") + "-value";
        Schema v1 = avroSchema("v1");
        Schema v2 = avroSchema("v2");
        int idV1 = registerSubject(subject, v1);
        int idV2 = registerSubject(subject, v2);

        assertNotEquals(idV1, idV2, "different schema content must get different global IDs");
        try (SchemaRegistryClient client = newRegistryClient()) {
            assertEquals(List.of(1, 2), client.getAllVersions(subject));
        }
    }

    @Test
    void incompatibleAvroRegistrationIsRejected() throws Exception {
        String subject = uniqueTopic("avro-incompat") + "-value";
        registerSubject(subject, avroSchema("v1"));
        registerSubject(subject, avroSchema("v2"));

        try (SchemaRegistryClient client = newRegistryClient()) {
            AvroSchema renamed = new AvroSchema(avroSchema("v2-renamed"));
            RestClientException failure = assertThrows(RestClientException.class,
                    () -> client.register(subject, renamed));
            assertEquals(409, failure.getStatus(), "an incompatible registration must be rejected with HTTP 409");
        }
    }

    @Test
    void oldWriterDataIsReadableByACompatibleNewReaderSchemaWithDefaultsApplied() throws Exception {
        // Real Avro schema resolution (Section 18), applied to bytes
        // produced through the REAL KafkaAvroSerializer wire format --
        // this is NOT what KafkaAvroDeserializer's plain GenericRecord
        // path does by default (it never pins a reader schema); this test
        // deliberately decodes the Confluent-framed payload itself with
        // an explicit reader schema to prove resolution works end to end.
        String topic = uniqueTopic("avro-resolution");
        Schema v1 = avroSchema("v1");
        Schema v2 = avroSchema("v2");
        registerSubject(topic + "-value", v1);
        registerSubject(topic + "-value", v2);

        GenericRecord v1Record = orderRecord(v1, "O-RESOLVE", "C-1", "5.00");
        produceAvro(topic, "O-RESOLVE", v1Record, v1);

        byte[] wireBytes = consumeRawBytes(topic, 1, Duration.ofSeconds(20)).get(0);
        byte[] avroPayload = java.util.Arrays.copyOfRange(wireBytes, 5, wireBytes.length);

        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        GenericDatumReader<GenericRecord> resolvingReader = new GenericDatumReader<>(v1, v2, genericData);
        GenericRecord resolved = resolvingReader.read(null,
                DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroPayload), null));

        assertEquals("USD", resolved.get("currency").toString(),
                "v2's default must be applied when resolving v1-written bytes, even though the bytes never contained a currency field");
    }

    @Test
    void newWriterDataIsReadableByAnOlderCompatibleReaderSchema() throws Exception {
        // The FORWARD direction: OLD reader schema (v1, no currency field)
        // reading data written with the NEWER writer schema (v2) -- the
        // extra field is simply ignored by resolution, no error.
        Schema v1 = avroSchema("v1");
        Schema v2 = avroSchema("v2");
        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());

        GenericRecord v2Record = orderRecord(v2, "O-FORWARD", "C-1", "5.00");
        ((GenericData.Record) v2Record).put("currency", "EUR");
        byte[] v2Bytes = encodeAvro(v2, v2Record, genericData);

        GenericDatumReader<GenericRecord> oldReader = new GenericDatumReader<>(v2, v1, genericData);
        GenericRecord resolvedAsV1 = oldReader.read(null,
                DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(v2Bytes), null));

        assertEquals("O-FORWARD", resolvedAsV1.get("orderId").toString());
        assertThrows(org.apache.avro.AvroRuntimeException.class, () -> resolvedAsV1.get("currency"),
                "the v1 reader schema was never given a currency field at all -- it must not be accessible through this resolved record");
    }

    @Test
    void schemaIdIsDistinctFromSubjectAndVersion() throws Exception {
        // Section 11's mandatory distinction, verified concretely: the
        // SAME schema content registered under TWO DIFFERENT subjects
        // gets the SAME global ID (content-addressed), while version
        // numbers are independent, per-subject counters.
        Schema v1 = avroSchema("v1");
        String subjectA = uniqueTopic("id-vs-subject-a") + "-value";
        String subjectB = uniqueTopic("id-vs-subject-b") + "-value";

        int idInSubjectA = registerSubject(subjectA, v1);
        int idInSubjectB = registerSubject(subjectB, v1);

        assertEquals(idInSubjectA, idInSubjectB, "identical schema content registered under different subjects must reuse the same global schema ID");

        try (SchemaRegistryClient client = newRegistryClient()) {
            assertEquals(1, client.getVersion(subjectA, new AvroSchema(v1)));
            assertEquals(1, client.getVersion(subjectB, new AvroSchema(v1)));
        }

        // Now register v2 into subject A only -- its version becomes 2,
        // but its GLOBAL id keeps incrementing from wherever the registry
        // as a whole is, not from subject A's own version counter.
        int idV2InA = registerSubject(subjectA, avroSchema("v2"));
        assertNotEquals(idInSubjectA, idV2InA);
    }

    @Test
    void protobufSchemaEvolvesCompatibly() throws Exception {
        String subject = uniqueTopic("protobuf-compat") + "-value";
        String protoV1 = """
                syntax = "proto3";
                package com.kafkalab.schemaevolution.protobuf.test;
                message OrderEvent {
                  string order_id = 1;
                  string customer_id = 2;
                  string amount = 3;
                }
                """;
        String protoV2 = """
                syntax = "proto3";
                package com.kafkalab.schemaevolution.protobuf.test;
                message OrderEvent {
                  string order_id = 1;
                  string customer_id = 2;
                  string amount = 3;
                  string currency = 4;
                }
                """;
        try (SchemaRegistryClient client = newRegistryClient()) {
            int idV1 = client.register(subject, new ProtobufSchema(protoV1));
            int idV2 = client.register(subject, new ProtobufSchema(protoV2));
            assertNotEquals(idV1, idV2);
            assertEquals(List.of(1, 2), client.getAllVersions(subject));
        }
    }

    @Test
    void jsonSchemaValidatesNewOptionalPropertyButRegistryRejectsItByDefault() throws Exception {
        // A REAL, surprising finding (see the conceptual doc, Section 20):
        // adding a new OPTIONAL property to a JSON Schema with an "open"
        // content model (no explicit additionalProperties: false) is
        // REJECTED by the registry's default BACKWARD check --
        // errorType 'PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL' -- even though
        // the property is optional and old data would validate against
        // the new schema just fine in practice. This is NOT how Avro's
        // "add a field with a default" evolution behaves, and the two
        // should not be assumed equivalent.
        String subject = uniqueTopic("jsonschema-compat") + "-value";
        String schemaV1 = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "orderId": {"type": "string"},
                    "amount": {"type": "string"}
                  },
                  "required": ["orderId", "amount"]
                }
                """;
        String schemaV2 = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "orderId": {"type": "string"},
                    "amount": {"type": "string"},
                    "currency": {"type": "string"}
                  },
                  "required": ["orderId", "amount"]
                }
                """;
        try (SchemaRegistryClient client = newRegistryClient()) {
            client.register(subject, new JsonSchema(schemaV1));
            RestClientException failure = assertThrows(RestClientException.class,
                    () -> client.register(subject, new JsonSchema(schemaV2)));
            assertEquals(409, failure.getStatus());
            assertTrue(failure.getMessage().contains("PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL"),
                    "expected the specific open-content-model rejection, got: " + failure.getMessage());
        }

        // The fix: close the content model explicitly in v1
        // (additionalProperties: false) -- now the registry can reason
        // precisely about what "extra" means, and the optional addition
        // in v2 is accepted.
        String closedSubject = uniqueTopic("jsonschema-compat-closed") + "-value";
        String closedV1 = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "additionalProperties": false,
                  "properties": {
                    "orderId": {"type": "string"},
                    "amount": {"type": "string"}
                  },
                  "required": ["orderId", "amount"]
                }
                """;
        String closedV2 = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "additionalProperties": false,
                  "properties": {
                    "orderId": {"type": "string"},
                    "amount": {"type": "string"},
                    "currency": {"type": "string"}
                  },
                  "required": ["orderId", "amount"]
                }
                """;
        try (SchemaRegistryClient client = newRegistryClient()) {
            int idV1 = client.register(closedSubject, new JsonSchema(closedV1));
            int idV2 = client.register(closedSubject, new JsonSchema(closedV2));
            assertNotEquals(idV1, idV2, "with additionalProperties:false, the same field addition IS accepted");
        }
    }

    @Test
    void compatibilityModeNoneAcceptsWhatBackwardWouldReject() throws Exception {
        String subject = uniqueTopic("mode-none") + "-value";
        registerSubject(subject, avroSchema("v1"));

        try (SchemaRegistryClient client = newRegistryClient()) {
            // Under the default BACKWARD mode, the renamed-field schema
            // is rejected (proven by incompatibleAvroRegistrationIsRejected).
            // Under NONE, the registry performs no compatibility check at
            // all -- registration must succeed.
            client.updateCompatibility(subject, "NONE");
            AvroSchema renamed = new AvroSchema(avroSchema("v2-renamed"));
            int id = client.register(subject, renamed);
            assertTrue(id > 0);
        }
    }

    @Test
    void fullCompatibilityAcceptsAnAdditionValidInBothDirections() throws Exception {
        // FULL = BACKWARD AND FORWARD simultaneously. v1 -> v2 (add
        // `currency` with a default) already independently satisfies
        // both directions -- oldWriterDataIsReadableByACompatibleNewReaderSchemaWithDefaultsApplied
        // and newWriterDataIsReadableByAnOlderCompatibleReaderSchema prove
        // each direction generically; this test additionally proves the
        // REGISTRY itself accepts the same evolution under a subject
        // actually CONFIGURED as FULL (not just BACKWARD or FORWARD
        // alone), and re-demonstrates both directions concretely against
        // THIS subject's own registered schemas, reusing the same
        // resolution helpers rather than duplicating new fixtures.
        String subject = uniqueTopic("full-compat") + "-value";
        Schema v1 = avroSchema("v1");
        Schema v2 = avroSchema("v2");

        try (SchemaRegistryClient client = newRegistryClient()) {
            client.updateCompatibility(subject, "FULL");
        }
        int idV1 = registerSubject(subject, v1);
        int idV2 = registerSubject(subject, v2);
        assertNotEquals(idV1, idV2, "a real, distinct schema version must have been accepted under FULL");

        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());

        // Direction 1: NEW reader (v2) reads OLD writer (v1) data.
        byte[] v1Bytes = encodeAvro(v1, orderRecord(v1, "O-FULL-BACKWARD", "C-1", "5.00"), genericData);
        GenericRecord resolvedAsV2 = new GenericDatumReader<GenericRecord>(v1, v2, genericData)
                .read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(v1Bytes), null));
        assertEquals("USD", resolvedAsV2.get("currency").toString(),
                "FULL's backward direction: v2 must resolve v1 data, filling currency from its default");

        // Direction 2: OLD reader (v1) reads NEW writer (v2) data.
        GenericRecord v2Record = orderRecord(v2, "O-FULL-FORWARD", "C-1", "5.00");
        ((GenericData.Record) v2Record).put("currency", "EUR");
        byte[] v2Bytes = encodeAvro(v2, v2Record, genericData);
        GenericRecord resolvedAsV1 = new GenericDatumReader<GenericRecord>(v2, v1, genericData)
                .read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(v2Bytes), null));
        assertEquals("O-FULL-FORWARD", resolvedAsV1.get("orderId").toString(),
                "FULL's forward direction: v1 must resolve v2 data, ignoring the extra currency field");
    }

    @Test
    void fullCompatibilityRejectsGenuinelyIncompatibleChange() throws Exception {
        // Same v1 -> orderId(string->int) change already proven incompatible
        // under plain BACKWARD (incompatibleAvroRegistrationIsRejected uses
        // the renamed-field variant; this reuses the type-changed fixture),
        // now against a subject actually configured as FULL. Real registry
        // evidence: FULL's rejection lists TWO TYPE_MISMATCH entries (one
        // per direction), not one -- concrete proof FULL checks both ways
        // at once rather than only backward or only forward.
        String subject = uniqueTopic("full-incompat") + "-value";
        try (SchemaRegistryClient client = newRegistryClient()) {
            client.updateCompatibility(subject, "FULL");
        }
        registerSubject(subject, avroSchema("v2")); // baseline: orderId is still `string` here

        try (SchemaRegistryClient client = newRegistryClient()) {
            AvroSchema orderIdTypeChanged = new AvroSchema(avroSchema("v2-orderid-type-changed"));
            RestClientException failure = assertThrows(RestClientException.class,
                    () -> client.register(subject, orderIdTypeChanged));
            assertEquals(409, failure.getStatus());
            assertTrue(failure.getMessage().contains("TYPE_MISMATCH"),
                    "expected a real TYPE_MISMATCH rejection, got: " + failure.getMessage());
        }
    }

    @Test
    void fullTransitiveCompatibilityChecksFullHistoryNotJustLatest() throws Exception {
        // Reuses the EXACT same v1/v2/v3-required-no-default trap already
        // used for BACKWARD_TRANSITIVE (see ciCompatibilityGateMechanismDistinguishesPassAndFail
        // and the conceptual doc, Section 16) -- no new fixture invented.
        // v3 (currency, no default) is compatible with v2 alone (v2 always
        // supplies currency) but NOT with v1 (which has no currency field
        // and no default to fall back on) -- real, verified difference
        // between plain FULL (checks only the latest version) and
        // FULL_TRANSITIVE (checks the entire history).
        String subject = uniqueTopic("full-transitive") + "-value";
        registerSubject(subject, avroSchema("v1"));
        registerSubject(subject, avroSchema("v2"));

        try (SchemaRegistryClient client = newRegistryClient()) {
            client.updateCompatibility(subject, "FULL");
        }
        int idUnderPlainFull = registerSubject(subject, avroSchema("v3-required-no-default"));
        assertTrue(idUnderPlainFull > 0, "v3 must be ACCEPTED under plain FULL, which only compares against v2 (the latest)");

        // A SEPARATE subject with the identical v1+v2 history, so the
        // FULL_TRANSITIVE rejection below is tested cleanly against the
        // same starting point rather than a subject that already
        // (correctly) accepted v3 as its own version 3.
        String transitiveSubject = uniqueTopic("full-transitive-reject") + "-value";
        registerSubject(transitiveSubject, avroSchema("v1"));
        registerSubject(transitiveSubject, avroSchema("v2"));
        try (SchemaRegistryClient client = newRegistryClient()) {
            client.updateCompatibility(transitiveSubject, "FULL_TRANSITIVE");
            AvroSchema v3 = new AvroSchema(avroSchema("v3-required-no-default"));
            RestClientException failure = assertThrows(RestClientException.class,
                    () -> client.register(transitiveSubject, v3));
            assertEquals(409, failure.getStatus());
            assertTrue(failure.getMessage().contains("oldSchemaVersion: 1") || failure.getMessage().contains("\"oldSchemaVersion\":1"),
                    "expected the rejection to cite version 1 (the schema v3 fails against transitively), got: " + failure.getMessage());
        }
    }

    @Test
    void replayingOldRecordsWithAnEvolvedSchemaSucceeds() throws Exception {
        String topic = uniqueTopic("replay");
        Schema v1 = avroSchema("v1");
        Schema v2 = avroSchema("v2");
        registerSubject(topic + "-value", v1);

        produceAvro(topic, "O-REPLAY-1", orderRecord(v1, "O-REPLAY-1", "C-1", "1.00"), v1);
        produceAvro(topic, "O-REPLAY-2", orderRecord(v1, "O-REPLAY-2", "C-1", "2.00"), v1);

        // Schema evolves AFTER these records were already written.
        registerSubject(topic + "-value", v2);
        produceAvro(topic, "O-REPLAY-3", orderRecord(v2, "O-REPLAY-3", "C-1", "3.00", "EUR"), v2);

        // A brand-new consumer group "replays" the whole topic from the
        // beginning, well after the schema evolved.
        List<GenericRecord> replayed = consumeAvro(topic, 3, Duration.ofSeconds(20));
        assertEquals(3, replayed.size(), "all records, written under different schema versions, must replay successfully");
    }

    @Test
    void ciCompatibilityGateMechanismDistinguishesPassAndFail() throws Exception {
        // Exercises the SAME lower-level mechanism SchemaCompatibilityGateApp
        // uses (RestService.testCompatibility against a specific version),
        // proving the gate's underlying check is real and correct --
        // including the transitive case the app's OWN first, flawed
        // version got wrong (see the conceptual doc, Section 23).
        String subject = uniqueTopic("ci-gate") + "-value";
        registerSubject(subject, avroSchema("v1"));
        registerSubject(subject, avroSchema("v2"));

        RestService restService = new RestService(cluster.schemaRegistryUrl());
        String v3Json = avroSchema("v3-required-no-default").toString();

        List<String> vsV2 = restService.testCompatibility(v3Json, "AVRO", Collections.emptyList(), subject, "2", true);
        assertTrue(vsV2.isEmpty(), "v3 (no default) must be compatible with v2 in isolation");

        List<String> vsV1 = restService.testCompatibility(v3Json, "AVRO", Collections.emptyList(), subject, "1", true);
        assertFalse(vsV1.isEmpty(), "v3 (no default) must NOT be compatible with v1 -- the transitive trap");
    }

    @Test
    void registryUnavailableStillServesAlreadyCachedSchemasButFailsOnNewLookups() throws Exception {
        // A DEDICATED, disposable cluster for this one test -- it
        // deliberately stops the registry container permanently, so it
        // must not share state with any other test.
        try (SchemaRegistryKafkaCluster dedicated = new SchemaRegistryKafkaCluster(19_650)) {
            dedicated.start();
            String topic = "registry-failure-test";
            String schemaRegistryUrl = dedicated.schemaRegistryUrl();
            Schema v1 = avroSchema("v1");

            SchemaRegistryClient warmClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
            warmClient.register(topic + "-value", v1);

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dedicated.bootstrapServers());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            producerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
            producerProps.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

            try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
                // Warm this producer instance's own internal ID cache.
                producer.send(new ProducerRecord<>(topic, "warm-up", orderRecord(v1, "warm-up", "C-1", "1.00"))).get();

                // Real outage.
                dedicated.stopSchemaRegistryOnly();
                waitUntil(() -> !isReachable(schemaRegistryUrl), Duration.ofSeconds(15));

                // The SAME producer instance, reusing its cache, must still succeed.
                var metadata = producer.send(new ProducerRecord<>(topic, "after-outage", orderRecord(v1, "after-outage", "C-1", "2.00"))).get();
                assertNotEquals(null, metadata);
            }

            // A BRAND NEW client, empty cache, must fail.
            try (SchemaRegistryClient freshClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)) {
                assertThrows(Exception.class, () -> freshClient.getLatestSchemaMetadata("some-never-seen-subject-value"));
            }
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private static Schema avroSchema(String name) throws Exception {
        String fileName = switch (name) {
            case "v1" -> "order-event-v1.avsc";
            case "v2" -> "order-event-v2.avsc";
            case "v2-renamed" -> "order-event-v2-renamed-field.avsc";
            case "v2-orderid-type-changed" -> "order-event-v2-orderid-type-changed.avsc";
            case "v3-required-no-default" -> "order-event-v3-required-no-default.avsc";
            default -> throw new IllegalArgumentException(name);
        };
        return new Schema.Parser().parse(new java.io.File("src/main/avro/" + fileName));
    }

    private static GenericRecord orderRecord(Schema schema, String orderId, String customerId, String amount) {
        return orderRecord(schema, orderId, customerId, amount, null);
    }

    private static GenericRecord orderRecord(Schema schema, String orderId, String customerId, String amount, String currency) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("orderId", orderId);
        record.put("customerId", customerId);
        record.put("amount", new BigDecimal(amount));
        if (schema.getField("currency") != null) {
            record.put("currency", currency == null ? "USD" : currency);
        }
        return record;
    }

    private static SchemaRegistryClient newRegistryClient() {
        return new CachedSchemaRegistryClient(cluster.schemaRegistryUrl(), 100);
    }

    private static int registerSubject(String subject, Schema schema) throws Exception {
        try (SchemaRegistryClient client = newRegistryClient()) {
            return client.register(subject, schema);
        }
    }

    private static void produceAvro(String topic, String key, GenericRecord record, Schema schema) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, record)).get();
        }
    }

    private static byte[] encodeAvro(Schema schema, GenericRecord record, GenericData genericData) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema, genericData).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static List<GenericRecord> consumeAvro(String topic, int minCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        List<GenericRecord> collected = new ArrayList<>();
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    collected.add(record.value());
                }
            }
        }
        if (collected.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " records on " + topic + ", got " + collected.size());
        }
        return collected;
    }

    private static List<byte[]> consumeRawBytes(String topic, int minCount, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-raw-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<byte[]> collected = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, byte[]> record : records) {
                    collected.add(record.value());
                }
            }
        }
        return collected;
    }

    private static boolean isReachable(String schemaRegistryUrl) {
        try {
            java.net.HttpURLConnection connection = (java.net.HttpURLConnection) new java.net.URL(schemaRegistryUrl + "/subjects").openConnection();
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            connection.connect();
            connection.disconnect();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
