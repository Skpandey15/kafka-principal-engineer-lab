package com.kafkalab.schemaevolution.avro;

import com.kafkalab.schemaevolution.support.LabConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;

/**
 * Section 18's experiment, standalone and offline (no Kafka, no registry
 * -- this is Avro's OWN schema resolution mechanism, entirely independent
 * of how the bytes got there):
 *
 * <pre>
 * Producer writes using: Writer Schema v1
 * Consumer reads using:  Reader Schema v2
 * </pre>
 *
 * <p>This is deliberately built with {@link GenericDatumReader}'s TWO-
 * schema constructor ({@code writerSchema, readerSchema}) rather than
 * through {@code KafkaAvroDeserializer}, because
 * {@code AvroOrderEventConsumerApp} (Sections 5-8) does NOT pin a reader
 * schema at all -- it always deserializes each record against its OWN
 * writer schema, giving back records shaped however THAT record was
 * written, with no defaults applied for anything newer. That is a real,
 * important limitation worth being explicit about: plain
 * {@code GenericRecord} consumption doesn't get you real schema
 * resolution "for free" -- an application that wants EVERY record
 * normalized to one shape (with defaults filled in for older data) has to
 * do what THIS class does: explicitly decode with a chosen reader schema.
 */
public final class ReaderWriterResolutionDemoApp {

    public static void main(String[] args) throws Exception {
        Schema v1 = new Schema.Parser().parse(new java.io.File("src/main/avro/order-event-v1.avsc"));
        Schema v2 = new Schema.Parser().parse(new java.io.File("src/main/avro/order-event-v2.avsc"));
        Schema v3NoDefault = new Schema.Parser().parse(new java.io.File("src/main/avro/order-event-v3-required-no-default.avsc"));

        byte[] v1Bytes = encode(v1, buildV1Record(v1));
        System.out.println("Encoded " + v1Bytes.length + " bytes using WRITER schema v1 (no currency field at all).");

        System.out.println();
        System.out.println("--- Reading with READER schema = v1 (writer == reader, the trivial case) ---");
        GenericRecord asV1 = decode(v1, v1, v1Bytes);
        System.out.println("Result: " + asV1);

        System.out.println();
        System.out.println("--- Reading with READER schema = v2 (writer=v1, reader=v2 -- REAL Avro schema resolution) ---");
        GenericRecord asV2 = decode(v1, v2, v1Bytes);
        System.out.println("Result: " + asV2);
        System.out.println("currency = " + asV2.get("currency") + " -- filled in from v2's DEFAULT, even though the bytes never contained it.");

        System.out.println();
        System.out.println("--- Reading with READER schema = v3-required-no-default (writer=v1, reader has currency but NO default) ---");
        try {
            GenericRecord asV3 = decode(v1, v3NoDefault, v1Bytes);
            System.out.println("Result (unexpected -- should not resolve): " + asV3);
        } catch (Exception e) {
            System.out.println("FAILED to resolve, as expected: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("This is the SAME incompatibility the real Schema Registry rejected in Section 17 (BACKWARD_TRANSITIVE vs v1) -- here reproduced as an actual DATA-level decode failure, not just a schema-comparison rejection.");
        }
    }

    private static GenericRecord buildV1Record(Schema v1) {
        org.apache.avro.generic.GenericData.Record record = new org.apache.avro.generic.GenericData.Record(v1);
        record.put("orderId", "O-RESOLUTION-DEMO");
        record.put("customerId", "C-501");
        record.put("amount", new BigDecimal("42.50"));
        return record;
    }

    private static byte[] encode(Schema schema, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        // Logical-type-aware writing needs the same conversions the Confluent
        // serializer applies internally -- reused here directly via Avro's
        // own GenericData instance with the decimal conversion registered,
        // so this standalone demo's encoding matches what a real producer
        // (with avro.use.logical.type.converters=true) actually puts on the wire.
        org.apache.avro.generic.GenericData genericData = new org.apache.avro.generic.GenericData();
        genericData.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema, genericData);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static GenericRecord decode(Schema writerSchema, Schema readerSchema, byte[] bytes) throws Exception {
        org.apache.avro.generic.GenericData genericData = new org.apache.avro.generic.GenericData();
        genericData.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema, genericData);
        Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null);
        return reader.read(null, decoder);
    }
}
