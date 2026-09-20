package com.kafkalab.schemaevolution.protobuf;

import com.kafkalab.schemaevolution.support.LabConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Section 19's experiment: a concise, real Protobuf producer against the
 * same running Schema Registry. {@code OrderEventProto.OrderEvent} is
 * generated at build time by the {@code protobuf-gradle-plugin} from
 * {@code src/main/proto/order_event.proto} -- unlike the Avro side of this
 * lab, Confluent's Protobuf serializer works with real generated
 * {@link com.google.protobuf.Message} classes, not a "GenericRecord"
 * equivalent, so code generation is unavoidable here (not a lab choice).
 *
 * <p>{@code auto.register.schemas} defaults to {@code true} for the
 * Protobuf serializer too; left at its default here (unlike the Avro
 * apps) specifically to show the OTHER common real-world path -- a
 * producer that registers implicitly on first use -- for contrast with
 * this lab's Avro apps, which always register explicitly.
 */
public final class ProtobufOrderEventProducerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String schemaRegistryUrl = LabConfig.schemaRegistryUrl();
        String topic = LabConfig.get("topic", "protobuf-orders");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        OrderEventProto.OrderEvent event = OrderEventProto.OrderEvent.newBuilder()
                .setOrderId("O-PROTO-1")
                .setCustomerId("C-501")
                .setAmount("99.99")
                .setCurrency("USD")
                .build();

        try (KafkaProducer<String, OrderEventProto.OrderEvent> producer = new KafkaProducer<>(props)) {
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, event.getOrderId(), event)).get();
            System.out.printf("Produced Protobuf OrderEvent | topic=%s | partition=%d | offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }

        System.out.println("Subject registered (TopicNameStrategy default): " + topic + "-value");
        System.out.println("Field numbers on the wire: order_id=1, customer_id=2, amount=3, currency=4 -- see order_event.proto for why reusing any of these later would be dangerous.");
    }
}
