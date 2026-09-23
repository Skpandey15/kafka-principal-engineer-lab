package com.kafkalab.security.acl;

import com.kafkalab.security.support.LabConfig;
import com.kafkalab.security.support.SecureClientProps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * Produces one record over the real SASL_SSL/SCRAM-SHA-512 listener,
 * as a given user -- run this as {@code admin} (a super.user, always
 * allowed) or {@code reader} (denied until a real ACL grants it) to
 * see real authorization in effect.
 */
public final class SecurityDemoApp {

    public static void main(String[] args) throws Exception {
        String username = LabConfig.get("username", "admin");
        String password = LabConfig.get("password", "admin-secret");
        String topic = LabConfig.get("topic", "security-demo");

        Map<String, Object> props = SecureClientProps.forUser(username, password);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "k", "hello from " + username)).get();
            System.out.println("[security-demo] " + username + " produced successfully to " + topic);
        }
    }
}
