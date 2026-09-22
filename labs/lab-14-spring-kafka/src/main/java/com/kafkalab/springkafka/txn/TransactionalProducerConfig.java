package com.kafkalab.springkafka.txn;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * A SEPARATE, dedicated transactional producer, built explicitly rather
 * than via {@code spring.kafka.producer.transaction-id-prefix} (which
 * would make the app's DEFAULT KafkaTemplate transactional too -- see
 * {@code application.properties}'s comment for the real error this
 * caused). The native mechanism this wraps: WP-09's own hand-set
 * {@code transactional.id} on a dedicated {@code KafkaProducer}, not
 * shared with any non-transactional producer traffic.
 */
@Configuration
public class TransactionalProducerConfig {

    @Bean
    public DefaultKafkaProducerFactory<String, String> transactionalProducerFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix("lab14-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> transactionalKafkaTemplate(DefaultKafkaProducerFactory<String, String> transactionalProducerFactory) {
        return new KafkaTemplate<>(transactionalProducerFactory);
    }

    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager(DefaultKafkaProducerFactory<String, String> transactionalProducerFactory) {
        return new KafkaTransactionManager<>(transactionalProducerFactory);
    }
}
