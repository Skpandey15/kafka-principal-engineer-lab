package com.kafkalab.springkafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Two container factories beyond Spring Boot's own auto-configured
 * default (which already gives BATCH acknowledgment mode -- Spring
 * Kafka's actual default, the native equivalent of committing once per
 * poll batch, exactly the "batch-commit boundary" concept WP-06
 * already covers natively), plus this app's own plain, non-transactional
 * {@code KafkaTemplate}.
 *
 * <p>A real finding building this lab: defining ANY custom
 * {@code @Bean} of type {@code KafkaTemplate} -- regardless of its
 * generic type parameters -- suppresses Spring Boot's own
 * autoconfigured default entirely. {@code spring-boot-kafka}'s
 * {@code KafkaAutoConfiguration#kafkaTemplate(...)} method is annotated
 * {@code @ConditionalOnMissingBean}, and that condition matches by RAW
 * type ({@code KafkaTemplate.class}), not generics -- a custom
 * {@code KafkaTemplate<String, String>} bean elsewhere in this app
 * (originally {@link com.kafkalab.springkafka.txn.TransactionalProducerConfig})
 * was enough to make Boot's own {@code KafkaTemplate<Object, Object>}
 * bean never get created at all, breaking every OTHER bean in this app
 * that expected it (confirmed the hard way: a real
 * {@code NoSuchBeanDefinitionException}). Once you define your own
 * {@code KafkaTemplate} bean, you own ALL of them -- there's no partial
 * opt-in. This class's own {@code kafkaTemplate} bean below is the
 * fix: a plain, explicitly-built, non-transactional template used by
 * this app's ordinary listeners and the DLT recoverer, kept entirely
 * separate from {@code TransactionalProducerConfig}'s dedicated
 * transactional one.
 *
 * <p>Both container factories build their OWN
 * {@link DefaultKafkaConsumerFactory} directly from plain
 * {@link ConsumerConfig} properties, deliberately NOT reusing Spring
 * Boot's autoconfigured {@code ConsumerFactory<Object, Object>} bean --
 * the SAME generics-vs-raw-type subtlety, just on the consumer side:
 * an {@code @Autowired ConsumerFactory<String, String>} injection point
 * would not be satisfied by a bean declared as
 * {@code ConsumerFactory<Object, Object>}, so this class builds the
 * properties explicitly instead of fighting that.
 */
@Configuration
public class ListenerContainerFactoryConfig {

    // Also aliased as "defaultRetryTopicKafkaTemplate" -- a real,
    // separate requirement of @RetryableTopic's infrastructure
    // (RetryableTopicListener): with more than one KafkaTemplate bean
    // in the context (this one, plus TransactionalProducerConfig's
    // dedicated transactional one), it cannot pick a default on its
    // own and fails fast at startup with "A single KafkaTemplate bean
    // could not be found in the context; a single instance must exist,
    // or one specifically named defaultRetryTopicKafkaTemplate" --
    // confirmed by hitting that exact real error.
    @Bean(name = {"kafkaTemplate", "defaultRetryTopicKafkaTemplate"})
    public KafkaTemplate<Object, Object> kafkaTemplate(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualAckContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps(bootstrapServers, "lab14-manual-ack-group")));
        // The native mechanism this wraps: WP-06's manual commitSync()
        // per record, instead of relying on the container to commit
        // for you once a batch finishes.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> errorHandlingContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers, KafkaTemplate<Object, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps(bootstrapServers, "lab14-error-handling-group")));
        // The native mechanism this wraps: WP-13's own hand-rolled
        // retry-with-backoff-then-DLQ RetryingRecordProcessor -- this
        // is the SAME pattern, framework-provided.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2L));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private static Map<String, Object> consumerProps(String bootstrapServers, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
