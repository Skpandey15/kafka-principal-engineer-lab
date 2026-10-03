package com.kafkalab.eventconsole.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    // KafkaAdmin creates these on startup if they do not exist, and never alters an existing
    // topic. The producer service declares the main topic with the same settings: whichever
    // starts first creates it, so neither depends on the other's start order. The dead-letter
    // topic belongs to the consumer alone. In a real platform, provision topics as code.
    @Bean
    NewTopic eventsTopic(AppProperties props) {
        return TopicBuilder.name(props.topic())
                .partitions(props.topicPartitions())
                .replicas(props.topicReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(props.topicMinInsyncReplicas()))
                .build();
    }

    // The dead-letter recoverer publishes to the SAME partition number as the
    // failed record, so the DLT needs at least as many partitions as the source.
    @Bean
    NewTopic deadLetterTopic(AppProperties props) {
        return TopicBuilder.name(props.deadLetterTopic())
                .partitions(props.topicPartitions())
                .replicas(props.topicReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(props.topicMinInsyncReplicas()))
                .build();
    }
}
