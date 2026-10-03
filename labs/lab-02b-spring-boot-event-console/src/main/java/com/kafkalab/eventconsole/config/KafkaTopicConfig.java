package com.kafkalab.eventconsole.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    // KafkaAdmin creates these on startup if they do not exist. It never alters
    // an existing topic, so a changed partition count here is not applied to a
    // topic that already exists -- partition count is capacity-planned state.
    @Bean
    NewTopic eventsTopic(AppProperties props) {
        return TopicBuilder.name(props.topic())
                .partitions(props.topicPartitions())
                .replicas(props.topicReplicas())
                .build();
    }

    // The dead-letter recoverer publishes to the SAME partition number as the
    // failed record, so the DLT needs at least as many partitions as the source.
    @Bean
    NewTopic deadLetterTopic(AppProperties props) {
        return TopicBuilder.name(props.deadLetterTopic())
                .partitions(props.topicPartitions())
                .replicas(props.topicReplicas())
                .build();
    }
}
