package com.kafkalab.eventconsole.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    // KafkaAdmin creates the topic on startup if it does not exist, and never alters an existing
    // one, so a changed partition count here is NOT applied to a topic that already exists --
    // partition count is capacity-planned state. The consumer service declares the same topic
    // with the same settings: whichever starts first creates it, so neither depends on the
    // other's start order. In a real platform, provision topics as code instead.
    @Bean
    NewTopic eventsTopic(AppProperties props) {
        return TopicBuilder.name(props.topic())
                .partitions(props.topicPartitions())
                .replicas(props.topicReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(props.topicMinInsyncReplicas()))
                .build();
    }
}
