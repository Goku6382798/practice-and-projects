package com.thecodinginterface.peopleservicebasic.configs;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    @Value("${topics.people-basic.name}")
    String topicName;

    @Value("${topics.people-basic.replicas}")
    int topicReplicas;

    @Value("${topics.people-basic.partitions}")
    int topicPartitions;

    @Bean
    public NewTopic peopleBasicTopic() {
        return TopicBuilder.name(topicName)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }
}
