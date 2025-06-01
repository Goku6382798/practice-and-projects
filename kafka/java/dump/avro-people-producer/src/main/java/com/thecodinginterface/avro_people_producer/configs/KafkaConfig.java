package com.thecodinginterface.avro_people_producer.configs;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


@Configuration
public class KafkaConfig {    
    @Value("${topics.people-avro.name}")
    String peopleAvroTopic;

    @Value("${topics.people-avro.replicas}")
    private int peopleAvroTopicReplicas;

    @Value("${topics.people-avro.partitions}")
    private int peopleAvroTopicPartitions;

    @Bean
    public NewTopic makePeopleAvroTopic() {
        return TopicBuilder.name(peopleAvroTopic)
                .replicas(peopleAvroTopicReplicas)
                .partitions(peopleAvroTopicPartitions)
                .build();
    }
}