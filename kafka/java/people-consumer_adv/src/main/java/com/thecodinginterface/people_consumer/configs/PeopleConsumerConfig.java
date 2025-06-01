package com.thecodinginterface.people_consumer.configs;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.thecodinginterface.people_consumer.entities.Person;


@EnableKafka
@Configuration

public class PeopleConsumerConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServer;

    @Value("${topics.people-adv.name}")
    String peopleTopic;

    public Map<String, Object> consumerConfigs() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
                ConsumerConfig.GROUP_ID_CONFIG, "people.adv.java.grp-0"
        );
    }

    @Bean
    public ConsumerFactory<String, Person> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
            consumerConfigs(),
            new StringDeserializer(),
            new JsonDeserializer<>(Person.class, false)

    );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Person> personListenerFactory() {
        var factor = new ConcurrentKafkaListenerContainerFactory<String, Person>();
        factor.setConsumerFactory(consumerFactory());
        factor.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factor;
    }
}