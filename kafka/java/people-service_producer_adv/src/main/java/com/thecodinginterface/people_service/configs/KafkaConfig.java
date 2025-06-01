package com.thecodinginterface.people_service.configs;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    @Value("${topics.people-adv.name}")
    String topicName;

    @Value("${topics.people-adv.replicas}")
    int topicReplicas;

    @Value("${topics.people-adv.partitions}")
    int topicPartitions;

    @Bean
    public NewTopic peopleAdvTopic() {
        return TopicBuilder.name(topicName)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

    @Bean
    public NewTopic peopleBasicShortTopic() {
        return TopicBuilder.name(topicName + "-short")
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, "360000")
                .build();
    }

    @EventListener(ContextRefreshedEvent.class)
    public void changePeopleBasicTopicRetention() {
        // create a connection with configs to bootstrap server
        Map<String, Object> connectionConfigs = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (var admin = AdminClient.create(connectionConfigs)) {
        
            // create a config resource to fetch topics configs
            var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);   
        
            // filter down to specific topic (retention.ms)
            ConfigEntry topicConfigEntry = admin.describeConfigs(Collections.singleton(configResource))
                .all().get().entrySet().stream()
                .findFirst().get().getValue().entries().stream()
                .filter(ce -> ce.name().equals(TopicConfig.RETENTION_MS_CONFIG))
                .findFirst().get();

                // check if the config is 360,000 if not update to 1 hour
            if (Long.parseLong(topicConfigEntry.value()) != 360000L) {
                // create a config entry and a alter config op to specify whaat config to change (retrntion.ms)
                var alterConfigEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "360000");
                var alterOp = new AlterConfigOp(alterConfigEntry, AlterConfigOp.OpType.SET);
                //use admin to exec the alter operation
                Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Map.of(configResource, Collections.singletonList(alterOp));
                admin.incrementalAlterConfigs(alterConfigs).all().get();
                logger.info("Updated topic retention for " + topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error while modifying Kafka topic config", e);
            Thread.currentThread().interrupt();
        }
    }
}    