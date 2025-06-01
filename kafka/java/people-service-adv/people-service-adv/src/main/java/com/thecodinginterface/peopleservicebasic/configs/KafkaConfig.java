package com.thecodinginterface.peopleservicebasic.configs;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Configuration
public class KafkaConfig {

    static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

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

    @Bean
    public NewTopic peopleBasicShortTopic() {
        return TopicBuilder.name(topicName + "-short")
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, "360000")
                .build();
    }

    @PostConstruct
    public void changePeopleBasicTopicRetention() throws Exception {
        // Create a connection config for admin
        Map<String, Object> connectionConfigs = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // create admin client within try-with-resources block
        try (var admin = AdminClient.create(connectionConfigs)) {

            // crate a config resource to instruct the admin client to fetch the topics configs
            var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            // fetch and filter down to just the retention config
            ConfigEntry topicConfigEntry = admin.describeConfigs(Collections.singleton(configResource))
                    .all().get().entrySet().stream()
                    .findFirst().get().getValue().entries().stream()
                    .filter(ce -> ce.name().equals(TopicConfig.RETENTION_MS_CONFIG))
                    .findFirst().get();

            // see if its already what we want it to be (1 hour) and if not use admin to set to 1 hour
            if (Long.parseLong(topicConfigEntry.value()) != 360000L) {

                // create a config entry and an alter config op to specify what config to change
                var alterConfigEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "360000");
                var alterOp = new AlterConfigOp(alterConfigEntry, AlterConfigOp.OpType.SET);

                // use admin to alter the config passing it a map of the alter config op
                Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Map.of(configResource, Collections.singletonList(alterOp));
                admin.incrementalAlterConfigs(alterConfigs).all().get();
                logger.info("Updated topic retention for " + topicName);
            }
        }
    }
}
