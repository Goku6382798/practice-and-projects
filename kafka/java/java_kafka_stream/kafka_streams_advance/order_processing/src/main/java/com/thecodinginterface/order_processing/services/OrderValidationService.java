package com.thecodinginterface.order_processing.services;

import order.domain.events.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class OrderValidationService {

    static final Logger logger = LoggerFactory.getLogger(OrderValidationService.class);

    @Value("${spring.kafka.properties.schema.registry.url}")
    String schemaRegUrl;

    @Value("${topics.order-created.name}")
    String orderCreationTopic;

    @Value("${topics.order-validated.name}")
    String orderValidationTopic;

    static final double VALID_PROPORTION = 0.95;

    public Map<String, Object> serdeConfig() {
        return Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegUrl);
    }

    @Autowired
    public void process(StreamsBuilder streamBuilder) {
        var orderSerde = new SpecificAvroSerde<Order>();
        orderSerde.configure(serdeConfig(), false);

        KStream<Integer, Order> orderStream = streamBuilder.stream(orderCreationTopic,
                Consumed.with(Serdes.Integer(), orderSerde));

        orderStream
            .mapValues(order -> {
                double randNum = ThreadLocalRandom.current().nextDouble();
                return Order.newBuilder(order)
                            .setValid(randNum <= VALID_PROPORTION)
                            .build();
            })
            .peek((k, v) -> logger.info("✅ Validated Order: {}", v))
            .to(orderValidationTopic, Produced.with(Serdes.Integer(), orderSerde));
    }
}
