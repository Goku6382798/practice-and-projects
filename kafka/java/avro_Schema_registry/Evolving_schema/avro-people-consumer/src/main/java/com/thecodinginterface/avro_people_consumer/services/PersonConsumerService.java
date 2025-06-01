package com.thecodinginterface.avro_people_consumer.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.thecodinginterface.avrodomainevents.Person;

@Service
public class PersonConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(PersonConsumerService.class);

    @KafkaListener(
        topics = "${topics.people-avro.name}",
        containerFactory = "personListenerFactory"
    )
    public void listenForEvents(Person person) {
        logger.info("Consuming: {}", person);
    }
}
