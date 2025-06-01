package com.thecodinginterface.people_consumer.consumers;

import com.thecodinginterface.people_consumer.entities.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class PeopleConsumer {

   static final Logger logger = LoggerFactory.getLogger(PeopleConsumer.class);
   
   @KafkaListener(topics = "${topics.people-adv.name}", containerFactory="personListenerFactory")
   public void handlePersonEvent(Person person) {
   logger.info("Processing " + person);
   }
}