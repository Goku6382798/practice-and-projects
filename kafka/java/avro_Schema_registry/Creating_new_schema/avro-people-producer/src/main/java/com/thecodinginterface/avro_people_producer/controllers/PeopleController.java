package com.thecodinginterface.avro_people_producer.controllers;

import com.thecodinginterface.avro_people_producer.commands.CreatePeopleCommand;
import com.thecodinginterface.avro_people_producer.models.PersonDto;
import com.thecodinginterface.avrodomainevents.Person;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
public class PeopleController {

    private static final Logger logger = LoggerFactory.getLogger(PeopleController.class);

    @Value("${topics.people-avro.name}")
    private String personAvroTopic;

    private final KafkaTemplate<String, Person> kafkaTemplate;
    private final Random random = new Random();

    public PeopleController(KafkaTemplate<String, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/people")
    @ResponseStatus(HttpStatus.CREATED)
    public List<PersonDto> create(@RequestBody CreatePeopleCommand cmd) {
        logger.info("Creating people cmd: {}", cmd);
        List<PersonDto> people = new ArrayList<>();

        for (int i = 0; i < cmd.getCount(); i++) {
            String name = generateRandomName();
            String title = generateRandomTitle();

            Person person = Person.newBuilder()
                    .setName(name)
                    .setTitle(title)
                    .build();

            people.add(new PersonDto(name, title));

            CompletableFuture<SendResult<String, Person>> future = kafkaTemplate.send(
                    personAvroTopic,
                    name.toLowerCase().replaceAll("\\s+", "-"),
                    person
            );

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("Failed to produce " + person, ex);
                } else {
                    RecordMetadata metadata = result.getRecordMetadata();
                    logger.info("Produced {} to topic-partition {}-{} offset {}", person,
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        }

        kafkaTemplate.flush();
        return people;
    }

    private String generateRandomName() {
        return "User-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String generateRandomTitle() {
        String[] titles = {"Engineer", "Manager", "Data Analyst", "Intern", "Consultant", "Product Owner"};
        return titles[random.nextInt(titles.length)];
    }
}
