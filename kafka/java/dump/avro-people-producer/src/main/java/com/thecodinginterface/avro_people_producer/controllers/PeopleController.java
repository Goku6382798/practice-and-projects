package com.thecodinginterface.avro_people_producer.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.thecodinginterface.avro_people_producer.commands.CreatePeopleCommand;
import com.thecodinginterface.avro_people_producer.models.PersonDto;
import com.thecodinginterface.avrodomainevents.Person;

@RestController
@RequestMapping("/api")
public class PeopleController {
    private static final Logger logger = LoggerFactory.getLogger(PeopleController.class);

    @Value("${topics.people-avro.name}")
    private String personAvroTopic;

    private final KafkaTemplate<String, Person> kafkaTemplate;

    private final String[] randomNames = {
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Heidi"
    };

    private final String[] randomTitles = {
        "Engineer", "Manager", "Data Scientist", "Architect", "Developer", "Consultant"
    };

    private final Random random = new Random();

    public PeopleController(KafkaTemplate<String, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/people")
    @ResponseStatus(HttpStatus.CREATED)
    public List<PersonDto> create(@RequestBody CreatePeopleCommand cmd) {
        logger.info("Creating people with command: {}", cmd);
        List<PersonDto> people = new ArrayList<>();

        for (int i = 0; i < cmd.getCount(); i++) {
            String name = randomNames[random.nextInt(randomNames.length)];
            String title = randomTitles[random.nextInt(randomTitles.length)];

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
                if (ex == null) {
                    logger.info("Produced {}", person);
                } else {
                    logger.error("Failed to produce " + person, ex);
                }
            });
        }

        kafkaTemplate.flush();
        return people;
    }
}
