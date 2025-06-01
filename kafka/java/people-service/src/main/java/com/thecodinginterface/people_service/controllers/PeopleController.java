package com.thecodinginterface.people_service.commands;

import com.thecodinginterface.people_service.entities.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class PeopleController {

    private static final Logger logger = LoggerFactory.getLogger(PeopleController.class);

    @Value("${topics.people-basic.name}")
    private String peopleTopic;

    private final KafkaTemplate<String, Person> kafkaTemplate;

    public PeopleController(KafkaTemplate<String, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/people")
    @ResponseStatus(HttpStatus.CREATED)
    public List<Person> create(@RequestBody CreatePeopleCommand cmd) {
        logger.info("create command is {}", cmd);

        List<Person> people = new ArrayList<>();

        for (int i = 0; i < cmd.getCount(); i++) {
            Person person = new Person(
                UUID.randomUUID().toString(),
                "John Doe " + i,
                "Engineer"
            );

            people.add(person);

            kafkaTemplate.send(
                peopleTopic,
                person.getTitle().toLowerCase().replaceAll("\\s+", "-"),
                person
            );
        }

        kafkaTemplate.flush();
        return people;
    }
}
