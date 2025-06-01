package com.thecodinginterface.peopleservicebasic.controllers;

import net.datafaker.Faker;
import com.thecodinginterface.peopleservicebasic.commands.CreatePeopleCommand;
import com.thecodinginterface.peopleservicebasic.entities.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class PeopleController {

    static final Logger logger = LoggerFactory.getLogger(PeopleController.class);

    @Value("${topics.people-basic.name}")
    private String peopleTopic;

    private KafkaTemplate<String, Person> kafkaTemplate;

    public PeopleController(KafkaTemplate<String, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/people")
    public String helloPeople() {
        return "Hello People";
    }

    @PostMapping("/people")
    @ResponseStatus(HttpStatus.CREATED)
    public List<Person> create(@RequestBody CreatePeopleCommand cmd) {
        logger.info("create command is " + cmd);

        var faker = new Faker();
        List<Person> people = new ArrayList<>();
        for (int i = 0; i < cmd.getCount(); i++) {
            var person = new Person(
                    UUID.randomUUID().toString(),
                    faker.name().fullName(),
                    faker.job().title()
            );
            people.add(person);
            ListenableFuture<SendResult<String, Person>> future = kafkaTemplate.send(
                    peopleTopic,
                    person.getTitle().toLowerCase().replaceAll("\\s+", "-"),
                    person
            );
        }
        kafkaTemplate.flush();
        return people;
    }

}
