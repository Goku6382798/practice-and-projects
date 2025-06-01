package com.thecodinginterface.peopleservicebasic.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class PeopleController {

    @GetMapping("/people")
    public String helloPeople() {
        return "Hello People";
    }
}

