package com.thecodinginterface.avro_people_producer.commands;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CreatePeopleCommand {
    private int count;
}