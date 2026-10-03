package com.kafkalab.eventconsole;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class EventConsoleApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventConsoleApplication.class, args);
    }
}
