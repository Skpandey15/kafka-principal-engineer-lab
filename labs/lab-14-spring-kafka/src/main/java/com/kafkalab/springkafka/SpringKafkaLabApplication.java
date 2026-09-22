package com.kafkalab.springkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * The FIRST Spring Boot application in this repository -- every prior
 * lab is plain Java with a hand-written {@code main} method and no
 * dependency-injection framework at all. See the README, "Why Spring
 * Boot here and nowhere else," for why that changes starting with this
 * WP specifically.
 */
@SpringBootApplication
@EnableKafka
public class SpringKafkaLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaLabApplication.class, args);
    }
}
