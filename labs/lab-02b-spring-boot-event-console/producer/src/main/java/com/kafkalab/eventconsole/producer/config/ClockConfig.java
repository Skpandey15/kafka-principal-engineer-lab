package com.kafkalab.eventconsole.producer.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfig {

    /** Injected rather than reading the system clock directly, so time-dependent logic is testable without sleeping. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
