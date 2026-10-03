package com.kafkalab.eventconsole.consumer.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfig {

    /** Injected rather than calling Instant.now(), so time-dependent logic can be tested without sleeping. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
