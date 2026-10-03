package com.kafkalab.eventconsole.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import com.kafkalab.eventconsole.consumer.config.StartupConfigValidator;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ConsumerApplication.class);
        app.addListeners((ApplicationEnvironmentPreparedEvent event) ->
                StartupConfigValidator.validate(event.getEnvironment()));
        app.run(args);
    }
}
