package com.kafkalab.eventconsole.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import com.kafkalab.eventconsole.producer.config.StartupConfigValidator;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ProducerApplication.class);
        app.addListeners((ApplicationEnvironmentPreparedEvent event) ->
                StartupConfigValidator.validate(event.getEnvironment()));
        app.run(args);
    }
}
