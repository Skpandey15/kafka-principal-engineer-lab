package com.kafkalab.eventconsole;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import com.kafkalab.eventconsole.config.StartupConfigValidator;

@SpringBootApplication
@ConfigurationPropertiesScan
public class EventConsoleApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(EventConsoleApplication.class);
        app.addListeners((ApplicationEnvironmentPreparedEvent event) ->
                StartupConfigValidator.validate(event.getEnvironment()));
        app.run(args);
    }
}
