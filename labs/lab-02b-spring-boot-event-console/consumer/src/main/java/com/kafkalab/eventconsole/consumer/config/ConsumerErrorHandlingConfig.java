package com.kafkalab.eventconsole.consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class ConsumerErrorHandlingConfig {

    /**
     * What happens when the listener itself throws -- which, by design, means ONE thing: MongoDB
     * could not be written.
     *
     * <p>A bad event never reaches this handler. Processing failures are caught per event and
     * stored as FAILED (see {@code EventConsumer}), and the retry worker owns them from there. So
     * anything that does escape the listener is an infrastructure problem that says nothing about
     * any particular event, and dead-lettering or skipping would be wrong: the consumer simply
     * waits, retrying with a capped backoff and never giving up, then continues exactly where it
     * stopped. Lag grows while it waits, which is the signal to alert on.
     *
     * <p>Spring Kafka's default is the opposite -- a few immediate retries, then the record is
     * silently skipped.
     */
    @Bean
    CommonErrorHandler consumerErrorHandler(AppProperties props) {
        return new DefaultErrorHandler(retryForever(props));
    }

    static BackOff retryForever(AppProperties props) {
        ExponentialBackOff backOff = new ExponentialBackOff(props.consumerBackoffInitialMs(), 2.0);
        backOff.setMaxInterval(props.consumerBackoffMaxMs());
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }
}
