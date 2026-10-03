package com.kafkalab.eventconsole.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class ConsumerErrorHandlingConfig {

    /**
     * Two different kinds of failure need two different answers:
     *
     * <ul>
     *   <li><b>A record that can never be stored</b> (poison): retry a few times with
     *       exponential backoff, then publish it to the dead-letter topic and move on.
     *       Without this, Spring Kafka's default retries back-to-back and then SKIPS the
     *       record -- silent loss.</li>
     *   <li><b>An outage</b> (MongoDB unreachable or timing out): the record is fine, the
     *       database is not. Dead-lettering every record would empty the topic into the DLT
     *       the moment MongoDB has a bad minute. So retry forever with a capped backoff; the
     *       consumer simply stalls (and lag grows, which is the signal to alert on) until the
     *       database is back, then continues exactly where it left off.</li>
     * </ul>
     *
     * Spring Boot applies this bean to the listener container automatically.
     */
    @Bean
    CommonErrorHandler consumerErrorHandler(KafkaOperations<String, String> template, AppProperties props) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(props.deadLetterTopic(), record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, boundedBackOff(props));
        handler.setBackOffFunction((record, ex) -> isOutage(ex) ? unboundedBackOff(props) : boundedBackOff(props));
        return handler;
    }

    private static BackOff boundedBackOff(AppProperties props) {
        ExponentialBackOff backOff = new ExponentialBackOff(props.consumerBackoffInitialMs(), 2.0);
        backOff.setMaxInterval(props.consumerBackoffMaxMs());
        backOff.setMaxAttempts(props.consumerMaxRetries());
        return backOff;
    }

    private static BackOff unboundedBackOff(AppProperties props) {
        ExponentialBackOff backOff = new ExponentialBackOff(props.consumerBackoffInitialMs(), 2.0);
        backOff.setMaxInterval(props.consumerBackoffMaxMs());
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }

    /** True if anything in the cause chain says "the database is unavailable", not "this record is bad". */
    static boolean isOutage(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof DataAccessResourceFailureException || t instanceof TransientDataAccessException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
