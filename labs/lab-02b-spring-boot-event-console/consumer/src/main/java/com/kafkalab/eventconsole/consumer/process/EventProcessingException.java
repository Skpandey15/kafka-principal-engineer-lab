package com.kafkalab.eventconsole.consumer.process;

/** An event that could not be processed. The message is stored with the event and shown in the UI. */
public class EventProcessingException extends RuntimeException {

    public EventProcessingException(String message) {
        super(message);
    }
}
