package com.kafkalab.eventconsole.consumer.contract;

import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.process.ConsumedEvent;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;

/**
 * The processing step: an event is accepted only if it follows the contract it declares. A
 * record with no {@code x-schema-id} header declares nothing, so there is nothing to hold it to --
 * that is a failure of the event (it goes through the retry path and ends DEAD), not something to
 * guess around.
 */
@Component
public class ContractEventProcessor implements EventProcessor {

    private final EventContract contract;

    public ContractEventProcessor(EventContract contract) {
        this.contract = contract;
    }

    @Override
    public void process(ConsumedEvent event) {
        if (event.value() == null || event.value().isBlank()) {
            throw new EventProcessingException("value is empty");
        }
        if (event.schemaId() == null) {
            throw new EventProcessingException("event has no x-schema-id header, so it declares no contract");
        }
        int schemaId;
        try {
            schemaId = Integer.parseInt(event.schemaId().trim());
        } catch (NumberFormatException e) {
            throw new EventProcessingException("x-schema-id header is not a schema id");
        }
        contract.validate(schemaId, event.value());
    }
}
