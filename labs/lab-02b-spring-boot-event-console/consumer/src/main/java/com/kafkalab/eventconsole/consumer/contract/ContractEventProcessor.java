package com.kafkalab.eventconsole.consumer.contract;

import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.process.ConsumedEvent;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.EventProcessor;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The processing step: an event is accepted only if it follows the contract it declares.
 *
 * <p>A record with no {@code x-schema-id} header declares nothing, so there is nothing to hold it to.
 * Two very different things look like that, and the topic's cut-over tells them apart:
 * <ul>
 *   <li><b>Legacy history</b> -- the record sits below the cut-over offset for its partition, so it was
 *       written before the contract existed. It is accepted as it is (and counted), so the topic can
 *       be replayed without turning all of its history into failures.</li>
 *   <li><b>Everything else</b> -- a producer that skipped the service. That is a failure of the event
 *       (it goes through the retry path and ends DEAD), not something to guess around.</li>
 * </ul>
 * A record that DOES carry a header is always held to it, wherever it sits.
 */
@Component
public class ContractEventProcessor implements EventProcessor {

    private final EventContract contract;
    private final LegacyHistory legacy;
    private final MeterRegistry metrics;

    public ContractEventProcessor(EventContract contract, LegacyHistory legacy, MeterRegistry metrics) {
        this.contract = contract;
        this.legacy = legacy;
        this.metrics = metrics;
    }

    @Override
    public void process(ConsumedEvent event) {
        if (event.schemaId() == null && legacy.covers(event.partition(), event.offset())) {
            // Written before the contract: nothing to validate it against, and nothing to say about its content.
            metrics.counter("eventconsole.consume.legacy").increment();
            return;
        }
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
