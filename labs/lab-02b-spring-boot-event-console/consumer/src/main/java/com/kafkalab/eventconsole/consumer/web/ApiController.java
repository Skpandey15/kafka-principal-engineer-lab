package com.kafkalab.eventconsole.consumer.web;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;

/** The consumer's whole public surface: read what was consumed. It accepts no event payloads. */
@RestController
@RequestMapping("/api/events")
public class ApiController {

    private final EventQueryRepository events;
    private final AppProperties props;
    private final String consumerGroup;

    public ApiController(EventQueryRepository events, AppProperties props,
            @Value("${spring.kafka.consumer.group-id}") String consumerGroup) {
        this.events = events;
        this.props = props;
        this.consumerGroup = consumerGroup;
    }

    @GetMapping
    public EventQueryRepository.Page events(
            @RequestParam(required = false) String key,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {
        return events.search(key, partition, q, Math.max(page, 0), Math.min(Math.max(size, 1), 200));
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of("total", events.count(), "byPartition", events.countByPartition());
    }

    /** Which topic and group this service reads; no broker addresses or other infrastructure detail. */
    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of("topic", props.topic(), "partitions", props.topicPartitions(), "consumerGroup", consumerGroup);
    }

    /** Clears the MongoDB read model only. The records stay in Kafka (consuming never deletes). */
    @DeleteMapping
    public ResponseEntity<Map<String, Long>> clear() {
        return ResponseEntity.ok(Map.of("deleted", events.deleteAll()));
    }
}
