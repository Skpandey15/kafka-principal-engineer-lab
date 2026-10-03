package com.kafkalab.eventconsole.consumer.web;

import java.time.Clock;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.model.EventStatus;
import com.kafkalab.eventconsole.consumer.repo.EventQueryRepository;
import com.kafkalab.eventconsole.consumer.repo.EventRetryRepository;

/** The consumer's whole public surface: read what was consumed, and requeue what died. It accepts no event payloads. */
@RestController
@RequestMapping("/api/events")
public class ApiController {

    private final EventQueryRepository events;
    private final EventRetryRepository retries;
    private final AppProperties props;
    private final String consumerGroup;
    private final Clock clock;

    public ApiController(EventQueryRepository events, EventRetryRepository retries, AppProperties props,
            @Value("${spring.kafka.consumer.group-id}") String consumerGroup, Clock clock) {
        this.events = events;
        this.retries = retries;
        this.props = props;
        this.consumerGroup = consumerGroup;
        this.clock = clock;
    }

    @GetMapping
    public EventQueryRepository.Page events(
            @RequestParam(required = false) String key,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) EventStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {
        return events.search(key, partition, q, status, Math.max(page, 0), Math.min(Math.max(size, 1), 200));
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of("total", events.count(), "byPartition", events.countByPartition(), "byStatus",
                events.countByStatus());
    }

    /** Which topic and group this service reads; no broker addresses or other infrastructure detail. */
    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of("topic", props.topic(), "partitions", props.topicPartitions(), "consumerGroup", consumerGroup,
                "maxRetries", props.retry().maxRetries(), "deadLetterTopic", props.deadLetterTopic());
    }

    /** Gives a DEAD event a fresh retry budget. Only DEAD events can be requeued: anything else is still being handled. */
    @PostMapping("/{id}/requeue")
    public ResponseEntity<Map<String, Object>> requeue(@PathVariable String id) {
        if (retries.requeue(id, clock.instant())) {
            return ResponseEntity.ok(Map.of("requeued", true, "id", id));
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "No DEAD event with that id"));
    }

    /** Clears the MongoDB read model only. The records stay in Kafka (consuming never deletes). */
    @DeleteMapping
    public ResponseEntity<Map<String, Long>> clear() {
        return ResponseEntity.ok(Map.of("deleted", events.deleteAll()));
    }
}
