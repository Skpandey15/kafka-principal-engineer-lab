package com.kafkalab.eventconsole.web;

import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import jakarta.validation.Valid;
import com.kafkalab.eventconsole.config.AppProperties;
import com.kafkalab.eventconsole.model.PublishJob;
import com.kafkalab.eventconsole.publish.BulkPublishRequest;
import com.kafkalab.eventconsole.publish.BulkPublishService;
import com.kafkalab.eventconsole.repo.EventQueryRepository;
import com.kafkalab.eventconsole.repo.PublishJobRepository;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final BulkPublishService publisher;
    private final EventQueryRepository events;
    private final PublishJobRepository jobs;
    private final AppProperties props;
    private final String consumerGroup;
    private final String bootstrapServers;

    public ApiController(BulkPublishService publisher, EventQueryRepository events, PublishJobRepository jobs,
            AppProperties props,
            @Value("${spring.kafka.consumer.group-id}") String consumerGroup,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.publisher = publisher;
        this.events = events;
        this.jobs = jobs;
        this.props = props;
        this.consumerGroup = consumerGroup;
        this.bootstrapServers = bootstrapServers;
    }

    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of(
                "topic", props.topic(),
                "partitions", props.topicPartitions(),
                "consumerGroup", consumerGroup,
                "bootstrapServers", bootstrapServers,
                "maxBulkEvents", props.maxBulkEvents());
    }

    @PostMapping("/publish/bulk")
    public PublishJob publish(@Valid @RequestBody BulkPublishRequest request) {
        return publisher.publish(request);
    }

    @GetMapping("/jobs")
    public List<PublishJob> jobs(@RequestParam(defaultValue = "10") int limit) {
        return jobs.findAllByOrderByCreatedAtDesc(PageRequest.of(0, Math.min(Math.max(limit, 1), 100)));
    }

    @GetMapping("/events")
    public EventQueryRepository.Page events(
            @RequestParam(required = false) String key,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {
        return events.search(key, partition, q, Math.max(page, 0), Math.min(Math.max(size, 1), 200));
    }

    @GetMapping("/events/stats")
    public Map<String, Object> stats() {
        return Map.of("total", events.count(), "byPartition", events.countByPartition());
    }

    /** Clears the MongoDB read model only. The records stay in Kafka (consuming never deletes). */
    @DeleteMapping("/events")
    public ResponseEntity<Map<String, Long>> clear() {
        return ResponseEntity.ok(Map.of("deleted", events.deleteAll()));
    }
}
