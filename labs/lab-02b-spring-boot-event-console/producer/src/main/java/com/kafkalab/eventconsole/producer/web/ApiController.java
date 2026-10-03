package com.kafkalab.eventconsole.producer.web;

import java.util.List;
import java.util.Map;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import jakarta.validation.Valid;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.model.PublishJob;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest;
import com.kafkalab.eventconsole.producer.publish.BulkPublishService;
import com.kafkalab.eventconsole.producer.repo.PublishJobRepository;

/** The producer's whole public surface: publish, and the audit log of what was published. */
@RestController
@RequestMapping("/api")
public class ApiController {

    private final BulkPublishService publisher;
    private final PublishJobRepository jobs;
    private final AppProperties props;

    public ApiController(BulkPublishService publisher, PublishJobRepository jobs, AppProperties props) {
        this.publisher = publisher;
        this.jobs = jobs;
        this.props = props;
    }

    /** What the UI needs to build its form. Deliberately no broker addresses or other infrastructure detail. */
    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of(
                "topic", props.topic(),
                "partitions", props.topicPartitions(),
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
}
