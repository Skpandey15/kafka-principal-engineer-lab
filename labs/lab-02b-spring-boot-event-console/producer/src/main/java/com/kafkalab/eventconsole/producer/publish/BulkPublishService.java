package com.kafkalab.eventconsole.producer.publish;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.MeterRegistry;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.contract.ContractUnavailableException;
import com.kafkalab.eventconsole.producer.contract.ContractViolationException;
import com.kafkalab.eventconsole.producer.contract.EventContract;
import com.kafkalab.eventconsole.producer.model.PublishJob;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.KeyStrategy;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.Mode;
import com.kafkalab.eventconsole.producer.repo.PublishJobRepository;

@Service
public class BulkPublishService {

    /** Kafka header naming the Schema Registry id of the contract the event was checked against. */
    public static final String SCHEMA_ID_HEADER = "x-schema-id";

    private record Outgoing(String key, String value) {
    }

    private final KafkaTemplate<String, String> kafka;
    private final PublishJobRepository jobs;
    private final AppProperties props;
    private final MeterRegistry metrics;
    private final EventContract contract;

    public BulkPublishService(KafkaTemplate<String, String> kafka, PublishJobRepository jobs, AppProperties props, EventContract contract,
            MeterRegistry metrics) {
        this.kafka = kafka;
        this.jobs = jobs;
        this.props = props;
        this.metrics = metrics;
        this.contract = contract;
    }

    public PublishJob publish(BulkPublishRequest req) {
        String topic = props.topic();
        List<Outgoing> batch = buildBatch(req);

        // The contract is enforced at the edge, before anything is sent: a bad event is cheapest to
        // stop here, and this is the only place its sender can still be told what is wrong. All or
        // nothing -- a request with one bad event publishes none, so nobody has to work out what got through.
        EventContract.Current schema;
        try {
            schema = contract.current();
            contract.requireAllValid(schema, batch.stream().map(Outgoing::key).toList(),
                    batch.stream().map(Outgoing::value).toList());
        } catch (ContractViolationException rejected) {
            metrics.counter("eventconsole.publish.rejected").increment(rejected.total());
            throw rejected;
        } catch (ContractUnavailableException unavailable) {
            metrics.counter("eventconsole.contract.unavailable").increment();
            throw unavailable;
        }
        byte[] schemaId = String.valueOf(schema.id()).getBytes(StandardCharsets.UTF_8);
        long started = System.nanoTime();

        // send() only enqueues the record in the producer's buffer; the returned
        // future (the callback) is the one thing that says the broker stored it.
        // So: fire every send first, THEN wait for every future.
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(batch.size());
        for (Outgoing o : batch) {
            // The header tells every consumer which version of the contract this event was checked against.
            futures.add(kafka.send(new ProducerRecord<>(topic, null, o.key(), o.value(),
                    List.of(new RecordHeader(SCHEMA_ID_HEADER, schemaId)))));
        }

        Map<String, Long> perPartition = new TreeMap<>(java.util.Comparator.comparingInt(Integer::parseInt));
        int acked = 0;
        int failed = 0;
        String firstError = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        for (CompletableFuture<SendResult<String, String>> f : futures) {
            try {
                long remaining = Math.max(1, deadline - System.nanoTime());
                RecordMetadata md = f.get(remaining, TimeUnit.NANOSECONDS).getRecordMetadata();
                acked++;
                perPartition.merge(String.valueOf(md.partition()), 1L, Long::sum);
            } catch (Exception e) {
                failed++;
                if (firstError == null) {
                    Throwable root = e.getCause() != null ? e.getCause() : e;
                    firstError = root.getClass().getSimpleName() + ": " + root.getMessage();
                }
            }
        }
        kafka.flush();
        metrics.counter("eventconsole.publish.acked").increment(acked);
        metrics.counter("eventconsole.publish.failed").increment(failed);

        PublishJob job = new PublishJob(
                UUID.randomUUID().toString(),
                Instant.now(),
                topic,
                req.mode().name(),
                req.mode() == Mode.GENERATE ? effectiveStrategy(req).name() : "PASTED",
                batch.size(),
                acked,
                failed,
                (System.nanoTime() - started) / 1_000_000,
                schema.id(),
                schema.version(),
                perPartition,
                firstError);
        return jobs.save(job);
    }

    private List<Outgoing> buildBatch(BulkPublishRequest req) {
        if (req.mode() == Mode.PASTE) {
            if (req.events() == null || req.events().isEmpty()) {
                throw new IllegalArgumentException("PASTE mode needs at least one event");
            }
            if (req.events().size() > props.maxBulkEvents()) {
                throw new IllegalArgumentException("At most " + props.maxBulkEvents() + " events per bulk publish");
            }
            return req.events().stream()
                    .map(e -> new Outgoing(blankToNull(e.key()), e.value()))
                    .toList();
        }

        if (req.count() == null) {
            throw new IllegalArgumentException("GENERATE mode needs a count");
        }
        if (req.count() > props.maxBulkEvents()) {
            throw new IllegalArgumentException("At most " + props.maxBulkEvents() + " events per bulk publish");
        }
        KeyStrategy strategy = effectiveStrategy(req);
        String keyPrefix = req.keyPrefix() == null || req.keyPrefix().isBlank() ? "order-" : req.keyPrefix();
        String valuePrefix = req.valuePrefix() == null || req.valuePrefix().isBlank() ? "evt" : req.valuePrefix();
        int keyCount = req.keyCount() == null ? 10 : req.keyCount();
        String fixedKey = req.fixedKey() == null || req.fixedKey().isBlank() ? "hot-key" : req.fixedKey();
        String batchId = UUID.randomUUID().toString().substring(0, 8);

        List<Outgoing> out = new ArrayList<>(req.count());
        for (int i = 0; i < req.count(); i++) {
            String key = switch (strategy) {
                case UNIQUE -> keyPrefix + batchId + "-" + i;
                case CYCLE -> keyPrefix + (i % keyCount);
                case FIXED -> fixedKey;
                case NONE -> null;
            };
            String value = "{\"id\":\"" + valuePrefix + "-" + batchId + "-" + i + "\",\"seq\":" + i
                    + ",\"batch\":\"" + batchId + "\",\"ts\":\"" + Instant.now() + "\"}";
            out.add(new Outgoing(key, value));
        }
        return out;
    }

    private static KeyStrategy effectiveStrategy(BulkPublishRequest req) {
        return req.keyStrategy() == null ? KeyStrategy.UNIQUE : req.keyStrategy();
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
