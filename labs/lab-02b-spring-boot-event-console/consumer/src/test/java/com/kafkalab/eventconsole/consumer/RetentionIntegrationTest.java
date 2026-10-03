package com.kafkalab.eventconsole.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import com.kafkalab.eventconsole.consumer.config.EventStoreSetup;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;

/**
 * Proves the retention rule against MongoDB's real TTL monitor, which wakes up once a minute: old
 * SUCCESS events are deleted, old FAILED and DEAD events are not. It is its own class (and needs
 * no Kafka) because a 3-second TTL would otherwise delete the other suites' fixtures mid-test.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetentionIntegrationTest {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("eventconsole_consumer_retention"));
        registry.add("app.events-ttl", () -> "3s");
        registry.add("app.retry.enabled", () -> "false");
        // No broker in this test: nothing consumes and nothing tries to reach one.
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("spring.kafka.admin.auto-create", () -> "false");
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1");
    }

    @Autowired
    MongoTemplate mongo;

    private static EventDocument event(String id, EventStatus status, Instant consumedAt) {
        return new EventDocument(id, "t", 0, 1, "k", "v", consumedAt, consumedAt, status, 0, null, null, null, null, null,
                status == EventStatus.DEAD ? Boolean.TRUE : null);
    }

    @Test
    void oldSuccessfulEventsExpireButUnfinishedOnesNeverDo() {
        IndexInfo ttl = mongo.indexOps(EventDocument.class).getIndexInfo().stream()
                .filter(i -> i.getName().equals(EventStoreSetup.TTL_INDEX)).findFirst().orElseThrow();
        assertThat(ttl.getExpireAfter()).contains(Duration.ofSeconds(3));

        Instant anHourAgo = Instant.now().minus(Duration.ofHours(1));
        mongo.insert(event("old-success", EventStatus.SUCCESS, anHourAgo));
        mongo.insert(event("old-failed", EventStatus.FAILED, anHourAgo));
        mongo.insert(event("old-dead", EventStatus.DEAD, anHourAgo));

        // MongoDB's TTL monitor runs every 60 seconds.
        await().atMost(Duration.ofSeconds(150)).pollInterval(Duration.ofSeconds(2))
                .until(() -> mongo.findById("old-success", EventDocument.class) == null);

        // The same pass that deleted the successful event looked at these two and left them alone.
        assertThat(mongo.findById("old-failed", EventDocument.class)).isNotNull();
        assertThat(mongo.findById("old-dead", EventDocument.class)).isNotNull();
    }
}
