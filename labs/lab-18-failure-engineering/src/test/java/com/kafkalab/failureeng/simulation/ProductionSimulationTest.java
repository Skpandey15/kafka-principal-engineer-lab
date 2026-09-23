package com.kafkalab.failureeng.simulation;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * "A production-simulation lab combining prior labs into one running
 * system" -- this WP's own roadmap language, taken literally: real,
 * continuous production and consumption, with a real broker KILL
 * (WP-07's mechanic) and a real consumer-group REBALANCE (WP-05's
 * mechanic, triggered by a second consumer instance joining)
 * happening close together, not in isolation the way each WP's own
 * lab demonstrated it alone. The question this test actually answers:
 * do the guarantees each WP proved individually still hold when
 * multiple real failures overlap, the way they actually do in
 * production?
 */
class ProductionSimulationTest {

    @Test
    void noDataIsLostWhenABrokerFailureAndAConsumerRebalanceOverlap() throws Exception {
        Map<Integer, Integer> nodeIdToHostPort = Map.of(1, 19_710, 2, 19_711, 3, 19_712);
        String topic = "production-sim";
        String groupId = "production-sim-group";

        try (ThreeBrokerSimulationCluster cluster = new ThreeBrokerSimulationCluster(nodeIdToHostPort)) {
            cluster.start();
            String bootstrapServers = cluster.bootstrapServers();

            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                // RF=3, min.insync.replicas=2 (set per-broker above) --
                // the SAME real durability configuration WP-07 covers,
                // chosen specifically so the topic tolerates losing
                // exactly one broker without becoming unwritable.
                admin.createTopics(List.of(new NewTopic(topic, 6, (short) 3))).all().get();
            }

            Set<String> producedIds = new CopyOnWriteArraySet<>();
            Set<String> consumedIds = new CopyOnWriteArraySet<>();
            Map<String, Integer> consumeCounts = new ConcurrentHashMap<>();
            AtomicBoolean keepProducing = new AtomicBoolean(true);
            AtomicBoolean keepConsuming = new AtomicBoolean(true);
            AtomicInteger nextId = new AtomicInteger();

            ExecutorService executor = Executors.newFixedThreadPool(4);

            // A real, continuous producer -- acks=all, so a successful
            // send is a real durability guarantee (WP-07's own acks
            // mechanic), not just "the client thinks it worked."
            Runnable producerTask = () -> {
                Map<String, Object> props = Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.RETRIES_CONFIG, 10,
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    while (keepProducing.get()) {
                        String id = "id-" + nextId.getAndIncrement();
                        try {
                            producer.send(new ProducerRecord<>(topic, id, id)).get(20, TimeUnit.SECONDS);
                            producedIds.add(id);
                        } catch (Exception e) {
                            // A real send failure during the broker-kill
                            // window is expected -- this record is simply
                            // NOT counted as produced (never durably
                            // committed), consistent with what "acks=all
                            // failed" really means.
                        }
                        Thread.sleep(20);
                    }
                } catch (InterruptedException ignored) {
                }
            };

            Runnable consumerTask = () -> {
                Map<String, Object> props = Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(List.of(topic));
                    while (keepConsuming.get()) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                        records.forEach(r -> {
                            consumedIds.add(r.value());
                            consumeCounts.merge(r.value(), 1, Integer::sum);
                        });
                        consumer.commitAsync();
                    }
                }
            };

            // Steady-state traffic first, so there's real, meaningful
            // production/consumption already in flight before either
            // failure hits -- not a cold start racing the failures.
            executor.submit(producerTask);
            executor.submit(consumerTask);
            Thread.sleep(5_000);

            // The broker failure (WP-07's mechanic): a real, hard KILL,
            // not a graceful stop.
            cluster.killBroker(2);

            // The consumer rebalance (WP-05's mechanic), deliberately
            // overlapping the broker-failure recovery window, not
            // sequenced after it -- a second group member joins WHILE
            // the cluster is still recovering from losing a broker.
            Thread.sleep(2_000);
            executor.submit(consumerTask);

            // A real recovery window -- both failures need real time to
            // settle (leader re-election, rebalance completion) before
            // asserting anything.
            Thread.sleep(20_000);

            keepProducing.set(false);
            Thread.sleep(3_000);
            keepConsuming.set(false);
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            Set<String> lost = new java.util.HashSet<>(producedIds);
            lost.removeAll(consumedIds);
            long duplicated = consumeCounts.values().stream().filter(c -> c > 1).count();

            assertTrue(lost.isEmpty(),
                    "every record whose send() was ACKNOWLEDGED (acks=all) must eventually be consumed, "
                            + "even with a broker kill AND a consumer rebalance overlapping -- lost: " + lost.size()
                            + " of " + producedIds.size() + " produced");
            // Duplicates are EXPECTED, not a failure -- at-least-once
            // delivery (WP-06's own mental model) under a real rebalance
            // means some records legitimately get reprocessed. Logged
            // as real evidence, not asserted against.
            System.out.println("[production-sim] produced=" + producedIds.size()
                    + " consumed(unique)=" + consumedIds.size()
                    + " duplicated-keys=" + duplicated);
        }
    }
}
