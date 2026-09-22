package com.kafkalab.streams.app;

import com.kafkalab.streams.support.LabConfig;
import com.kafkalab.streams.topology.EnrichedOrderTopology;
import com.kafkalab.streams.topology.OrderAggregationTopology;
import com.kafkalab.streams.topology.WindowedOrderTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.Properties;

/**
 * A real, runnable Kafka Streams application instance for manual
 * walkthroughs. {@code -Ptopology=aggregation|windowed|enriched} picks
 * which of this lab's three topologies to run;
 * {@code -PprocessingGuarantee=exactly_once_v2} (the default is
 * {@code at_least_once}) wires the same producer/consumer transactional
 * machinery WP-09 covered, this time configured for you by the Streams
 * library instead of hand-implemented.
 */
public final class StreamsApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = LabConfig.bootstrapServers();
        String applicationId = LabConfig.get("applicationId", "lab-13-streams-app");
        String topologyName = LabConfig.get("topology", "aggregation");
        String processingGuarantee = LabConfig.get("processingGuarantee", StreamsConfig.AT_LEAST_ONCE);
        int numStandbyReplicas = LabConfig.getInt("numStandbyReplicas", 0);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);

        StreamsBuilder builder = new StreamsBuilder();
        switch (topologyName) {
            case "aggregation" -> OrderAggregationTopology.build(builder, "streams-orders", "streams-order-totals", "order-totals-store");
            case "windowed" -> WindowedOrderTopology.build(builder, "streams-orders", "streams-windowed-totals", "windowed-totals-store", Duration.ofSeconds(10));
            case "enriched" -> EnrichedOrderTopology.build(builder, "streams-orders", "streams-customers", "streams-enriched-orders");
            default -> throw new IllegalArgumentException("unknown topology: " + topologyName);
        }

        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.setStateListener((newState, oldState) -> System.out.println("[streams] state: " + oldState + " -> " + newState));
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            streams.start();
            System.out.println("[streams] running topology '" + topologyName + "' as application.id=" + applicationId
                    + " (processing.guarantee=" + processingGuarantee + ", num.standby.replicas=" + numStandbyReplicas + ")");
            Thread.currentThread().join();
        }
    }
}
