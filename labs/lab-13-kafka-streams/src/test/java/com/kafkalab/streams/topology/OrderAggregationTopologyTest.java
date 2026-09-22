package com.kafkalab.streams.topology;

import com.kafkalab.streams.support.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TopologyTestDriver -- an in-memory Kafka Streams test harness, no
 * real broker at all. A different, much faster testing style from
 * every prior lab's Testcontainers-only suites, and the exact tool
 * `docs/references/REFERENCE_REPOSITORIES.md` names for this WP.
 */
class OrderAggregationTopologyTest {

    @Test
    void runningTotalAggregatesAcrossMultipleOrdersForTheSameCustomer() {
        StreamsBuilder builder = new StreamsBuilder();
        OrderAggregationTopology.build(builder, "orders-in", "totals-out", "totals-store");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-aggregation");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> input = driver.createInputTopic("orders-in", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("totals-out", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("k1", new OrderEvent("E1", "C-1", 10.0).toWireFormat());
            input.pipeInput("k2", new OrderEvent("E2", "C-1", 5.0).toWireFormat());

            List<String> values = output.readValuesToList();
            assertEquals("customerId=C-1|runningTotal=10.0", values.get(0));
            assertEquals("customerId=C-1|runningTotal=15.0", values.get(values.size() - 1));

            ReadOnlyKeyValueStore<String, Double> store = driver.getKeyValueStore("totals-store");
            assertEquals(15.0, store.get("C-1"), "the materialized state store must be directly queryable (interactive queries)");
        }
    }
}
