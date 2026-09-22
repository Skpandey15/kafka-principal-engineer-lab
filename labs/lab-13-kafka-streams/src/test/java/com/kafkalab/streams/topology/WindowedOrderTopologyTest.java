package com.kafkalab.streams.topology;

import com.kafkalab.streams.support.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowedOrderTopologyTest {

    @Test
    void tumblingWindowSumsOrdersWithinAWindowAndStartsFreshInTheNextWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        WindowedOrderTopology.build(builder, "orders-in", "windowed-out", "windowed-store", Duration.ofSeconds(10));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-windowed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> input = driver.createInputTopic("orders-in", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("windowed-out", Serdes.String().deserializer(), Serdes.String().deserializer());

            Instant windowStart = Instant.parse("2026-01-01T00:00:00Z");
            input.pipeInput("k1", new OrderEvent("E1", "C-1", 10.0).toWireFormat(), windowStart);
            input.pipeInput("k2", new OrderEvent("E2", "C-1", 5.0).toWireFormat(), windowStart.plusSeconds(2));
            // Past the 10s tumbling window boundary -- a NEW window, not a continuation.
            input.pipeInput("k3", new OrderEvent("E3", "C-1", 7.0).toWireFormat(), windowStart.plusSeconds(12));

            List<KeyValue<String, String>> records = output.readKeyValuesToList();
            assertEquals(3, records.size(), "one output record per aggregate UPDATE, not one per window");
            assertTrue(records.get(0).value.contains("windowSum=10.0"));
            assertTrue(records.get(1).value.contains("windowSum=15.0"), "second order in the SAME window must add to the running sum");
            assertTrue(records.get(2).value.contains("windowSum=7.0"),
                    "the third order falls in a NEW window and must start fresh at 7.0, not continue accumulating to 22.0");
        }
    }
}
