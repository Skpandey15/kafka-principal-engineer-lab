package com.kafkalab.streams.topology;

import com.kafkalab.streams.support.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnrichedOrderTopologyTest {

    @Test
    void streamTableJoinEnrichesMatchedOrdersAndDropsUnmatchedOnes() {
        StreamsBuilder builder = new StreamsBuilder();
        EnrichedOrderTopology.build(builder, "orders-in", "customers-in", "enriched-out");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-enriched");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> customers = driver.createInputTopic("customers-in", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> orders = driver.createInputTopic("orders-in", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("enriched-out", Serdes.String().deserializer(), Serdes.String().deserializer());

            // The customer profile must land in the KTable BEFORE the
            // matching order arrives for the join to find it.
            customers.pipeInput("C-1", "name=Alice|tier=GOLD");
            orders.pipeInput("k1", new OrderEvent("E1", "C-1", 10.0).toWireFormat());

            assertEquals(1, output.getQueueSize());
            String enriched = output.readValue();
            assertTrue(enriched.contains("name=Alice|tier=GOLD"));
            assertTrue(enriched.contains("customerId=C-1"));

            // C-2 was never added to the customer KTable -- an INNER
            // join must drop this order entirely, not emit a
            // null-enriched record.
            orders.pipeInput("k2", new OrderEvent("E2", "C-2", 20.0).toWireFormat());
            assertTrue(output.isEmpty(), "an order for a customerId absent from the KTable must produce NO output record (inner join)");
        }
    }
}
