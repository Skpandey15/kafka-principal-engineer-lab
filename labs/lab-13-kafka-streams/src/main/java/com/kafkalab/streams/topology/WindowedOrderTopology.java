package com.kafkalab.streams.topology;

import com.kafkalab.streams.support.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

/**
 * DSL concept: tumbling-window aggregation. Sums each customer's order
 * amounts within non-overlapping {@code windowSize} buckets --
 * {@code TimeWindows.ofSizeWithNoGrace} means a record arriving after
 * its window has already closed is dropped rather than retroactively
 * updating a closed window (no grace period configured here,
 * deliberately, to keep this lab's window-boundary tests fast and
 * deterministic).
 */
public final class WindowedOrderTopology {

    private WindowedOrderTopology() {
    }

    public static void build(StreamsBuilder builder, String inputTopic, String outputTopic, String storeName, Duration windowSize) {
        KStream<String, String> orders = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        orders
                .map((key, value) -> {
                    OrderEvent event = OrderEvent.parse(value);
                    return new KeyValue<>(event.customerId(), event.amount());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                .aggregate(
                        () -> 0.0,
                        (customerId, amount, runningTotal) -> runningTotal + amount,
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()))
                .toStream()
                .map((windowedKey, total) -> new KeyValue<>(windowedKey.key(),
                        "customerId=" + windowedKey.key() + "|windowStart=" + windowedKey.window().start() + "|windowSum=" + total))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }
}
