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
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * DSL concept: stateful aggregation into a state store. Re-keys the
 * order stream by {@code customerId} and maintains a running total,
 * materialized so it can be queried directly (interactive queries) as
 * well as emitted as a changelog stream on {@code outputTopic}. This
 * materialized state store is exactly what {@code docs/roadmap}'s
 * failure-matrix rows for this WP are about: it's backed by an
 * internal Kafka changelog topic, which is what makes it survive a
 * process crash (see {@link com.kafkalab.streams.support}).
 */
public final class OrderAggregationTopology {

    private OrderAggregationTopology() {
    }

    public static void build(StreamsBuilder builder, String inputTopic, String outputTopic, String storeName) {
        KStream<String, String> orders = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        orders
                .map((key, value) -> {
                    OrderEvent event = OrderEvent.parse(value);
                    return new KeyValue<>(event.customerId(), event.amount());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .aggregate(
                        () -> 0.0,
                        (customerId, amount, runningTotal) -> runningTotal + amount,
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()))
                .toStream()
                .mapValues((customerId, total) -> "customerId=" + customerId + "|runningTotal=" + total)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }
}
