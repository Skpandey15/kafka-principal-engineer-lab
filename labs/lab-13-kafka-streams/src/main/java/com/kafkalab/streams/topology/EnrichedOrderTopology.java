package com.kafkalab.streams.topology;

import com.kafkalab.streams.support.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/**
 * DSL concept: a stream-table join. Re-keys the order stream by
 * {@code customerId} (Kafka Streams automatically inserts an internal
 * repartition topic here, since the key changed upstream of the join)
 * and joins it against a {@code KTable} of customer profiles.
 * {@link KStream#join(KTable, org.apache.kafka.streams.kstream.ValueJoiner)}
 * is an INNER join -- an order for a customerId not (yet) present in
 * the profile table produces NO output record at all, not a
 * null-enriched one.
 */
public final class EnrichedOrderTopology {

    private EnrichedOrderTopology() {
    }

    public static void build(StreamsBuilder builder, String orderTopic, String customerTopic, String outputTopic) {
        KStream<String, String> orders = builder.stream(orderTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> customers = builder.table(customerTopic, Consumed.with(Serdes.String(), Serdes.String()));

        orders
                .map((key, value) -> new KeyValue<>(OrderEvent.parse(value).customerId(), value))
                // A real finding building this topology: after .map()
                // changes the key, the DSL no longer has an implicit
                // key serde to carry into the join -- Joined.with(...)
                // must supply it explicitly, or topology initialization
                // fails with "Please specify a key serde" even though
                // the downstream .to() already has its own Produced.
                .join(customers, (orderValue, customerValue) -> "order=[" + orderValue + "] customer=[" + customerValue + "]",
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }
}
