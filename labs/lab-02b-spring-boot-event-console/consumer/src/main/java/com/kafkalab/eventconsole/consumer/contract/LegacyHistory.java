package com.kafkalab.eventconsole.consumer.contract;

import java.util.Map;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.config.AppProperties;

/**
 * Which part of the topic was written BEFORE the event contract existed.
 *
 * <p>Introducing a contract into a topic that already holds history creates a problem the contract
 * itself cannot solve: every old record lacks the {@code x-schema-id} header, so replaying the topic
 * (a new consumer group, a rebuilt read model) would fail every one of them. Treating a missing
 * header as "legacy" everywhere would be wrong too -- it would excuse exactly the records the
 * contract exists to catch, a producer that bypasses the service.
 *
 * <p>The answer is a <b>cut-over per partition</b>: records below the configured offset are legacy and
 * are accepted as they are; everything at or above it must carry a header. An offset is assigned by
 * the broker when the record is written, so unlike a header or a producer-set timestamp it cannot be
 * forged by the very producer being checked. With nothing configured (the default), no record is
 * legacy and the contract applies to the whole topic.
 *
 * <p>The cut-over is a fact about the topic, recorded once at the moment the contract was introduced
 * ({@code platform-k8s/event-console/contracts/set-cutover.sh}); it never moves afterwards.
 */
@Component
public class LegacyHistory {

    private final Map<Integer, Long> legacyBelow;

    public LegacyHistory(AppProperties props) {
        this.legacyBelow = Map.copyOf(props.schema().legacyUntilOffsets());
    }

    /** True if the record at this address predates the contract. */
    public boolean covers(int partition, long offset) {
        Long limit = legacyBelow.get(partition);
        return limit != null && offset < limit;
    }

    public boolean isConfigured() {
        return !legacyBelow.isEmpty();
    }
}
