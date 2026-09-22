package com.kafkalab.observability.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * Computes real per-partition consumer-group lag the SAME way
 * `kafka-consumer-groups.sh --describe` and kafka-exporter both do:
 * committed offset (from {@code Admin.listConsumerGroupOffsets}) versus
 * the partition's real log-end offset (from
 * {@code Admin.listOffsets(..., OffsetSpec.latest())}), subtracted --
 * NOT a broker-side JMX metric (there isn't one for this; see
 * `platform/observability/prometheus.yml`'s own comment on why
 * kafka-exporter exists as a separate scrape target).
 */
public final class LagInspector {

    private LagInspector() {
    }

    public static Map<TopicPartition, Long> computeLag(Admin admin, String groupId, String topic) throws Exception {
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
        for (TopicPartition tp : committed.keySet()) {
            if (tp.topic().equals(topic)) {
                latestRequest.put(tp, OffsetSpec.latest());
            }
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                admin.listOffsets(latestRequest).all().get();

        Map<TopicPartition, Long> lag = new HashMap<>();
        for (var entry : latestRequest.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committedOffset = committed.get(tp).offset();
            long endOffset = endOffsets.get(tp).offset();
            lag.put(tp, endOffset - committedOffset);
        }
        return lag;
    }

    public static long totalLag(Admin admin, String groupId, String topic) throws Exception {
        return computeLag(admin, groupId, topic).values().stream().mapToLong(Long::longValue).sum();
    }
}
