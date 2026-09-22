package com.kafkalab.outbox.outbox;

import com.kafkalab.outbox.support.ConnectRestClient;
import com.kafkalab.outbox.support.LabConfig;

import java.time.Duration;

/**
 * Registers the real Debezium PostgreSQL connector configured with
 * Debezium's own {@code io.debezium.transforms.outbox.EventRouter}
 * single message transform -- confirmed present in the SAME Debezium
 * plugin bundle WP-11 already downloaded
 * (debezium-connect-plugins-3.6.3.Final.jar contains
 * io/debezium/transforms/outbox/EventRouter.class), no separate plugin
 * fetch needed for this WP.
 *
 * <p>Unlike WP-11's connector, {@code table.include.list} here names
 * ONLY {@code public.outbox_event} -- the business table
 * {@code outbox_demo_orders} is never captured. The EventRouter
 * transform then REWRITES the topic each captured row is produced to,
 * based on the row's own {@code aggregatetype} column value (default
 * {@code route.by.field}), before the record is ever produced -- the
 * raw, topic-prefix-based CDC topic this connector would otherwise use
 * never actually appears.
 */
public final class OutboxConnectorRegistrationApp {

    public static void main(String[] args) throws Exception {
        String connectUrl = LabConfig.connectUrl();
        String connectorName = LabConfig.get("connectorName", "debezium-postgres-outbox");
        String topicPrefix = LabConfig.get("topicPrefix", "outboxcdc");
        String slotName = LabConfig.get("slotName", "debezium_outbox_slot");
        String snapshotMode = LabConfig.get("snapshotMode", "no_data");

        ConnectRestClient connect = new ConnectRestClient(connectUrl);
        connect.registerConnector(connectorName, """
                {"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
                 "database.hostname":"connect-postgres","database.port":"5432",
                 "database.user":"postgres","database.password":"postgres","database.dbname":"inventory",
                 "topic.prefix":"%s","table.include.list":"public.outbox_event",
                 "publication.name":"dbz_outbox_publication","publication.autocreate.mode":"disabled",
                 "slot.name":"%s","plugin.name":"pgoutput","snapshot.mode":"%s",
                 "tombstones.on.delete":"false",
                 "transforms":"outbox",
                 "transforms.outbox.type":"io.debezium.transforms.outbox.EventRouter",
                 "transforms.outbox.route.by.field":"aggregatetype",
                 "transforms.outbox.route.topic.replacement":"outbox.event.${routedByValue}",
                 "transforms.outbox.table.field.event.id":"id",
                 "transforms.outbox.table.field.event.key":"aggregateid",
                 "transforms.outbox.table.field.event.payload":"payload"}
                """.formatted(topicPrefix, slotName, snapshotMode));

        System.out.println("[outbox-connector] registered '" + connectorName + "', waiting for RUNNING...");
        var status = connect.waitForState(connectorName, "RUNNING", Duration.ofSeconds(30));
        System.out.println("[outbox-connector] status: " + status.toPrettyString());
    }
}
