package com.kafkalab.connectcdc.cdc;

import com.kafkalab.connectcdc.support.ConnectRestClient;
import com.kafkalab.connectcdc.support.LabConfig;

import java.time.Duration;

/**
 * Phase 2's registration step: the real Debezium PostgreSQL connector,
 * against the reused {@code platform/kafka-connect/} environment.
 *
 * <p>{@code publication.autocreate.mode=disabled} because
 * {@code platform/kafka-connect/init-postgres.sql} already creates the
 * publication explicitly -- a deliberate choice documented there (a real
 * deployment's connector user often lacks the privilege to create
 * publications itself). {@code plugin.name=pgoutput} is PostgreSQL's own
 * built-in logical-decoding output plugin (available since Postgres 10),
 * NOT an extra decoder Debezium needs installed separately -- older
 * Debezium/Postgres material that still references {@code wal2json} or
 * {@code decoderbufs} predates pgoutput becoming the default,
 * recommended choice.
 */
public final class DebeziumConnectorRegistrationApp {

    public static void main(String[] args) throws Exception {
        String connectUrl = LabConfig.connectUrl();
        String connectorName = LabConfig.get("connectorName", "debezium-postgres-orders");
        String topicPrefix = LabConfig.get("topicPrefix", "cdc");
        String slotName = LabConfig.get("slotName", "debezium_orders_slot");

        ConnectRestClient client = new ConnectRestClient(connectUrl);

        String configJson = """
                {"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
                 "database.hostname":"connect-postgres",
                 "database.port":"5432",
                 "database.user":"postgres",
                 "database.password":"postgres",
                 "database.dbname":"inventory",
                 "topic.prefix":"%s",
                 "table.include.list":"public.orders",
                 "publication.name":"dbz_publication",
                 "publication.autocreate.mode":"disabled",
                 "slot.name":"%s",
                 "plugin.name":"pgoutput"}
                """.formatted(topicPrefix, slotName);

        System.out.printf("Registering Debezium PostgreSQL connector '%s' (topic.prefix=%s, slot=%s)...%n", connectorName, topicPrefix, slotName);
        var result = client.registerConnector(connectorName, configJson);
        System.out.println("  -> " + result);

        var status = client.waitForState(connectorName, "RUNNING", Duration.ofSeconds(30));
        System.out.println("  connector/task state: " + status);
        System.out.printf("Done. CDC events for the 'orders' table will appear on topic '%s.public.orders'.%n", topicPrefix);
    }
}
