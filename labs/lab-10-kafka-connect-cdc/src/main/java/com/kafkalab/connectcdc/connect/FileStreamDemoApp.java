package com.kafkalab.connectcdc.connect;

import com.kafkalab.connectcdc.support.ConnectRestClient;
import com.kafkalab.connectcdc.support.LabConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;

/**
 * Sections 1-4's experiment: pure Apache Kafka Connect, no CDC yet --
 * real {@code FileStreamSourceConnector} and {@code FileStreamSinkConnector}
 * instances, registered against the real Connect worker
 * ({@code platform/kafka-connect/}), demonstrating the worker/task/offset
 * model with the connectors that ship inside Apache Kafka's own
 * distribution (not a vendor plugin).
 *
 * <p>The connector configs below reference {@code /data/...} -- the path
 * INSIDE the {@code connect-worker} container, where
 * {@code platform/kafka-connect/connect-data/} is bind-mounted. This app
 * writes to that SAME directory from the host side
 * ({@code ../../platform/kafka-connect/connect-data/}), so a file this
 * process appends to on the host is the exact file the connector reads
 * inside the container.
 *
 * <h2>A real finding this app's connector/topic/file names are built
 * around</h2>
 * Kafka Connect's offset storage ({@code connect-offsets}) is keyed by
 * {@code (connector name, connector-defined source partition)} -- for
 * {@code FileStreamSourceConnector}, that key is
 * {@code (connectorName, {"filename": "..."})}. This storage is
 * DURABLE and OUTLIVES a connector's own delete/recreate lifecycle:
 * deleting a connector via the REST API does NOT clear its offsets. A
 * real, concrete consequence hit while building this lab: re-running an
 * earlier version of this app with a SHORTER file than a previous run,
 * reusing the same connector name, silently sent ZERO records -- the
 * task's stored position (from the longer, earlier file) was already
 * past the new, shorter file's end. This is exactly why every connector
 * name and topic below includes a fresh, unique suffix per run -- not
 * cosmetic, a genuine correctness requirement for a REPEATABLE demo.
 * (A real Debezium deployment's `slot.name` faces the identical
 * consideration, at the PostgreSQL replication-slot level -- see the
 * conceptual doc's CDC section.)
 */
public final class FileStreamDemoApp {

    public static void main(String[] args) throws Exception {
        String connectUrl = LabConfig.connectUrl();
        String runId = String.valueOf(System.currentTimeMillis());
        String topic = "connect-fundamentals-orders-" + runId;
        String sourceConnectorName = "file-source-orders-" + runId;
        String sinkConnectorName = "file-sink-orders-" + runId;

        Path hostDataDir = Path.of("../../platform/kafka-connect/connect-data");
        Path sourceFile = hostDataDir.resolve("source-orders-" + runId + ".txt");
        Path sinkFile = hostDataDir.resolve("sink-orders-" + runId + ".txt");

        Files.createDirectories(hostDataDir);
        Files.writeString(sourceFile, "eventId=Order-1|customerId=C-501|amount=10.00\n");

        ConnectRestClient client = new ConnectRestClient(connectUrl);

        System.out.printf("Registering FileStreamSourceConnector '%s' (reads /data/%s -> topic %s)...%n",
                sourceConnectorName, sourceFile.getFileName(), topic);
        var sourceResult = client.registerConnector(sourceConnectorName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
                 "tasks.max":"1","file":"/data/%s","topic":"%s"}
                """.formatted(sourceFile.getFileName(), topic));
        System.out.println("  -> " + sourceResult);
        var sourceStatus = client.waitForState(sourceConnectorName, "RUNNING", Duration.ofSeconds(20));
        System.out.println("  source connector/task state: " + sourceStatus);

        System.out.printf("Registering FileStreamSinkConnector '%s' (topic %s -> /data/%s)...%n",
                sinkConnectorName, topic, sinkFile.getFileName());
        var sinkResult = client.registerConnector(sinkConnectorName, """
                {"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector",
                 "tasks.max":"1","file":"/data/%s","topics":"%s"}
                """.formatted(sinkFile.getFileName(), topic));
        System.out.println("  -> " + sinkResult);
        var sinkStatus = client.waitForState(sinkConnectorName, "RUNNING", Duration.ofSeconds(20));
        System.out.println("  sink connector/task state: " + sinkStatus);

        Thread.sleep(8000);
        System.out.println("sink file contents after the first line: " + Files.readString(sinkFile));

        System.out.println("Appending a SECOND line to the source file -- the source task's own offset (its position in this file, not a Kafka offset) determines it only sends the NEW line, not a resend of the first.");
        Files.writeString(sourceFile, "eventId=Order-2|customerId=C-502|amount=20.00\n", StandardOpenOption.APPEND);
        Thread.sleep(8000);
        System.out.println("sink file contents after the second line: " + Files.readString(sinkFile));

        System.out.println();
        System.out.printf("Done. Inspect the real offset record with: ./gradlew inspectConnectOffsets (look for connector name '%s')%n", sourceConnectorName);
    }
}
