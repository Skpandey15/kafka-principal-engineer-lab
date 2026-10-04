package com.kafkalab.eventconsole.producer.testsupport;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import com.sun.net.httpserver.HttpServer;

/**
 * A just-enough Confluent Schema Registry over real HTTP
 * ({@code GET /subjects/{subject}/versions/latest}), so the tests exercise the real client, real
 * timeouts and real failure modes. It can be switched "down" and it counts requests. The real
 * registry is exercised separately, on the k3d cluster.
 */
public final class FakeSchemaRegistry implements AutoCloseable {

    /** The v1 contract, identical to platform-k8s/event-console/contracts/event-v1.json. */
    public static final String V1 = """
            {"$schema":"http://json-schema.org/draft-07/schema#","type":"object","required":["id"],"additionalProperties":false,
             "properties":{"id":{"type":"string","minLength":1,"maxLength":128},"seq":{"type":"integer","minimum":0},
                           "batch":{"type":"string"},"ts":{"type":"string"},"status":{"type":"string"}}}
            """;

    private final HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();
    private volatile boolean down;
    private volatile int id;
    private volatile int version;
    private volatile String schemaType = "JSON";
    private volatile String schema;
    private volatile boolean subjectExists = true;

    public FakeSchemaRegistry(int id, int version, String schema) {
        this.id = id;
        this.version = version;
        this.schema = schema;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/subjects/", exchange -> {
            requests.incrementAndGet();
            int status = 200;
            String body;
            if (down) {
                status = 503;
                body = "{\"error_code\":50301,\"message\":\"unavailable\"}";
            } else if (!subjectExists) {
                status = 404;
                body = "{\"error_code\":40401,\"message\":\"Subject not found.\"}";
            } else {
                String subject = exchange.getRequestURI().getPath().split("/")[2];
                body = "{\"subject\":" + quote(subject) + ",\"version\":" + this.version + ",\"id\":" + this.id
                        + ",\"schemaType\":" + quote(schemaType) + ",\"schema\":" + quote(this.schema) + "}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    /** A new latest version is registered. */
    public void registerLatest(int id, int version, String schema) {
        this.id = id;
        this.version = version;
        this.schema = schema;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public void setSubjectExists(boolean exists) {
        this.subjectExists = exists;
    }

    public void setDown(boolean down) {
        this.down = down;
    }

    public int requestCount() {
        return requests.get();
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
}
