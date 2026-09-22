package com.kafkalab.outbox.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Same small Kafka Connect REST API wrapper as WP-11's lab-10 --
 * copied rather than shared, per this lab's README ("Why a separate
 * project"). See lab-10's ConnectRestClient for why this hand-written
 * HttpClient wrapper is the right tool (there is no dedicated "Kafka
 * Connect Java client" library).
 */
public final class ConnectRestClient {

    private final String baseUrl;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public ConnectRestClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public JsonNode registerConnector(String name, String configJson) throws Exception {
        String body = "{\"name\":\"" + name + "\",\"config\":" + configJson + "}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return mapper.readTree(response.body());
    }

    public JsonNode status(String name) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors/" + name + "/status"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return mapper.readTree(response.body());
    }

    public int delete(String name) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors/" + name))
                .timeout(Duration.ofSeconds(10))
                .DELETE()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }

    /** Polls {@code GET /connectors/{name}/status} until the connector AND every task report the given state, or the timeout elapses. */
    public JsonNode waitForState(String name, String expectedState, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        JsonNode last = null;
        while (System.currentTimeMillis() < deadline) {
            last = status(name);
            JsonNode connectorState = last.path("connector").path("state");
            boolean connectorMatches = connectorState.asText("").equals(expectedState);
            boolean tasksMatch = true;
            for (JsonNode task : last.path("tasks")) {
                if (!task.path("state").asText("").equals(expectedState)) {
                    tasksMatch = false;
                }
            }
            if (connectorMatches && tasksMatch && last.path("tasks").size() > 0) {
                return last;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("connector '" + name + "' did not reach state '" + expectedState + "' within " + timeout + "; last status: " + last);
    }
}
