package com.kafkalab.observability.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A small, direct wrapper around Prometheus's real HTTP query API
 * ({@code GET /api/v1/query}) using the JDK's own {@link HttpClient} --
 * same pattern as WP-11's {@code ConnectRestClient} and every other
 * lab's hand-rolled REST wrapper: there is no dedicated Java client
 * library this repository already depends on, and a raw HTTP call
 * against Prometheus's own documented API is both simpler and more
 * directly educational than adding one.
 */
public final class PrometheusQueryClient {

    /** One label-set + value pair from a Prometheus instant query result. */
    public record Sample(Map<String, String> labels, double value) {
    }

    private final String baseUrl;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public PrometheusQueryClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /** Runs a real PromQL instant query and returns every result series' current value. */
    public List<Sample> query(String promql) throws Exception {
        String encoded = URLEncoder.encode(promql, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/query?query=" + encoded))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode root = mapper.readTree(response.body());
        if (!"success".equals(root.path("status").asText())) {
            throw new IllegalStateException("Prometheus query failed: " + response.body());
        }
        List<Sample> samples = new ArrayList<>();
        for (JsonNode result : root.path("data").path("result")) {
            Map<String, String> labels = new java.util.LinkedHashMap<>();
            result.path("metric").fields().forEachRemaining(e -> labels.put(e.getKey(), e.getValue().asText()));
            double value = Double.parseDouble(result.path("value").get(1).asText());
            samples.add(new Sample(labels, value));
        }
        return samples;
    }

    /** Convenience for a query expected to return exactly one series -- sums all matching series' values otherwise. */
    public double queryScalarSum(String promql) throws Exception {
        double total = 0;
        for (Sample sample : query(promql)) {
            total += sample.value();
        }
        return total;
    }
}
