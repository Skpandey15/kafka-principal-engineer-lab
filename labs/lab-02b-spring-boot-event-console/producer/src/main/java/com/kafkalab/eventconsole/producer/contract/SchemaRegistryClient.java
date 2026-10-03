package com.kafkalab.eventconsole.producer.contract;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;
import com.kafkalab.eventconsole.producer.config.AppProperties;

/**
 * Reads the latest version of the contract from a Confluent-compatible Schema Registry. The
 * producer never registers schemas: that is a reviewed step (CI / setup, with a compatibility
 * check), not something a running service does on its own.
 */
@Component
public class SchemaRegistryClient {

    /** The latest registered version of a subject. */
    public record Registered(int id, int version, String schemaType, String schema) {
    }

    // The registry adds fields over time (references, ruleSet, ...); ignore what we do not read.
    private static final JsonMapper JSON = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    private static final MediaType REGISTRY_JSON = MediaType.parseMediaType("application/vnd.schemaregistry.v1+json");

    private final RestClient http;
    private final String subject;

    public SchemaRegistryClient(AppProperties props) {
        AppProperties.Schema cfg = props.schema();
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofMillis(cfg.connectTimeoutMs())).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(Duration.ofMillis(cfg.readTimeoutMs()));
        this.http = RestClient.builder().baseUrl(cfg.registryUrl()).requestFactory(factory).build();
        this.subject = cfg.subject();
    }

    public Registered latest() {
        Registered registered;
        try {
            String body = http.get().uri("/subjects/{subject}/versions/latest", subject)
                    .accept(REGISTRY_JSON, MediaType.APPLICATION_JSON).retrieve().body(String.class);
            registered = JSON.readValue(body, Registered.class);
        } catch (HttpClientErrorException.NotFound notFound) {
            throw new ContractUnavailableException("no contract is registered for subject '" + subject + "'");
        } catch (RestClientException | JacksonException e) {
            throw new ContractUnavailableException("Schema Registry could not answer for subject '" + subject + "': "
                    + e.getMessage(), e);
        }
        if (registered == null || registered.schema() == null) {
            throw new ContractUnavailableException("Schema Registry returned no schema for subject '" + subject + "'");
        }
        if (!"JSON".equals(registered.schemaType())) {
            throw new ContractUnavailableException("subject '" + subject + "' holds a " + registered.schemaType()
                    + " schema, not a JSON Schema");
        }
        return registered;
    }
}
