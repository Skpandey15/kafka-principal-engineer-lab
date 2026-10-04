package com.kafkalab.eventconsole.consumer.contract;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException;

/**
 * Reads schemas from a Confluent-compatible Schema Registry. The consumer only ever reads by id:
 * registering schemas is a deliberate, reviewed step (CI / setup), never something a running
 * service does on its own.
 */
@Component
public class SchemaRegistryClient {

    // The registry adds fields over time (references, ruleSet, ...); ignore what we do not read.
    private static final JsonMapper JSON = JsonMapper.builder()
            .disable(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    private static final MediaType REGISTRY_JSON = MediaType.parseMediaType("application/vnd.schemaregistry.v1+json");

    private record SchemaResponse(String schemaType, String schema) {
    }

    private final RestClient http;

    public SchemaRegistryClient(AppProperties props) {
        AppProperties.Schema cfg = props.schema();
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofMillis(cfg.connectTimeoutMs())).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(Duration.ofMillis(cfg.readTimeoutMs()));
        this.http = RestClient.builder().baseUrl(cfg.registryUrl()).requestFactory(factory).build();
    }

    /**
     * The JSON Schema text registered under {@code id}.
     *
     * @throws EventProcessingException           the id is not registered (or is not a JSON Schema): the EVENT is wrong
     * @throws InfrastructureUnavailableException the registry cannot answer: the event is not at fault
     */
    public String jsonSchemaById(int id) {
        SchemaResponse response;
        try {
            String body = http.get().uri("/schemas/ids/{id}", id).accept(REGISTRY_JSON, MediaType.APPLICATION_JSON)
                    .retrieve().body(String.class);
            response = JSON.readValue(body, SchemaResponse.class);
        } catch (org.springframework.web.client.HttpClientErrorException.NotFound notFound) {
            throw new EventProcessingException("schema id " + id + " is not registered");
        } catch (RestClientException | JacksonException e) {
            throw new InfrastructureUnavailableException("Schema Registry could not answer for schema id " + id + ": "
                    + e.getMessage(), e);
        }
        if (response == null || response.schema() == null) {
            throw new InfrastructureUnavailableException("Schema Registry returned no schema for id " + id);
        }
        if (!"JSON".equals(response.schemaType())) {
            throw new EventProcessingException("schema id " + id + " is a " + response.schemaType()
                    + " schema, not a JSON Schema");
        }
        return response.schema();
    }
}
