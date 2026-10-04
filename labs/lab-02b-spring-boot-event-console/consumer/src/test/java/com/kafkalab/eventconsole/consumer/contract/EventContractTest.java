package com.kafkalab.eventconsole.consumer.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.kafkalab.eventconsole.consumer.config.AppProperties;
import com.kafkalab.eventconsole.consumer.process.ConsumedEvent;
import com.kafkalab.eventconsole.consumer.process.EventProcessingException;
import com.kafkalab.eventconsole.consumer.process.InfrastructureUnavailableException;
import com.kafkalab.eventconsole.consumer.testsupport.FakeSchemaRegistry;
import com.sun.net.httpserver.HttpServer;

/** The contract check against a registry spoken to over real HTTP. */
class EventContractTest {

    private FakeSchemaRegistry registry;
    private ContractEventProcessor processor;
    private EventContract contract;

    @BeforeEach
    void start() {
        registry = new FakeSchemaRegistry().withJsonSchema(1, FakeSchemaRegistry.V1);
        contract = contractFor(registry.url());
        processor = new ContractEventProcessor(contract, new LegacyHistory(propsFor(registry.url(), java.util.Map.of())), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    @AfterEach
    void stop() {
        registry.close();
    }

    private static AppProperties propsFor(String url, java.util.Map<Integer, Long> legacyUntilOffsets) {
        AppProperties.Retry retry = new AppProperties.Retry(true, 5, 5_000, 300_000, 2_000, 50, 60_000, 15_000);
        return new AppProperties("orders", 3, 1, 1, 500, 5_000, Duration.ofDays(7), retry,
                new AppProperties.Schema(url, 2_000, 5_000, legacyUntilOffsets));
    }

    private static EventContract contractFor(String url) {
        return new EventContract(new SchemaRegistryClient(propsFor(url, java.util.Map.of())));
    }

    private void process(String value, String schemaId) {
        processor.process(new ConsumedEvent("k", value, schemaId, 0, 100));
    }

    // --- replaying history from before the contract ------------------------------------------------

    private ContractEventProcessor withLegacyBelow(java.util.Map<Integer, Long> offsets,
            io.micrometer.core.instrument.MeterRegistry metrics) {
        return new ContractEventProcessor(contract, new LegacyHistory(propsFor(registry.url(), offsets)), metrics);
    }

    @Test
    void aHeaderlessRecordBelowTheCutoverIsLegacyHistoryAndIsAcceptedAsItIs() {
        var metrics = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        ContractEventProcessor p = withLegacyBelow(java.util.Map.of(0, 100L), metrics);

        // Anything at all: free text, an empty value, a JSON array -- it predates the contract.
        assertThatCode(() -> p.process(new ConsumedEvent(null, "random text from before schemas", null, 0, 99)))
                .doesNotThrowAnyException();
        assertThatCode(() -> p.process(new ConsumedEvent("k", null, null, 0, 0))).doesNotThrowAnyException();
        assertThat(metrics.counter("eventconsole.consume.legacy").count()).isEqualTo(2);
    }

    @Test
    void theCutoverIsExclusiveSoTheFirstRecordAtItNeedsAHeader() {
        ContractEventProcessor p = withLegacyBelow(java.util.Map.of(0, 100L), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

        assertThatThrownBy(() -> p.process(new ConsumedEvent("k", "{\"id\":\"x\"}", null, 0, 100)))
                .isInstanceOf(EventProcessingException.class).hasMessageContaining("no x-schema-id header");
    }

    @Test
    void theCutoverIsPerPartition() {
        ContractEventProcessor p = withLegacyBelow(java.util.Map.of(0, 100L), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

        assertThatCode(() -> p.process(new ConsumedEvent("k", "old", null, 0, 5))).doesNotThrowAnyException();
        // Partition 1 has no cut-over configured: nothing there is legacy.
        assertThatThrownBy(() -> p.process(new ConsumedEvent("k", "old", null, 1, 5))).isInstanceOf(EventProcessingException.class);
    }

    @Test
    void aRecordThatDoesCarryAHeaderIsHeldToItEvenInsideTheLegacyRange() {
        ContractEventProcessor p = withLegacyBelow(java.util.Map.of(0, 100L), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

        assertThatThrownBy(() -> p.process(new ConsumedEvent("k", "{\"seq\":-1}", "1", 0, 5)))
                .isInstanceOf(EventProcessingException.class).hasMessageStartingWith("violates schema 1:");
    }

    @Test
    void withNoCutoverConfiguredNothingIsLegacyAndTheContractCoversTheWholeTopic() {
        assertThatThrownBy(() -> process("old record", null)).isInstanceOf(EventProcessingException.class)
                .hasMessageContaining("no x-schema-id header");
    }

    @Test
    void anEventThatFollowsItsContractIsAccepted() {
        assertThatCode(() -> process("{\"id\":\"evt-1\",\"seq\":3,\"batch\":\"b\",\"ts\":\"2026-10-03T10:00:00Z\"}", "1"))
                .doesNotThrowAnyException();
    }

    @Test
    void thePropertiesAreClosedSoAMisspeltFieldIsRejectedInsteadOfSilentlyLost() {
        assertThatThrownBy(() -> process("{\"id\":\"evt-1\",\"statsu\":\"PAID\"}", "1"))
                .isInstanceOf(EventProcessingException.class)
                .hasMessageStartingWith("violates schema 1:")
                .hasMessageContaining("statsu");
    }

    @Test
    void aPropertyAddedByALaterCompatibleVersionIsValidUnderThatVersionAndOnlyThere() {
        registry.withJsonSchema(2, """
                {"$schema":"http://json-schema.org/draft-07/schema#","type":"object","required":["id"],"additionalProperties":false,
                 "properties":{"id":{"type":"string"},"customerId":{"type":"string"}}}
                """);
        assertThatCode(() -> process("{\"id\":\"evt-1\",\"customerId\":\"c-9\"}", "2")).doesNotThrowAnyException();
        // The same event declared under v1 is wrong: each event is held to the version it names.
        assertThatThrownBy(() -> process("{\"id\":\"evt-1\",\"customerId\":\"c-9\"}", "1"))
                .isInstanceOf(EventProcessingException.class);
    }

    @Test
    void aViolationIsReportedWithWhatIsWrongNotJustThatSomethingIs() {
        assertThatThrownBy(() -> process("{\"seq\":-5,\"batch\":42}", "1"))
                .isInstanceOf(EventProcessingException.class)
                .hasMessageStartingWith("violates schema 1:")
                .hasMessageContaining("id")
                .hasMessageContaining("seq")
                .hasMessageContaining("batch");
    }

    @Test
    void onlyTheFirstFewViolationsAreReportedAndTheRestAreCounted() {
        String broken = "{\"id\":7,\"seq\":\"x\",\"batch\":1,\"ts\":2,\"status\":3}";
        assertThatThrownBy(() -> process(broken, "1")).isInstanceOf(EventProcessingException.class)
                .hasMessageContaining("(+2 more)");
    }

    @Test
    void textThatIsNotJsonFailsTheEventNotTheService() {
        assertThatThrownBy(() -> process("not json at all", "1")).isInstanceOf(EventProcessingException.class)
                .hasMessageStartingWith("value is not valid JSON");
    }

    @Test
    void validJsonOfTheWrongShapeViolatesTheContract() {
        assertThatThrownBy(() -> process("[1,2,3]", "1")).isInstanceOf(EventProcessingException.class)
                .hasMessageStartingWith("violates schema 1:");
    }

    @Test
    void anEventWithNoSchemaIdDeclaresNoContractAndIsRejected() {
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", null)).isInstanceOf(EventProcessingException.class)
                .hasMessageContaining("no x-schema-id header");
        assertThat(registry.requestCount()).as("nothing to look up").isZero();
    }

    @Test
    void aSchemaIdThatIsNotANumberIsRejected() {
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "latest")).isInstanceOf(EventProcessingException.class)
                .hasMessageContaining("not a schema id");
    }

    @Test
    void anEmptyValueIsRejectedBeforeAnythingElse() {
        assertThatThrownBy(() -> process(null, "1")).isInstanceOf(EventProcessingException.class).hasMessage("value is empty");
        assertThatThrownBy(() -> process("  ", "1")).isInstanceOf(EventProcessingException.class).hasMessage("value is empty");
    }

    @Test
    void aSchemaIdThatIsNotRegisteredIsTheEventsFaultNotAnOutage() {
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "99")).isInstanceOf(EventProcessingException.class)
                .hasMessage("schema id 99 is not registered");
    }

    @Test
    void aSchemaThatIsNotAJsonSchemaIsRejected() {
        registry.withSchemaOfType(5, "AVRO", "{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}");
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "5")).isInstanceOf(EventProcessingException.class)
                .hasMessageContaining("AVRO schema, not a JSON Schema");
    }

    @Test
    void schemasAreFetchedOnceBecauseARegistryIdIsImmutable() {
        for (int i = 0; i < 25; i++) {
            process("{\"id\":\"evt-" + i + "\"}", "1");
        }
        assertThat(registry.requestCount()).isEqualTo(1);
    }

    @Test
    void aRegistryOutageIsNotTheEventsFaultAndIsNeverCached() {
        registry.setDown(true);
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "1")).isInstanceOf(InfrastructureUnavailableException.class);

        registry.setDown(false);
        assertThatCode(() -> process("{\"id\":\"x\"}", "1")).doesNotThrowAnyException();
    }

    @Test
    void schemasAlreadySeenKeepWorkingWhileTheRegistryIsDown() {
        process("{\"id\":\"warm-up\"}", "1");
        registry.setDown(true);

        assertThatCode(() -> process("{\"id\":\"still fine\"}", "1")).doesNotThrowAnyException();
        // ...but a schema it has never seen cannot be checked, and says so as an outage.
        registry.withJsonSchema(2, FakeSchemaRegistry.V1);
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "2")).isInstanceOf(InfrastructureUnavailableException.class);
    }

    @Test
    void anUnreachableRegistryIsAnOutage() {
        registry.close();
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "1")).isInstanceOf(InfrastructureUnavailableException.class);
    }

    @Test
    void aSchemaThatCannotBeCompiledIsAProblemWithTheContractNotWithTheEvent() {
        registry.withJsonSchema(7, "{\"type\":\"object\",\"properties\":{\"id\":{\"pattern\":\"([\"}}}");
        assertThatThrownBy(() -> process("{\"id\":\"x\"}", "7")).isInstanceOf(InfrastructureUnavailableException.class)
                .hasMessageContaining("cannot be compiled");
    }

    @Test
    void aRemoteReferenceInASchemaIsNeverFetched() throws Exception {
        // The registry is the single source of truth: a schema must not be able to make the
        // consumer call arbitrary URLs.
        AtomicInteger hits = new AtomicInteger();
        HttpServer trap = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        trap.createContext("/", exchange -> {
            hits.incrementAndGet();
            byte[] body = "{\"type\":\"string\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        trap.start();
        try {
            String remote = "http://127.0.0.1:" + trap.getAddress().getPort() + "/other.json";
            registry.withJsonSchema(8, "{\"type\":\"object\",\"properties\":{\"id\":{\"$ref\":\"" + remote + "\"}}}");

            try {
                process("{\"id\":\"x\"}", "8");
            } catch (RuntimeException expectedEitherWay) {
                // Whether the reference is reported as unresolvable or ignored is not the point.
            }
            assertThat(hits.get()).isZero();
        } finally {
            trap.stop(0);
        }
    }
}
