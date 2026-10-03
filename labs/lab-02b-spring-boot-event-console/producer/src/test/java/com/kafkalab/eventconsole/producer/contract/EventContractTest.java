package com.kafkalab.eventconsole.producer.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.kafkalab.eventconsole.producer.config.AppProperties;
import com.kafkalab.eventconsole.producer.testsupport.FakeSchemaRegistry;

/** Contract enforcement at the edge, against a registry spoken to over real HTTP. */
class EventContractTest {

    /** A clock the test moves by hand, so the refresh interval can be crossed without sleeping. */
    static final class MovableClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-03T10:00:00Z"));

        void advance(Duration d) {
            now.updateAndGet(i -> i.plus(d));
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }

    private FakeSchemaRegistry registry;
    private MovableClock clock;
    private EventContract contract;

    @BeforeEach
    void start() {
        registry = new FakeSchemaRegistry(7, 1, FakeSchemaRegistry.V1);
        clock = new MovableClock();
        AppProperties props = new AppProperties("orders", 3, 1, 1, 1000,
                new AppProperties.Schema(registry.url(), "event-console-value", 2_000, 5_000, 60_000));
        contract = new EventContract(new SchemaRegistryClient(props), props, clock);
    }

    @AfterEach
    void stop() {
        registry.close();
    }

    private void require(String... values) {
        List<String> keys = new ArrayList<>(Collections.nCopies(values.length, "k"));
        contract.requireAllValid(contract.current(), keys, List.of(values));
    }

    @Test
    void validEventsPass() {
        assertThatCode(() -> require("{\"id\":\"a\"}", "{\"id\":\"b\",\"seq\":2,\"batch\":\"x\",\"ts\":\"t\",\"status\":\"NEW\"}"))
                .doesNotThrowAnyException();
    }

    @Test
    void thePropertiesAreClosedSoAMisspeltFieldIsRejectedAtTheEdge() {
        assertThatThrownBy(() -> require("{\"id\":\"a\",\"statsu\":\"PAID\"}")).isInstanceOfSatisfying(
                ContractViolationException.class, e -> assertThat(e.violations().getFirst().reason()).contains("statsu"));
    }

    @Test
    void theContractInForceIsTheLatestRegisteredVersion() {
        assertThat(contract.current().id()).isEqualTo(7);
        assertThat(contract.current().version()).isEqualTo(1);
    }

    @Test
    void oneBadEventRejectsTheWholeRequestAndSaysWhichOneAndWhy() {
        assertThatThrownBy(() -> require("{\"id\":\"ok\"}", "{\"seq\":-1}", "not json", "{\"id\":\"ok2\"}"))
                .isInstanceOfSatisfying(ContractViolationException.class, e -> {
                    assertThat(e.total()).isEqualTo(2);
                    assertThat(e.schemaId()).isEqualTo(7);
                    assertThat(e.violations()).extracting(ContractViolationException.Violation::index).containsExactly(1, 2);
                    assertThat(e.violations().get(0).reason()).contains("seq").contains("id");
                    assertThat(e.violations().get(1).reason()).startsWith("value is not valid JSON");
                });
    }

    @Test
    void anEmptyValueAndAWrongShapeAreViolations() {
        assertThatThrownBy(() -> require("  ", "[1]")).isInstanceOfSatisfying(ContractViolationException.class,
                e -> assertThat(e.total()).isEqualTo(2));
    }

    @Test
    void aHugeNumberOfViolationsIsCountedButOnlyTheFirstFewAreReported() {
        String[] bad = new String[500];
        java.util.Arrays.fill(bad, "oops");
        assertThatThrownBy(() -> require(bad)).isInstanceOfSatisfying(ContractViolationException.class, e -> {
            assertThat(e.total()).isEqualTo(500);
            assertThat(e.violations()).hasSize(20);
        });
    }

    @Test
    void theLatestVersionIsReusedWithinTheTtlAndRefreshedAfterIt() {
        contract.current();
        contract.current();
        assertThat(registry.requestCount()).isEqualTo(1);

        registry.registerLatest(8, 2, FakeSchemaRegistry.V1);
        clock.advance(Duration.ofSeconds(30));
        assertThat(contract.current().id()).as("still inside the TTL").isEqualTo(7);

        clock.advance(Duration.ofSeconds(31));
        assertThat(contract.current().id()).as("TTL passed: the new version is picked up").isEqualTo(8);
        assertThat(contract.current().version()).isEqualTo(2);
    }

    @Test
    void aRegistryOutageAfterTheContractWasKnownKeepsEnforcingTheLastVersion() {
        contract.current();
        registry.setDown(true);
        clock.advance(Duration.ofMinutes(5));

        assertThat(contract.current().id()).isEqualTo(7);
        assertThatThrownBy(() -> require("{\"seq\":1}")).isInstanceOf(ContractViolationException.class);
    }

    @Test
    void duringAnOutageTheRegistryIsNotHammeredOnEveryRequest() {
        contract.current();
        int before = registry.requestCount();
        registry.setDown(true);
        clock.advance(Duration.ofMinutes(5));

        for (int i = 0; i < 50; i++) {
            contract.current();
        }
        assertThat(registry.requestCount() - before).as("one attempt, then back off").isEqualTo(1);

        clock.advance(Duration.ofSeconds(6));
        contract.current();
        assertThat(registry.requestCount() - before).isEqualTo(2);
    }

    @Test
    void aProducerThatHasNeverObtainedTheContractRefusesToPublishUnchecked() {
        registry.setDown(true);
        assertThatThrownBy(() -> contract.current()).isInstanceOf(ContractUnavailableException.class);
        assertThat(contract.currentOrNull()).isNull();
    }

    @Test
    void aSubjectWithNoRegisteredVersionIsUnavailableNotEmptyOrPermissive() {
        registry.setSubjectExists(false);
        assertThatThrownBy(() -> contract.current()).isInstanceOf(ContractUnavailableException.class)
                .hasMessageContaining("no contract is registered");
    }

    @Test
    void aSubjectHoldingANonJsonSchemaIsRejected() {
        registry.setSchemaType("AVRO");
        assertThatThrownBy(() -> contract.current()).isInstanceOf(ContractUnavailableException.class)
                .hasMessageContaining("AVRO");
    }

    @Test
    void aSchemaThatCannotBeCompiledIsUnavailable() {
        registry.registerLatest(9, 3, "{\"type\":\"object\",\"properties\":{\"id\":{\"pattern\":\"([\"}}}");
        assertThatThrownBy(() -> contract.current()).isInstanceOf(ContractUnavailableException.class)
                .hasMessageContaining("cannot be compiled");
    }
}
