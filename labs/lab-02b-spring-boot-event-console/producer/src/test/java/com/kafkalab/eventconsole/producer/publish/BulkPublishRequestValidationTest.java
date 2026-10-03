package com.kafkalab.eventconsole.producer.publish;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.KeyStrategy;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.Mode;
import com.kafkalab.eventconsole.producer.publish.BulkPublishRequest.PastedEvent;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

class BulkPublishRequestValidationTest {

    private static ValidatorFactory factory;
    private static Validator validator;

    @BeforeAll
    static void start() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void stop() {
        factory.close();
    }

    private static BulkPublishRequest generate(Integer count, String keyPrefix, String valuePrefix) {
        return new BulkPublishRequest(Mode.GENERATE, count, KeyStrategy.UNIQUE, 5, keyPrefix, null, valuePrefix, null);
    }

    @Test
    void aSensibleGenerateRequestIsValid() {
        assertThat(validator.validate(generate(100, "order-", "evt"))).isEmpty();
    }

    @Test
    void countMustBeBetweenOneAndFiftyThousand() {
        assertThat(validator.validate(generate(0, "k-", "v"))).isNotEmpty();
        assertThat(validator.validate(generate(50001, "k-", "v"))).isNotEmpty();
        assertThat(validator.validate(generate(50000, "k-", "v"))).isEmpty();
    }

    @Test
    void prefixesMayNotContainCharactersThatCouldBreakTheGeneratedJson() {
        assertThat(validator.validate(generate(1, "k-", "a\"},{\"x"))).isNotEmpty();
        assertThat(validator.validate(generate(1, "has space", "v"))).isNotEmpty();
        assertThat(validator.validate(generate(1, "ok.name_1:x-2", "v"))).isEmpty();
    }

    @Test
    void modeIsRequired() {
        assertThat(validator.validate(new BulkPublishRequest(null, 1, null, null, null, null, null, null))).isNotEmpty();
    }

    @Test
    void pastedEventsNeedANonBlankBoundedValue() {
        BulkPublishRequest blank = new BulkPublishRequest(Mode.PASTE, null, null, null, null, null, null,
                List.of(new PastedEvent("k", "   ")));
        BulkPublishRequest tooLong = new BulkPublishRequest(Mode.PASTE, null, null, null, null, null, null,
                List.of(new PastedEvent("k", "x".repeat(10001))));
        BulkPublishRequest fine = new BulkPublishRequest(Mode.PASTE, null, null, null, null, null, null,
                List.of(new PastedEvent("k", "x".repeat(10000)), new PastedEvent(null, "v")));

        assertThat(validator.validate(blank)).isNotEmpty();
        assertThat(validator.validate(tooLong)).isNotEmpty();
        assertThat(validator.validate(fine)).isEmpty();
    }
}
