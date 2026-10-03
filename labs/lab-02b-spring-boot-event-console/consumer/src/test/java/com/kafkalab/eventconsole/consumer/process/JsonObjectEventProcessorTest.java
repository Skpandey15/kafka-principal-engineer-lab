package com.kafkalab.eventconsole.consumer.process;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JsonObjectEventProcessorTest {

    private final EventProcessor processor = new JsonObjectEventProcessor();

    @ParameterizedTest
    @ValueSource(strings = {
            "{}",
            "{\"id\":\"evt-1\",\"seq\":1}",
            "  {\"nested\":{\"a\":[1,2,3]},\"unicode\":\"é\"}  ",
    })
    void aJsonObjectIsAccepted(String value) {
        assertThatCode(() -> processor.process("k", value)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "this is not json",
            "{\"truncated\":",
            "{'single':'quotes'}",
            "order-1|{\"status\":\"CREATED\"}",
    })
    void textThatIsNotJsonIsRejectedWithAReason(String value) {
        assertThatThrownBy(() -> processor.process("k", value))
                .isInstanceOf(EventProcessingException.class)
                .hasMessageStartingWith("value is not valid JSON");
    }

    @ParameterizedTest
    @ValueSource(strings = { "[1,2,3]", "42", "\"just a string\"", "true", "null" })
    void validJsonThatIsNotAnObjectIsRejected(String value) {
        assertThatThrownBy(() -> processor.process("k", value))
                .isInstanceOf(EventProcessingException.class)
                .hasMessageStartingWith("value must be a JSON object");
    }

    @Test
    void anEmptyOrMissingValueIsRejected() {
        // A null value is a Kafka tombstone; this consumer has nothing to store for it.
        assertThatThrownBy(() -> processor.process("k", null)).isInstanceOf(EventProcessingException.class)
                .hasMessage("value is empty");
        assertThatThrownBy(() -> processor.process("k", "   ")).isInstanceOf(EventProcessingException.class)
                .hasMessage("value is empty");
    }

    @Test
    void theKeyDoesNotMatter() {
        assertThatCode(() -> processor.process(null, "{}")).doesNotThrowAnyException();
    }
}
