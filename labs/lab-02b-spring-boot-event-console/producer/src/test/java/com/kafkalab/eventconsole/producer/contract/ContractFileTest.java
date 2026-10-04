package com.kafkalab.eventconsole.producer.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import com.kafkalab.eventconsole.producer.testsupport.FakeSchemaRegistry;

/**
 * The contract registered in production is a file in this repository. The tests in this project use
 * a copy of it (FakeSchemaRegistry.V1); this keeps the copy honest, so a test can never pass against
 * a schema that is not the one being deployed.
 *
 * <p>It finds the file by walking up from the project directory, so it works wherever the project sits
 * inside the repository. A copy of the project built outside the repository (a bare export) has no
 * contract file to compare with, and the test says so instead of failing.
 */
class ContractFileTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static Path findContract() {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            Path candidate = dir.resolve("platform-k8s/event-console/contracts/event-v1.json");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    @Test
    void theTestCopyOfTheV1ContractMatchesTheFileThatGetsRegistered() throws Exception {
        Path file = findContract();
        assumeTrue(file != null, "not inside the repository: no contract file to compare with");

        JsonNode deployed = JSON.readTree(Files.readString(file));
        JsonNode inTests = JSON.readTree(FakeSchemaRegistry.V1);

        // title/description are documentation; everything that decides what is valid must be equal.
        for (String keyword : new String[] { "$schema", "type", "required", "additionalProperties", "properties" }) {
            assertThat(inTests.get(keyword)).as(keyword).isEqualTo(deployed.get(keyword));
        }
    }
}
