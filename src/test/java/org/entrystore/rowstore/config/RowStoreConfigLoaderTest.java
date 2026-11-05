/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.entrystore.rowstore.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.diagnostics.FailureAnalysis;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for resolving the configuration location and reporting configuration failures.
 * Specification: CFG-1.03, CFG-2.02, test specification specs/tests/13-deployment.md
 */
class RowStoreConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("TC-DEPL-004: Absolute URIs are used as they are, file paths are resolved against the working directory")
    void configLocations_areResolved() {
        assertThat(RowStoreConfigLoader.toUri("file:///etc/rowstore/rowstore.json"))
                .isEqualTo(URI.create("file:///etc/rowstore/rowstore.json"));
        assertThat(RowStoreConfigLoader.toUri("https://example.com/rowstore.json"))
                .isEqualTo(URI.create("https://example.com/rowstore.json"));
        assertThat(RowStoreConfigLoader.toUri("/etc/rowstore/rowstore.json"))
                .isEqualTo(Path.of("/etc/rowstore/rowstore.json").toUri());
        assertThat(RowStoreConfigLoader.toUri("conf/rowstore.json"))
                .isEqualTo(Path.of("").toAbsolutePath().resolve("conf/rowstore.json").toUri());
        assertThat(RowStoreConfigLoader.toUri("./my config.json"))
                .isEqualTo(Path.of("").toAbsolutePath().resolve("my config.json").toUri());
    }

    @Test
    @DisplayName("TC-DEPL-005: A missing, unreadable or invalid configuration is reported without a stack trace")
    void configFailures_areReportedAsMessage() throws Exception {
        RowStoreConfigLoader loader = new RowStoreConfigLoader();
        Path invalidJson = Files.writeString(tempDir.resolve("invalid.json"), "{\"baseurl\":");
        Path incomplete = Files.writeString(tempDir.resolve("incomplete.json"), "{\"baseurl\": \"http://localhost/\"}");

        assertThatThrownBy(() -> loader.rowStoreConfig(tempDir.resolve("missing.json").toString()))
                .isInstanceOf(RowStoreConfigException.class)
                .hasMessageContaining("Unable to read the configuration from " + tempDir.resolve("missing.json").toUri());
        assertThatThrownBy(() -> loader.rowStoreConfig(invalidJson.toString()))
                .isInstanceOf(RowStoreConfigException.class)
                .hasMessageStartingWith("rowstore.json invalid:");
        assertThatThrownBy(() -> loader.rowStoreConfig(incomplete.toUri().toString()))
                .isInstanceOf(RowStoreConfigException.class)
                .hasMessage("rowstore.json: missing required key 'database'");

        // the failure of the bean creation is reported with the message of the cause only
        RowStoreConfigException cause = new RowStoreConfigException("rowstore.json: missing required key 'database'");
        FailureAnalysis analysis = new RowStoreConfigFailureAnalyzer()
                .analyze(new BeanCreationException("rowStoreConfig", "Failed to instantiate", cause));
        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).isEqualTo("rowstore.json: missing required key 'database'");
        assertThat(analysis.getAction()).contains("Fix the configuration");
    }

}
