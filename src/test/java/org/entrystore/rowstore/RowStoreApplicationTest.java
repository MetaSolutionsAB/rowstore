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

package org.entrystore.rowstore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the command line check of {@link RowStoreApplication}.
 * Specification: CFG-7.05, test specification specs/tests/13-deployment.md
 */
class RowStoreApplicationTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = ';', value = {
            "-c;                          --rowstore.config.uri=<URI>",
            "--config=file:///x.json;     --rowstore.config.uri=<URI>",
            "--config;                    --rowstore.config.uri=<URI>",
            "-p;                          --server.port=<PORT>",
            "--port=9000;                 --server.port=<PORT>",
            "-l;                          loglevel",
            "--log-level=DEBUG;           loglevel",
            "--connector-params=a=b;      maxQueryConnections",
            "-h;                          README",
    })
    @DisplayName("TC-DEPL-001: Options of 1.x are rejected with their replacement")
    void removedOptions_areRejectedWithReplacement(String arg, String replacement) {
        assertThat(RowStoreApplication.checkArguments(new String[]{"--server.port=8282", arg}))
                .contains("'" + arg + "'")
                .contains("options of RowStore 1.x were removed")
                .contains(replacement);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(value = {"9000", "file:///etc/rowstore.json", "-x", "--", "rowstore.json"})
    @DisplayName("TC-DEPL-002: Arguments that are not of the form --name=value are rejected")
    void nonOptionArguments_areRejected(String arg) {
        assertThat(RowStoreApplication.checkArguments(new String[]{arg}))
                .contains("'" + arg + "'")
                .contains("only takes options of the form --name=value");
    }

    @Test
    @DisplayName("TC-DEPL-003: Spring Boot options are accepted")
    void springBootOptions_areAccepted() {
        assertThat(RowStoreApplication.checkArguments(new String[0])).isNull();
        assertThat(RowStoreApplication.checkArguments(new String[]{
                "--rowstore.config.uri=file:///etc/rowstore.json", "--server.port=9000", "--debug",
                "--server.servlet.context-path=/rowstore"})).isNull();
    }

}
