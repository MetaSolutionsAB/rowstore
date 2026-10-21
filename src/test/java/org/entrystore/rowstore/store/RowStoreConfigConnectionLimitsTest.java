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

package org.entrystore.rowstore.store;

import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the parsing of the connection limits of the database blocks (CFG-4.05 to CFG-4.07).
 * Specification: specs/tests/12-connection-limits.md
 */
class RowStoreConfigConnectionLimitsTest {

    @Test
    @DisplayName("TC-CONN-008: Connection limits default to 10 query and 2 export connections and a wait timeout of 30 s")
    void defaults() {
        RowStoreConfig config = new RowStoreConfig(config(database()));

        assertThat(config.getDatabase().getMaxQueryConnections()).isEqualTo(10);
        assertThat(config.getDatabase().getMaxExportConnections()).isEqualTo(2);
        assertThat(config.getDatabase().getConnectionWaitTimeout()).isEqualTo(30);
    }

    @Test
    @DisplayName("TC-CONN-008: Connection limits are read per database block, the wait timeout defaults to querytimeout")
    void configuredValues_perDatabaseBlock() {
        RowStoreConfig config = new RowStoreConfig(config(database()
                .put("maxQueryConnections", 4)
                .put("maxExportConnections", 1)
                .put("connectionWaitTimeout", 7))
                .put("querytimeout", 12)
                .put("queryDatabase", database().put("maxQueryConnections", 20)));

        assertThat(config.getDatabase().getMaxQueryConnections()).isEqualTo(4);
        assertThat(config.getDatabase().getMaxExportConnections()).isEqualTo(1);
        assertThat(config.getDatabase().getConnectionWaitTimeout()).isEqualTo(7);
        assertThat(config.getQueryDatabase().getMaxQueryConnections()).isEqualTo(20);
        assertThat(config.getQueryDatabase().getMaxExportConnections()).isEqualTo(2);
        assertThat(config.getQueryDatabase().getConnectionWaitTimeout()).isEqualTo(12);
    }

    @ParameterizedTest
    @CsvSource({
            "database, maxQueryConnections, 0",
            "database, maxExportConnections, -1",
            "database, connectionWaitTimeout, 0",
            "queryDatabase, maxQueryConnections, -5",
            "queryDatabase, connectionWaitTimeout, abc"
    })
    @DisplayName("TC-CONN-009: Connection limits that are not greater than 0 are rejected at startup")
    void nonPositiveValues_areRejected(String block, String key, String value) {
        JSONObject json = config(database()).put("queryDatabase", database());
        json.getJSONObject(block).put(key, value);

        assertThatThrownBy(() -> new RowStoreConfig(json))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("rowstore.json: '" + block + "." + key + "' must be a number greater than 0");
    }

    private static JSONObject config(JSONObject database) {
        return new JSONObject().put("baseurl", "http://localhost/").put("database", database);
    }

    private static JSONObject database() {
        return new JSONObject()
                .put("host", "localhost")
                .put("database", "rowstore")
                .put("user", "rowstore")
                .put("password", "secret");
    }

}
