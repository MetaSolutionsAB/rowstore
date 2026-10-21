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

package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the session statements of the connection budgets (lock timeout of query connections, ARCH-7.03, CFG-4.08).
 * Specification: specs/tests/12-connection-limits.md
 */
class PgRowStoreSessionSqlTest {

    @Test
    @DisplayName("TC-CONN-027: Query connections get a lock timeout of half the wait timeout by default, reset for pooled export and load connections")
    void defaultLockTimeout() {
        RowStoreConfig.Database unpooled = database(new JSONObject().put("connectionWaitTimeout", 30));
        assertThat(PgRowStore.sessionSql(unpooled, true)).isEqualTo("SET lock_timeout = 15000");
        // a direct connection is a new session, nothing to reset
        assertThat(PgRowStore.sessionSql(unpooled, false)).isNull();

        RowStoreConfig.Database pooled = database(new JSONObject().put("connectionPoolMax", 20).put("queryLockTimeout", 5));
        assertThat(PgRowStore.sessionSql(pooled, true)).isEqualTo("SET lock_timeout = 5000");
        assertThat(PgRowStore.sessionSql(pooled, false)).isEqualTo("RESET lock_timeout");
    }

    @Test
    @DisplayName("TC-CONN-027: queryLockTimeout 0 changes no session state on any connection")
    void lockTimeoutDisabled() {
        for (JSONObject db : new JSONObject[]{new JSONObject(), new JSONObject().put("connectionPoolMax", 20)}) {
            RowStoreConfig.Database database = database(db.put("queryLockTimeout", 0));
            assertThat(database.getQueryLockTimeoutMillis()).isZero();
            assertThat(PgRowStore.sessionSql(database, true)).isNull();
            assertThat(PgRowStore.sessionSql(database, false)).isNull();
        }
    }

    private static RowStoreConfig.Database database(JSONObject settings) {
        JSONObject db = new JSONObject(settings.toString())
                .put("host", "localhost")
                .put("database", "rowstore")
                .put("user", "rowstore")
                .put("password", "secret");
        return new RowStoreConfig(new JSONObject().put("baseurl", "http://localhost/").put("database", db)).getDatabase();
    }

}
