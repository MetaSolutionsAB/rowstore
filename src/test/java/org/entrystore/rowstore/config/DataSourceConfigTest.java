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

import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the unpooled (PGSimpleDataSource) path of {@link DataSourceConfig}. Building a
 * PGSimpleDataSource does not connect, so no database is needed. The HikariCP path connects on
 * construction and is covered by ConnectionPoolIT.
 */
@DisplayName("DataSourceConfig Tests")
class DataSourceConfigTest {

    // An empty ssl value means that the key is not set
    @ParameterizedTest(name = "ssl={0}")
    @CsvSource({"false, disable", "true, require", ", prefer"})
    @DisplayName("TC-DS-001: Unpooled query data source applies SSL mode, timeouts and read-only mode")
    void unpooledQueryDataSource_appliesDriverSettings(Boolean ssl, String expectedSslMode) throws Exception {
        JSONObject db = new JSONObject()
                .put("host", "db-replica")
                .put("database", "rowstore")
                .put("user", "reader")
                .put("password", "pw")
                .put("socketTimeout", 60)
                .put("connectTimeout", 7)
                .put("loginTimeout", 8);
        if (ssl != null) {
            db.put("ssl", ssl);
        }
        JSONObject json = new JSONObject()
                .put("baseurl", "http://localhost/")
                .put("database", new JSONObject(db.toString()).put("host", "db-primary"))
                .put("queryDatabase", db);
        RowStoreConfig config = new RowStoreConfig(json);

        DataSource ds = new DataSourceConfig().queryDataSource(config, new PGSimpleDataSource());

        assertThat(ds).isInstanceOf(PGSimpleDataSource.class);
        PGSimpleDataSource pgDs = (PGSimpleDataSource) ds;
        assertThat(pgDs.getServerNames()).containsExactly("db-replica");
        assertThat(pgDs.getSsl()).isEqualTo(Boolean.TRUE.equals(ssl));
        assertThat(pgDs.getSslMode()).isEqualTo(expectedSslMode);
        assertThat(pgDs.getSocketTimeout()).isEqualTo(60);
        assertThat(pgDs.getConnectTimeout()).isEqualTo(7);
        assertThat(pgDs.getLoginTimeout()).isEqualTo(8);
        assertThat(pgDs.getReadOnly()).isTrue();
        assertThat(pgDs.getReadOnlyMode()).isEqualTo("always");
    }

}
