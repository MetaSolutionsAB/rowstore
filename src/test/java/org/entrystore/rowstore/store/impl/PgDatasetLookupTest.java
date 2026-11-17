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

import org.entrystore.rowstore.etl.EtlProcessor;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that looking up a dataset distinguishes client errors (unknown or malformed IDs, 404, logged at DEBUG level
 * only) from database failures, which propagate (503 or 500) instead of being mistaken for an unknown dataset
 * (ROWS-169, API-6.05, API-6.06).
 */
class PgDatasetLookupTest {

    @Test
    @DisplayName("TC-ERROR-023: A database outage while looking up a dataset propagates instead of meaning an unknown dataset")
    void databaseOutage_propagates() {
        DataAccessException outage = new DataAccessResourceFailureException("Simulated database outage");

        // while resolving the alias (every lookup starts with it)
        assertThatThrownBy(() -> new PgDataset(rowStore(new FakeJdbcTemplate(outage, null)), UUID.randomUUID().toString()))
                .isSameAs(outage);
        // while loading the dataset
        assertThatThrownBy(() -> new PgDataset(rowStore(new FakeJdbcTemplate(null, outage)), UUID.randomUUID().toString()))
                .isSameAs(outage);
    }

    @Test
    @DisplayName("TC-ERROR-023: An alias rejected by the database is an unknown dataset, other database errors propagate")
    void rejectedAlias_isUnknownDataset() {
        // e.g., an invalid byte sequence (22021)
        DataAccessException rejected = new UncategorizedSQLException("alias", "SELECT", new SQLException("invalid byte sequence", "22021"));
        assertThatThrownBy(() -> new PgDataset(rowStore(new FakeJdbcTemplate(rejected, null)), "not-a-uuid"))
                .isInstanceOf(IllegalArgumentException.class);

        // an unknown UUID
        DataAccessException unknown = new EmptyResultDataAccessException(1);
        assertThatThrownBy(() -> new PgDataset(rowStore(new FakeJdbcTemplate(null, unknown)), UUID.randomUUID().toString()))
                .isInstanceOf(IllegalStateException.class);

        // e.g., missing privileges (42501): not a client error
        DataAccessException denied = new UncategorizedSQLException("dataset", "SELECT", new SQLException("permission denied", "42501"));
        assertThatThrownBy(() -> new PgDataset(rowStore(new FakeJdbcTemplate(null, denied)), UUID.randomUUID().toString()))
                .isSameAs(denied);
    }

    @Test
    @DisplayName("TC-ERROR-024: An invalid data table name in the datasets table is a server fault, not an unknown dataset")
    void invalidDataTableName_isServerFault() {
        String id = UUID.randomUUID().toString();
        Map<String, Object> corrupt = Map.of("id", UUID.fromString(id), "status", 3,
                "created", new Timestamp(System.currentTimeMillis()), "data_table", "data_x; DROP TABLE datasets");
        assertThatThrownBy(() -> new PgDataset(rowStore(new FakeJdbcTemplate(null, null, corrupt)), id))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining(id);
    }

    private static RowStore rowStore(JdbcTemplate jdbcTemplate) {
        RowStoreConfig config = new RowStoreConfig(new JSONObject()
                .put("baseurl", "http://localhost/")
                .put("database", new JSONObject()
                        .put("host", "localhost")
                        .put("database", "unused")
                        .put("user", "unused")
                        .put("password", "unused")));
        return new RowStore() {
            @Override public Connection getConnection() { throw new UnsupportedOperationException(); }
            @Override public Connection getQueryConnection() { throw new UnsupportedOperationException(); }
            @Override public JdbcTemplate getJdbcTemplate() { return jdbcTemplate; }
            @Override public JdbcTemplate getQueryJdbcTemplate() { return jdbcTemplate; }
            @Override public Datasets getDatasets() { throw new UnsupportedOperationException(); }
            @Override public RowStoreConfig getConfig() { return config; }
            @Override public EtlProcessor getEtlProcessor() { throw new UnsupportedOperationException(); }
            @Override public void shutdown() { }
        };
    }

    /**
     * Resolves no aliases and finds no datasets (or the given dataset row), or fails the alias resolution or the
     * loading of the dataset with the given exceptions.
     */
    private static final class FakeJdbcTemplate extends JdbcTemplate {
        private final DataAccessException aliasFailure;
        private final DataAccessException datasetFailure;
        private final Map<String, Object> datasetRow;

        FakeJdbcTemplate(DataAccessException aliasFailure, DataAccessException datasetFailure) {
            this(aliasFailure, datasetFailure, null);
        }

        FakeJdbcTemplate(DataAccessException aliasFailure, DataAccessException datasetFailure, Map<String, Object> datasetRow) {
            this.aliasFailure = aliasFailure;
            this.datasetFailure = datasetFailure;
            this.datasetRow = datasetRow;
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
            if (aliasFailure != null) {
                throw aliasFailure;
            }
            return List.of();
        }

        @Override
        public Map<String, Object> queryForMap(String sql, Object... args) {
            if (datasetFailure != null) {
                throw datasetFailure;
            }
            if (datasetRow != null) {
                return datasetRow;
            }
            throw new EmptyResultDataAccessException(1);
        }
    }

}
