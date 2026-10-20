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
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.lang.reflect.Proxy;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the status handling of {@link PgDataset#populate(File, boolean)} when the load fails with a non-SQL
 * exception and the database is unreachable for reads and (temporarily) for writes.
 */
class PgDatasetPopulateTest {

    @TempDir
    Path tempDir;

    private final FakeJdbcTemplate jdbcTemplate = new FakeJdbcTemplate();

    @Test
    @DisplayName("TC-ETL-007: A non-SQL failure sets the status to error without reading it back and propagates")
    void nonSqlFailure_setsErrorWithoutReadingStatusAndPropagates() {
        PgDataset dataset = newDataset();
        File missingFile = tempDir.resolve("missing.csv").toFile();

        assertThatThrownBy(() -> dataset.populate(missingFile, true)).isInstanceOf(NoSuchFileException.class);

        assertThat(jdbcTemplate.statusWrites).containsExactly(EtlStatus.PROCESSING, EtlStatus.ERROR);
        assertThat(jdbcTemplate.statusReads.get()).isZero();
    }

    @Test
    @DisplayName("TC-ETL-008: A failed error status write is retried until it succeeds")
    void failedErrorStatusWrite_isRetried() {
        PgDataset dataset = newDataset();
        dataset.setStatusRetryDelays(1, 1, 1, 1);
        jdbcTemplate.failingErrorWrites.set(2);

        assertThatThrownBy(() -> dataset.populate(tempDir.resolve("missing.csv").toFile(), true))
                .isInstanceOf(NoSuchFileException.class);

        assertThat(jdbcTemplate.statusWrites)
                .containsExactly(EtlStatus.PROCESSING, EtlStatus.ERROR, EtlStatus.ERROR, EtlStatus.ERROR);
        assertThat(jdbcTemplate.failingErrorWrites.get()).isZero();
    }

    @Test
    @DisplayName("TC-ETL-009: Retrying a failed status write is bounded and does not mask the original exception")
    void failedErrorStatusWrite_retryIsBounded() {
        PgDataset dataset = newDataset();
        dataset.setStatusRetryDelays(1, 1);
        jdbcTemplate.failingErrorWrites.set(Integer.MAX_VALUE);

        assertThatThrownBy(() -> dataset.populate(tempDir.resolve("missing.csv").toFile(), true))
                .isInstanceOf(NoSuchFileException.class);

        assertThat(jdbcTemplate.statusWrites)
                .containsExactly(EtlStatus.PROCESSING, EtlStatus.ERROR, EtlStatus.ERROR, EtlStatus.ERROR);
    }

    private PgDataset newDataset() {
        String id = UUID.randomUUID().toString();
        return new PgDataset(rowStore(), id, EtlStatus.ACCEPTED_DATA, new Date(), "data_" + id.replace("-", ""));
    }

    private RowStore rowStore() {
        RowStoreConfig config = new RowStoreConfig(new JSONObject()
                .put("baseurl", "http://localhost/")
                .put("database", new JSONObject()
                        .put("host", "localhost")
                        .put("database", "unused")
                        .put("user", "unused")
                        .put("password", "unused")));
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> method.getReturnType() == boolean.class ? false : null);
        return new RowStore() {
            @Override public Connection getConnection() { return connection; }
            @Override public Connection getQueryConnection() { return connection; }
            @Override public JdbcTemplate getJdbcTemplate() { return jdbcTemplate; }
            @Override public JdbcTemplate getQueryJdbcTemplate() { return jdbcTemplate; }
            @Override public Datasets getDatasets() { throw new UnsupportedOperationException(); }
            @Override public RowStoreConfig getConfig() { return config; }
            @Override public EtlProcessor getEtlProcessor() { throw new UnsupportedOperationException(); }
            @Override public void shutdown() { }
        };
    }

    /**
     * Records status writes (successful or not) and simulates a database that cannot be read from
     * and that rejects a configurable number of writes of the error status.
     */
    private static final class FakeJdbcTemplate extends JdbcTemplate {
        final List<Integer> statusWrites = new CopyOnWriteArrayList<>();
        final AtomicInteger statusReads = new AtomicInteger();
        final AtomicInteger failingErrorWrites = new AtomicInteger();

        @Override
        public int update(String sql, Object... args) {
            if (sql.contains("SET status")) {
                int status = (Integer) args[0];
                statusWrites.add(status);
                if (status == EtlStatus.ERROR && failingErrorWrites.getAndUpdate(n -> Math.max(n - 1, 0)) > 0) {
                    throw new DataAccessResourceFailureException("Simulated database outage");
                }
            }
            return 1;
        }

        @Override
        public Map<String, Object> queryForMap(String sql, Object... args) {
            statusReads.incrementAndGet();
            throw new DataAccessResourceFailureException("Simulated database outage");
        }
    }

}
