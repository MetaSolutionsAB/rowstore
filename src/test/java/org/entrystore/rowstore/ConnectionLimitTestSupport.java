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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;

/**
 * Database manipulations for the connection limit tests, with direct connections to the test database: locking data
 * tables and making queries of a dataset slow, so that requests hold their connections.
 */
final class ConnectionLimitTestSupport {

    private ConnectionLimitTestSupport() {
    }

    /**
     * Runs concurrent client requests, each on its own thread. The common pool of CompletableFuture has too few threads
     * on machines with few CPUs (e.g., CI runners), so that requests that a test expects to wait at the same time would
     * run one after another.
     */
    private static final ExecutorService CLIENTS = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "connection-limit-client");
        thread.setDaemon(true);
        return thread;
    });

    static <T> CompletableFuture<T> supplyAsync(Supplier<T> request) {
        return CompletableFuture.supplyAsync(request, CLIENTS);
    }

    static CompletableFuture<Void> runAsync(Runnable request) {
        return CompletableFuture.runAsync(request, CLIENTS);
    }

    static String dataTable(String datasetId) {
        return "data_" + datasetId.replace("-", "");
    }

    /**
     * Locks the data table of a dataset in the given mode until the returned connection is rolled back or closed.
     */
    static Connection lockDataTable(ConfigurableTestEnvironment env, String datasetId, String mode) throws SQLException {
        Connection conn = env.openDatabaseConnection();
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LOCK TABLE " + dataTable(datasetId) + " IN " + mode + " MODE");
        }
        return conn;
    }

    /**
     * Waits until at least the given number of sessions wait for a lock on the data table of a dataset.
     */
    static void awaitLockWaiters(ConfigurableTestEnvironment env, String datasetId, int count) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).until(() ->
                count(env, "SELECT count(*) FROM pg_locks WHERE NOT granted AND relation = to_regclass(?)",
                        dataTable(datasetId)) >= count);
    }

    /**
     * Waits until at least the given number of sessions sleep in a query of a dataset made slow by {@link SlowDataset}.
     */
    static void awaitSleepers(ConfigurableTestEnvironment env, String datasetId, int count) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).until(() ->
                count(env, "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND query LIKE ?",
                        "%" + dataTable(datasetId) + "%") >= count);
    }

    /**
     * @return Returns the status of a dataset as stored in the database.
     */
    static int storedStatus(ConfigurableTestEnvironment env, String datasetId) throws SQLException {
        return count(env, "SELECT status FROM datasets WHERE id = ?::uuid", datasetId);
    }

    private static int count(ConfigurableTestEnvironment env, String sql, String param) throws SQLException {
        try (Connection conn = env.openDatabaseConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    /**
     * Makes every query of a dataset sleep (without waiting for a lock), by replacing its data table with a view that
     * calls pg_sleep. Closing cancels the sleeping queries and restores the data table.
     */
    static final class SlowDataset implements AutoCloseable {

        private final ConfigurableTestEnvironment env;
        private final String table;

        SlowDataset(ConfigurableTestEnvironment env, String datasetId, int sleepSeconds) throws SQLException {
            this.env = env;
            this.table = dataTable(datasetId);
            try (Connection conn = env.openDatabaseConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE " + table + " RENAME TO " + table + "_real");
                stmt.execute("CREATE VIEW " + table + " AS SELECT r.rownr, r.data FROM " + table + "_real r, pg_sleep(" +
                        sleepSeconds + ")");
            }
        }

        @Override
        public void close() throws SQLException {
            try (Connection conn = env.openDatabaseConnection(); Statement stmt = conn.createStatement()) {
                String sleepers = "SELECT pid FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND query LIKE '%" + table + "%'";
                await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).until(() -> {
                    try (Statement check = conn.createStatement()) {
                        check.execute("SELECT pg_cancel_backend(pid) FROM (" + sleepers + ") s");
                    }
                    try (Statement check = conn.createStatement(); ResultSet rs = check.executeQuery("SELECT count(*) FROM (" + sleepers + ") s")) {
                        rs.next();
                        return rs.getInt(1) == 0;
                    }
                });
                stmt.execute("DROP VIEW " + table);
                stmt.execute("ALTER TABLE " + table + "_real RENAME TO " + table);
            }
        }
    }

}
