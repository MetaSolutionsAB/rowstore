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

import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.RowStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the retries of {@link PgDataset#lockDataTableForReplace(Connection)} when the lock of the data table times
 * out, e.g., because of a long streaming export: every attempt waits at most 3 seconds, and the attempts are retried
 * until the wait limit. The waiting itself is covered by DatasetReplaceIT (TC-ETL-023, TC-ETL-053). Also tests the
 * attempts of {@link PgDataset#dropTablesOfDeletedDataset(int)}, which are retried in the same way (ETL-12.01).
 */
class PgDatasetSwapLockTest {

    private final String id = UUID.randomUUID().toString();

    private final String dataTable = "data_" + id.replace("-", "");

    private final List<String> executed = new ArrayList<>();

    private int failingLocks;

    private String lockFailureState = "55P03";

    private boolean lockTimeout;

    private String dropFailureState;

    private boolean loadConnectionFails;

    @Test
    @DisplayName("TC-ETL-027: A timed out lock for replacing the data table is retried within a savepoint, every attempt with a short timeout")
    void timedOutLock_isRetriedWithinSavepoint_everyAttemptWithShortTimeout() throws SQLException {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(1, 1, 1);
        dataset.setSwapLockWaitLimit(60000, 2, 60000);

        failingLocks = 2;
        assertThat(dataset.lockDataTableForReplace(connection())).isTrue();
        assertThat(executed).containsExactly(
                "SET LOCAL idle_in_transaction_session_timeout = 0",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE",
                "ROLLBACK TO SAVEPOINT",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE",
                "ROLLBACK TO SAVEPOINT",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE");

        // after the short delays, the attempts go on with the same short timeout; the lock holders are logged once
        executed.clear();
        failingLocks = 5;
        assertThat(dataset.lockDataTableForReplace(connection())).isTrue();
        assertThat(executed.stream().filter(s -> s.startsWith("SET LOCAL lock_timeout"))).hasSize(6)
                .allMatch(s -> s.equals("SET LOCAL lock_timeout = '3s'"));
        assertThat(executed.stream().filter(s -> s.equals("ROLLBACK TO SAVEPOINT"))).hasSize(5);
        assertThat(executed.stream().filter(s -> s.equals("LOCK HOLDERS"))).hasSize(1);
        assertThat(executed.subList(executed.size() - 3, executed.size())).containsExactly(
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE");
    }

    @Test
    @DisplayName("TC-ETL-051: If the lock cannot be acquired within the wait limit, locking gives up and logs the lock holders")
    void waitLimitExceeded_givesUp() throws SQLException {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(1, 1, 1);
        dataset.setSwapLockWaitLimit(200, 5, 60000);
        failingLocks = Integer.MAX_VALUE;

        assertThat(dataset.lockDataTableForReplace(connection())).isFalse();
        assertThat(executed.stream().filter(s -> s.startsWith("LOCK TABLE"))).hasSizeGreaterThan(4);
        assertThat(executed.stream().filter(s -> s.startsWith("SET LOCAL lock_timeout")))
                .allMatch(s -> s.equals("SET LOCAL lock_timeout = '3s'"));
        // once when the short delays are used up, and when giving up
        assertThat(executed.stream().filter(s -> s.equals("LOCK HOLDERS"))).hasSize(2);
        assertThat(executed.subList(executed.size() - 4, executed.size())).containsExactly(
                "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE", "ROLLBACK TO SAVEPOINT",
                "SAVEPOINT", "LOCK HOLDERS");
    }

    @Test
    @DisplayName("TC-ETL-054: While the lock is retried, the lock holders are logged at most once per interval")
    void lockHolders_areLoggedAtMostOncePerInterval() throws SQLException {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(1, 1, 1);
        dataset.setSwapLockWaitLimit(60000, 2, 0);

        // attempts 3 to 5 fail after the short delays are used up, each is followed by a log of the holders
        failingLocks = 5;
        assertThat(dataset.lockDataTableForReplace(connection())).isTrue();
        assertThat(executed.stream().filter(s -> s.equals("LOCK HOLDERS"))).hasSize(3);
    }

    @Test
    @DisplayName("TC-ETL-055: The delay between attempts to lock the data table grows up to 30 seconds")
    void retryDelay_growsUpToMaximum() {
        PgDataset dataset = newDataset();

        assertThat(dataset.swapRetryDelay(0)).isEqualTo(1000);
        assertThat(dataset.swapRetryDelay(3)).isEqualTo(5000);
        assertThat(dataset.swapRetryDelay(4)).isEqualTo(10000);
        assertThat(dataset.swapRetryDelay(5)).isEqualTo(20000);
        assertThat(dataset.swapRetryDelay(6)).isEqualTo(30000);
        assertThat(dataset.swapRetryDelay(100)).isEqualTo(30000);
    }

    @Test
    @DisplayName("TC-ETL-028: A lock for replacing the data table that fails for another reason is not retried")
    void otherLockFailure_isNotRetried() {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(1, 1, 1);
        failingLocks = Integer.MAX_VALUE;
        lockFailureState = "08006";

        assertThatThrownBy(() -> dataset.lockDataTableForReplace(connection()))
                .isInstanceOf(SQLException.class)
                .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("08006");
        assertThat(executed.stream().filter(s -> s.startsWith("LOCK TABLE"))).hasSize(1);
        assertThat(executed).doesNotContain("ROLLBACK TO SAVEPOINT");
    }

    @Test
    @DisplayName("TC-ETL-080: An attempt to drop a deleted dataset's tables waits up to 3 seconds for the lock, a timed out attempt is retried")
    void dropAttempt_waitsForLockWithShortTimeout_andIsRetried() {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(7, 7, 7);
        failingLocks = 1;

        assertThat(dataset.dropTablesOfDeletedDataset(0)).isEqualTo(7);
        assertThat(executed).containsExactly(
                "SET LOCAL lock_timeout = '3s'", "TABLES EXIST",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE",
                "ROLLBACK TO SAVEPOINT", "ROLLBACK");

        executed.clear();
        assertThat(dataset.dropTablesOfDeletedDataset(1)).isEqualTo(-1);
        assertThat(executed).containsExactly(
                "SET LOCAL lock_timeout = '3s'", "TABLES EXIST",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE",
                "DROP TABLE IF EXISTS " + dataTable + "_new", "DROP TABLE IF EXISTS " + dataTable, "COMMIT");
    }

    @Test
    @DisplayName("TC-ETL-081: A drop attempt that fails for another reason is retried until the wait limit, unless the failure cannot pass")
    void dropAttemptFailure_isRetriedUntilWaitLimit_unlessItCannotPass() {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(7, 7, 7);

        // e.g., no load connection within connectionWaitTimeout, a connection failure, a statement timeout
        loadConnectionFails = true;
        assertThat(dataset.dropTablesOfDeletedDataset(0)).isEqualTo(7);
        loadConnectionFails = false;
        for (String state : List.of("08006", "57014", "55P03")) {
            dropFailureState = state;
            assertThat(dataset.dropTablesOfDeletedDataset(1)).as(state).isEqualTo(7);
        }

        // insufficient privilege, dependent objects (e.g., a view on the data table)
        for (String state : List.of("42501", "2BP01")) {
            dropFailureState = state;
            assertThat(dataset.dropTablesOfDeletedDataset(1)).as(state).isEqualTo(-1);
        }

        dropFailureState = "08006";
        dataset.setSwapLockWaitLimit(0, 7, 60000);
        assertThat(dataset.dropTablesOfDeletedDataset(1)).isEqualTo(-1);
    }

    private PgDataset newDataset() {
        RowStore rowStore = (RowStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{RowStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getLoadConnection")) {
                        if (loadConnectionFails) {
                            throw new SQLTransientConnectionException("Simulated connection wait timeout");
                        }
                        return connection();
                    }
                    return null;
                });
        return new PgDataset(rowStore, id, EtlStatus.PROCESSING, new Date(), dataTable);
    }

    /**
     * A prepared statement for the query of the sessions holding the data table, recorded as "LOCK HOLDERS", which
     * finds no sessions; for the query whether the data table and the shadow table exist, recorded as "TABLES EXIST",
     * which finds only the data table; for other queries (whether a dataset references the data table), one that
     * finds nothing.
     */
    private PreparedStatement preparedStatement(String sql) {
        boolean tablesExist = sql.startsWith("SELECT to_regclass");
        String recordedAs = sql.contains("pg_locks") ? "LOCK HOLDERS" : tablesExist ? "TABLES EXIST" : null;
        boolean[] rowRead = {false};
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ResultSet.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "next" -> {
                        boolean hasRow = tablesExist && !rowRead[0];
                        rowRead[0] = true;
                        yield hasRow;
                    }
                    case "getBoolean" -> (Integer) args[0] == 1;
                    default -> null;
                });
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    if (method.getName().equals("executeQuery")) {
                        if (recordedAs != null) {
                            executed.add(recordedAs);
                        }
                        return resultSet;
                    }
                    return null;
                });
    }

    /**
     * A connection that records savepoints and statements, and lets a configurable number of LOCK TABLE
     * statements fail.
     */
    private Connection connection() {
        Savepoint savepoint = (Savepoint) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Savepoint.class}, (proxy, method, args) -> null);
        Statement statement = (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Statement.class}, (proxy, method, args) -> {
                    if (method.getName().equals("execute")) {
                        String sql = (String) args[0];
                        executed.add(sql);
                        if (sql.startsWith("SET LOCAL lock_timeout")) {
                            lockTimeout = !sql.endsWith("'0'");
                        }
                        // without a lock timeout, a lock request waits until it is granted
                        if (sql.startsWith("LOCK TABLE") && lockTimeout && failingLocks > 0) {
                            failingLocks--;
                            throw new SQLException("Simulated lock failure", lockFailureState);
                        }
                        if (sql.startsWith("DROP TABLE") && dropFailureState != null) {
                            throw new SQLException("Simulated drop failure", dropFailureState);
                        }
                        return false;
                    }
                    return null;
                });
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "setSavepoint" -> {
                        executed.add("SAVEPOINT");
                        yield savepoint;
                    }
                    case "rollback" -> {
                        executed.add(args != null ? "ROLLBACK TO SAVEPOINT" : "ROLLBACK");
                        yield null;
                    }
                    case "commit" -> {
                        executed.add("COMMIT");
                        yield null;
                    }
                    case "createStatement" -> statement;
                    case "prepareStatement" -> preparedStatement((String) args[0]);
                    default -> null;
                });
    }

}
