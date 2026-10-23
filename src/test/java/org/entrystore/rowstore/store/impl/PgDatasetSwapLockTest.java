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
 * until the wait limit. The waiting itself is covered by DatasetReplaceIT (TC-ETL-023, TC-ETL-053).
 */
class PgDatasetSwapLockTest {

    private final String id = UUID.randomUUID().toString();

    private final String dataTable = "data_" + id.replace("-", "");

    private final List<String> executed = new ArrayList<>();

    private int failingLocks;

    private String lockFailureState = "55P03";

    private boolean lockTimeout;

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

    private PgDataset newDataset() {
        RowStore rowStore = (RowStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{RowStore.class}, (proxy, method, args) -> null);
        return new PgDataset(rowStore, id, EtlStatus.PROCESSING, new Date(), dataTable);
    }

    /**
     * A prepared statement for the query of the sessions holding the data table, recorded as "LOCK HOLDERS",
     * which finds no sessions.
     */
    private PreparedStatement lockHoldersStatement() {
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ResultSet.class}, (proxy, method, args) -> method.getName().equals("next") ? false : null);
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    if (method.getName().equals("executeQuery")) {
                        executed.add("LOCK HOLDERS");
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
                    case "createStatement" -> statement;
                    case "prepareStatement" -> lockHoldersStatement();
                    default -> null;
                });
    }

}
