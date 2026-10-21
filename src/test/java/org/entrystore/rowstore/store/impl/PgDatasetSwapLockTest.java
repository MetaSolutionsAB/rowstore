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
 * out, e.g., because of a long streaming export, and that the last attempt waits without a timeout. The waiting itself is covered by DatasetReplaceIT (TC-ETL-023).
 */
class PgDatasetSwapLockTest {

    private final String id = UUID.randomUUID().toString();

    private final String dataTable = "data_" + id.replace("-", "");

    private final List<String> executed = new ArrayList<>();

    private int failingLocks;

    private String lockFailureState = "55P03";

    private boolean lockTimeout;

    @Test
    @DisplayName("TC-ETL-027: A timed out lock for replacing the data table is retried within a savepoint, the last attempt without timeout")
    void timedOutLock_isRetriedWithinSavepoint_lastAttemptWithoutTimeout() throws SQLException {
        PgDataset dataset = newDataset();
        dataset.setSwapRetryDelays(1, 1, 1);

        failingLocks = 2;
        assertThat(dataset.lockDataTableForReplace(connection())).isTrue();
        assertThat(executed).containsExactly(
                "SET LOCAL idle_in_transaction_session_timeout = 0",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE",
                "ROLLBACK TO SAVEPOINT",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE",
                "ROLLBACK TO SAVEPOINT",
                "SAVEPOINT", "SET LOCAL lock_timeout = '3s'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE");

        // one attempt with a timeout per delay, then an attempt that waits until it gets the lock
        executed.clear();
        failingLocks = Integer.MAX_VALUE;
        assertThat(dataset.lockDataTableForReplace(connection())).isTrue();
        assertThat(executed.stream().filter(s -> s.equals("SET LOCAL lock_timeout = '3s'"))).hasSize(3);
        assertThat(executed.stream().filter(s -> s.equals("ROLLBACK TO SAVEPOINT"))).hasSize(3);
        assertThat(executed.subList(executed.size() - 3, executed.size())).containsExactly(
                "SAVEPOINT", "SET LOCAL lock_timeout = '0'", "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE");
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
                    default -> null;
                });
    }

}
