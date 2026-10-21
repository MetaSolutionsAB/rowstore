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

import org.entrystore.rowstore.config.DatabaseExceptionHandler;
import org.entrystore.rowstore.store.ConnectionUsage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests the connection budget {@link LimitedDataSource} (ARCH-7) with a fake target data source.
 * Specification: specs/tests/12-connection-limits.md
 */
class LimitedDataSourceTest {

    private final FakeDataSource target = new FakeDataSource();

    @Test
    @DisplayName("TC-CONN-001: Waiting requests get a connection in FIFO order")
    void waitingRequests_areServedInFifoOrder() throws Exception {
        LimitedDataSource ds = new LimitedDataSource(target, "test", 1, 10000);
        Connection held = ds.getConnection();
        List<Integer> order = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> waiters = new CopyOnWriteArrayList<>();
        for (int i = 1; i <= 5; i++) {
            int id = i;
            waiters.add(CompletableFuture.runAsync(() -> {
                try (Connection ignored = ds.getConnection()) {
                    order.add(id);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }, runnable -> Thread.ofVirtual().start(runnable)));
            // the next waiter is started only after this one is queued
            awaitTrue(() -> ds.getUsage().waiting() == id);
        }

        held.close();
        CompletableFuture.allOf(waiters.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        assertThat(order).containsExactly(1, 2, 3, 4, 5);
        assertThat(ds.getUsage()).isEqualTo(new ConnectionUsage(0, 1, 0));
    }

    @Test
    @DisplayName("TC-CONN-002: A request waits for a free connection and gets it when one is closed")
    void request_waitsUntilConnectionIsClosed() throws Exception {
        LimitedDataSource ds = new LimitedDataSource(target, "test", 1, 10000);
        Connection held = ds.getConnection();
        CompletableFuture<Connection> waiter = CompletableFuture.supplyAsync(() -> {
            try {
                return ds.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, runnable -> Thread.ofVirtual().start(runnable));
        awaitTrue(() -> ds.getUsage().waiting() == 1);
        assertThat(ds.getUsage()).isEqualTo(new ConnectionUsage(1, 1, 1));
        Thread.sleep(200);
        assertThat(waiter).isNotDone();

        held.close();

        Connection acquired = waiter.get(5, TimeUnit.SECONDS);
        assertThat(ds.getUsage()).isEqualTo(new ConnectionUsage(1, 1, 0));
        acquired.close();
        assertThat(ds.getUsage()).isEqualTo(new ConnectionUsage(0, 1, 0));
        assertThat(target.opened.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC-CONN-003: A wait timeout throws an exception that maps to 503, also via JdbcTemplate")
    void waitTimeout_throwsExceptionMappedTo503() throws Exception {
        LimitedDataSource ds = new LimitedDataSource(target, "test", 1, 300);
        try (Connection ignored = ds.getConnection()) {
            long start = System.nanoTime();
            assertThatThrownBy(ds::getConnection).isInstanceOf(SQLTransientConnectionException.class)
                    .hasMessageContaining("No test connection available within 300 ms");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));

            Throwable thrown = null;
            try {
                new JdbcTemplate(ds).queryForList("SELECT 1");
            } catch (RuntimeException e) {
                thrown = e;
            }
            assertThat(thrown).isInstanceOf(CannotGetJdbcConnectionException.class)
                    .isInstanceOf(DataAccessResourceFailureException.class)
                    .hasCauseInstanceOf(SQLTransientConnectionException.class);
            assertThat(new DatabaseExceptionHandler().handleDatabaseUnavailable(
                    (DataAccessResourceFailureException) thrown, new MockHttpServletResponse()).getStatusCode())
                    .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        }
        assertThat(target.opened.get()).isEqualTo(1);
        assertThat(ds.getUsage()).isEqualTo(new ConnectionUsage(0, 1, 0));
    }

    @Test
    @DisplayName("TC-CONN-004: Closing a connection releases its slot exactly once, also on double close and failing close")
    void close_releasesSlotExactlyOnce() throws Exception {
        LimitedDataSource ds = new LimitedDataSource(target, "test", 2, 200);
        Connection c = ds.getConnection();
        c.close();
        c.close();
        assertThat(ds.getUsage().inUse()).isZero();

        target.failClose = true;
        Connection failing = ds.getConnection();
        assertThatThrownBy(failing::close).isInstanceOf(SQLException.class).hasMessage("close failed");
        assertThatThrownBy(failing::close).isInstanceOf(SQLException.class);
        target.failClose = false;
        assertThat(ds.getUsage().inUse()).isZero();

        // no additional slots were created: only two connections can be open
        Connection first = ds.getConnection();
        Connection second = ds.getConnection();
        assertThatThrownBy(ds::getConnection).isInstanceOf(SQLTransientConnectionException.class);
        first.close();
        second.close();
        assertThat(ds.getUsage()).isEqualTo(new ConnectionUsage(0, 2, 0));
    }

    @Test
    @DisplayName("TC-CONN-005: A failure to open a connection releases the slot")
    void openFailure_releasesSlot() {
        LimitedDataSource ds = new LimitedDataSource(target, "test", 1, 200);
        target.failOpen = true;
        assertThatThrownBy(ds::getConnection).isInstanceOf(SQLException.class).hasMessage("open failed");
        assertThatThrownBy(ds::getConnection).isInstanceOf(SQLException.class).hasMessage("open failed");
        assertThat(ds.getUsage().inUse()).isZero();
    }

    @Test
    @DisplayName("TC-CONN-006: Budgets on the same data source are independent")
    void budgets_areIndependent() throws Exception {
        LimitedDataSource export = new LimitedDataSource(target, "export", 1, 200);
        LimitedDataSource query = new LimitedDataSource(target, "query", 2, 200);
        try (Connection ignored = export.getConnection()) {
            assertThatThrownBy(export::getConnection).isInstanceOf(SQLTransientConnectionException.class);
            long start = System.nanoTime();
            try (Connection q1 = query.getConnection(); Connection q2 = query.getConnection()) {
                assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(200));
                assertThat(query.getUsage()).isEqualTo(new ConnectionUsage(2, 2, 0));
                assertThat(export.getUsage()).isEqualTo(new ConnectionUsage(1, 1, 0));
            }
        }
        assertThat(query.getUsage().inUse()).isZero();
        assertThat(export.getUsage().inUse()).isZero();
    }

    @Test
    @DisplayName("TC-CONN-007: Statements and result sets return the limited connection, closing it releases the slot")
    void statementAndResultSet_returnLimitedConnection() throws Exception {
        LimitedDataSource ds = new LimitedDataSource(target, "test", 1, 200);
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        assertThat(stmt.getConnection()).isSameAs(conn);
        assertThat(rs.getStatement()).isSameAs(stmt);
        assertThat(conn.unwrap(Connection.class)).isNotSameAs(conn);

        // like the export, which closes the connection of the statement of the result set
        rs.getStatement().getConnection().close();

        assertThat(ds.getUsage().inUse()).isZero();
        assertThat(target.closed.get()).isEqualTo(1);
    }

    private static void awaitTrue(BooleanSupplier condition) {
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(5)).until(condition::getAsBoolean);
    }

    /**
     * Opens fake connections whose statements and result sets reference the fake (unlimited) objects, like a driver.
     */
    private static final class FakeDataSource extends org.springframework.jdbc.datasource.AbstractDataSource {

        final AtomicInteger opened = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        volatile boolean failOpen;
        volatile boolean failClose;

        @Override
        public Connection getConnection() throws SQLException {
            if (failOpen) {
                throw new SQLException("open failed");
            }
            opened.incrementAndGet();
            Connection[] conn = new Connection[1];
            conn[0] = proxy(Connection.class, (p, method, args) -> switch (method.getName()) {
                case "close" -> {
                    if (failClose) {
                        throw new SQLException("close failed");
                    }
                    closed.incrementAndGet();
                    yield null;
                }
                case "createStatement" -> statement(conn[0]);
                case "unwrap" -> p;
                default -> defaultValue(method.getReturnType());
            });
            return conn[0];
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        private static Statement statement(Connection conn) {
            Statement[] stmt = new Statement[1];
            stmt[0] = proxy(Statement.class, (p, method, args) -> switch (method.getName()) {
                case "getConnection" -> conn;
                case "executeQuery" -> proxy(ResultSet.class, (rp, rm, ra) ->
                        rm.getName().equals("getStatement") ? stmt[0] : defaultValue(rm.getReturnType()));
                default -> defaultValue(method.getReturnType());
            });
            return stmt[0];
        }

        @SuppressWarnings("unchecked")
        private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(FakeDataSource.class.getClassLoader(), new Class<?>[]{type}, handler);
        }

        private static Object defaultValue(Class<?> type) {
            if (type == boolean.class) {
                return false;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            return null;
        }
    }

}
