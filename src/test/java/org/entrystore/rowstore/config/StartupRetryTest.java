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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import java.net.ConnectException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests of the retries of the database work at startup (ARCH-7.08).
 */
@DisplayName("StartupRetry Tests")
class StartupRetryTest {

    private final List<Long> sleeps = new ArrayList<>();

    private final StartupRetry retry = new StartupRetry(1000, 60000, sleeps::add);

    @Test
    @DisplayName("TC-CONN-030: Connectivity failures at startup are retried with growing delays until the task succeeds")
    void connectivityFailures_areRetriedUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();

        String result = retry.call("migrate", () -> {
            if (attempts.incrementAndGet() <= 8) {
                throw new RuntimeException("Unable to obtain connection", new SQLException("Connection refused", "08001"));
            }
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(9);
        assertThat(sleeps).containsExactly(1000L, 2000L, 4000L, 8000L, 16000L, 32000L, 60000L, 60000L);
    }

    @Test
    @DisplayName("TC-CONN-031: Other failures at startup, e.g., of a migration or the authentication, are not retried")
    void otherFailures_failFast() {
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException migrationFailure = new IllegalStateException("Migration failed",
                new SQLException("syntax error", "42601"));

        assertThatThrownBy(() -> retry.call("migrate", () -> {
            attempts.incrementAndGet();
            throw migrationFailure;
        })).isSameAs(migrationFailure);
        assertThatThrownBy(() -> retry.call("migrate", () -> {
            attempts.incrementAndGet();
            throw new SQLException("password authentication failed", "28P01");
        })).isInstanceOf(IllegalStateException.class).hasCauseInstanceOf(SQLException.class);
        assertThat(attempts).hasValue(2);
        assertThat(sleeps).isEmpty();
    }

    @Test
    @DisplayName("TC-CONN-032: Connection failures, an unavailable database and failed connection attempts count as connectivity failures")
    void connectivityFailures_areDetected() {
        assertThat(StartupRetry.isConnectivityFailure(new SQLException("refused", "08001"))).isTrue();
        assertThat(StartupRetry.isConnectivityFailure(new SQLException("the database system is starting up", "57P03"))).isTrue();
        assertThat(StartupRetry.isConnectivityFailure(new SQLException("terminating connection", "57P01"))).isTrue();
        assertThat(StartupRetry.isConnectivityFailure(new SQLException("too many clients", "53300"))).isTrue();
        assertThat(StartupRetry.isConnectivityFailure(new SQLTransientConnectionException("pool timeout"))).isTrue();
        assertThat(StartupRetry.isConnectivityFailure(new CannotGetJdbcConnectionException("no connection"))).isTrue();
        assertThat(StartupRetry.isConnectivityFailure(new RuntimeException(new ConnectException("refused")))).isTrue();

        assertThat(StartupRetry.isConnectivityFailure(new SQLException("authentication failed", "28P01"))).isFalse();
        assertThat(StartupRetry.isConnectivityFailure(new SQLException("relation does not exist", "42P01"))).isFalse();
        assertThat(StartupRetry.isConnectivityFailure(new IllegalStateException("other"))).isFalse();
    }

    @Test
    @DisplayName("TC-CONN-033: An interruption while waiting for the database fails the startup")
    void interruption_failsStartup() {
        StartupRetry interrupted = new StartupRetry(1000, 60000, millis -> {
            throw new InterruptedException();
        });

        assertThatThrownBy(() -> interrupted.call("migrate", () -> {
            throw new SQLException("refused", "08001");
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("Interrupted");
        assertThat(Thread.interrupted()).isTrue();
    }

}
