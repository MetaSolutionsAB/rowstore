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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.UncategorizedSQLException;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests which failures stop the cleanup of column names (ETL-11.03).
 */
class PgRowStoreConnectionFailureTest {

    @Test
    @DisplayName("TC-ETL-047: Connection failures, also as causes, stop the column name cleanup, other failures do not")
    void connectionFailures_areDetected() {
        assertThat(PgRowStore.isConnectionFailure(new CannotGetJdbcConnectionException("no connection"))).isTrue();
        assertThat(PgRowStore.isConnectionFailure(new SQLTransientConnectionException("pool timeout"))).isTrue();
        assertThat(PgRowStore.isConnectionFailure(new SQLException("connection refused", "08001"))).isTrue();
        assertThat(PgRowStore.isConnectionFailure(
                new CompletionException(new IllegalStateException(new SQLException("I/O error", "08006"))))).isTrue();

        assertThat(PgRowStore.isConnectionFailure(new SQLException("syntax error", "42601"))).isFalse();
        assertThat(PgRowStore.isConnectionFailure(
                new UncategorizedSQLException("query", "SELECT", new SQLException("division by zero", "22012")))).isFalse();
        assertThat(PgRowStore.isConnectionFailure(new IllegalStateException("other"))).isFalse();
    }

}
