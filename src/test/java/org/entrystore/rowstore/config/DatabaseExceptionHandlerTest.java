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

import org.entrystore.rowstore.store.impl.LimitedDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.mock.web.MockHttpServletResponse;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests of the mapping of database unavailability to 503 (ARCH-7.09).
 */
@DisplayName("DatabaseExceptionHandler Tests")
class DatabaseExceptionHandlerTest {

    private final DatabaseExceptionHandler handler = new DatabaseExceptionHandler();

    @Test
    @DisplayName("TC-CONN-034: Unavailability of the database results in 503 with Retry-After")
    void unavailableDatabase_returns503WithRetryAfter() {
        DataAccessException[] failures = {
                // no connection within the wait timeout
                new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection",
                        new LimitedDataSource.ConnectionWaitTimeoutException("No database query connection available")),
                // database not reachable
                new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection",
                        new SQLException("Connection refused", "08001")),
                // lock timeout
                new TransientDataAccessResourceException("Data table is locked",
                        new SQLException("canceling statement due to lock timeout", "55P03"))
        };
        for (DataAccessException failure : failures) {
            ResponseEntity<Void> response = handler.handleDatabaseUnavailable(failure, new MockHttpServletResponse());

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                    .isEqualTo(String.valueOf(DatabaseExceptionHandler.RETRY_AFTER_SECONDS));
            assertThat(response.getBody()).isNull();
        }
    }

    @Test
    @DisplayName("TC-CONN-035: A failure after a streamed response was committed is rethrown")
    void committedResponse_isRethrown() {
        MockHttpServletResponse committed = new MockHttpServletResponse();
        committed.setCommitted(true);
        CannotGetJdbcConnectionException failure = new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection");

        assertThatThrownBy(() -> handler.handleDatabaseUnavailable(failure, committed)).isSameAs(failure);
    }

}
