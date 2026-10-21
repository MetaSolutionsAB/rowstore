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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for the health check of a separate query database (ARCH-7.05): the query database is configured
 * with a port on which no database listens, the primary database is reachable.
 * Specification: specs/tests/12-connection-limits.md
 */
@DisplayName("Connection Limit Health Tests")
class ConnectionLimitHealthIT extends ConfigurableTestBase {

    private static final ConfigurableTestEnvironment ENV = ConfigurableTestEnvironment.builder()
            .queryDatabasePort(unusedPort())
            .buildEnvironment();

    @Override
    protected String getExtensionBaseUrl() {
        ENV.start();
        return ENV.getBaseUrl();
    }

    @AfterAll
    void cleanup() {
        ENV.stop();
    }

    @Test
    @DisplayName("TC-CONN-023: An unreachable separate query database is reported as DOWN by the health check")
    void unreachableQueryDatabase_isReportedDown() {
        given()
        .when()
                .get("/actuator/health")
        .then()
                .statusCode(503)
                .body("status", equalTo("DOWN"));

        // the primary database is reachable
        given().spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .body("datasets", greaterThanOrEqualTo(0))
                .body("connections.queryDatabase.query.max", equalTo(10))
                .body("connections.queryDatabase.export.max", equalTo(2))
                .body("connections.database.export", equalTo(null));
    }

    private static int unusedPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
