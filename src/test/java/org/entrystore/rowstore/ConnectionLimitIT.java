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

import io.restassured.response.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the connection budgets (ARCH-7) with small limits: 2 query connections, 1 export connection,
 * 2 ETL processes (load connections) and a wait timeout of 2 s. Data tables are locked with a direct database
 * connection to make requests hold their connections.
 * Specification: specs/tests/12-connection-limits.md
 */
@DisplayName("Connection Limit Tests")
class ConnectionLimitIT extends ConfigurableTestBase {

    private static final int WAIT_TIMEOUT_SECONDS = 2;

    private static final ConfigurableTestEnvironment ENV = ConfigurableTestEnvironment.builder()
            .maxQueryConnections(2)
            .maxExportConnections(1)
            .maxEtlProcesses(2)
            .connectionWaitTimeout(WAIT_TIMEOUT_SECONDS)
            .buildEnvironment();

    private DatasetUrls first;
    private DatasetUrls second;

    @Override
    protected String getExtensionBaseUrl() {
        ENV.start();
        return ENV.getBaseUrl();
    }

    @BeforeAll
    void createDatasets() {
        byte[] csv = loadTestData("dataset1_utf8.csv");
        first = createDatasetAndWait(csv);
        second = createDatasetAndWait(csv);
    }

    @AfterAll
    void cleanup() {
        if (first != null) {
            given().delete(first.datasetUrl);
        }
        if (second != null) {
            given().delete(second.datasetUrl);
        }
        ENV.stop();
    }

    @Test
    @DisplayName("TC-CONN-010: Status reports the usage of the connection budgets")
    void status_reportsConnectionUsage() {
        given().spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .body("service", equalTo("RowStore"))
                .body("connections.database.query.max", equalTo(2))
                .body("connections.database.query.inUse", instanceOf(Integer.class))
                .body("connections.database.query.waiting", equalTo(0))
                .body("connections.database.export.max", equalTo(1))
                .body("connections.database.export.inUse", equalTo(0))
                .body("connections.database.load.max", equalTo(2))
                .body("connections.database.load.inUse", equalTo(0))
                .body("connections.queryDatabase", equalTo(null));
    }

    @Test
    @DisplayName("TC-CONN-011: A burst of more concurrent queries than query connections succeeds without 503")
    void burstOfQueries_succeeds() throws Exception {
        List<CompletableFuture<Integer>> requests = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            requests.add(CompletableFuture.supplyAsync(() -> given().spec(jsonSpec)
                    .queryParam("name", "Åkesson")
                    .get(first.datasetUrl)
                    .statusCode()));
        }
        for (CompletableFuture<Integer> request : requests) {
            assertThat(request.get(30, TimeUnit.SECONDS)).isEqualTo(200);
        }
    }

    @Test
    @DisplayName("TC-CONN-012: A request gets 503 if all query connections stay in use for longer than the wait timeout")
    void allQueryConnectionsInUse_resultsIn503AfterWaitTimeout() throws Exception {
        try (Connection lock = lockDataTable(first.datasetId)) {
            // both queries hold a query connection while they wait for the lock
            CompletableFuture<Response> q1 = CompletableFuture.supplyAsync(() -> given().spec(jsonSpec).get(first.datasetUrl));
            CompletableFuture<Response> q2 = CompletableFuture.supplyAsync(() -> given().spec(jsonSpec).get(first.datasetUrl));
            awaitLockWaiters(lock, first.datasetId, 2);

            long start = System.nanoTime();
            given().spec(jsonSpec)
            .when()
                    .get(second.infoUrl)
            .then()
                    .statusCode(503);
            Duration waited = Duration.ofNanos(System.nanoTime() - start);
            assertThat(waited).isGreaterThanOrEqualTo(Duration.ofMillis(WAIT_TIMEOUT_SECONDS * 1000 - 100))
                    .isLessThan(Duration.ofSeconds(WAIT_TIMEOUT_SECONDS + 8));

            lock.rollback();
            assertThat(q1.get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
            assertThat(q2.get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        }
        given().spec(jsonSpec).get(second.infoUrl).then().statusCode(200);
    }

    @Test
    @DisplayName("TC-CONN-013: A long export does not block queries, an excess export fails after the wait timeout")
    void longExport_doesNotBlockQueries() throws Exception {
        try (Connection lock = lockDataTable(first.datasetId)) {
            // the export holds the only export connection while it waits for the lock
            CompletableFuture<Response> export = CompletableFuture.supplyAsync(() -> given()
                    .accept("application/json").get(first.datasetUrl + "/export"));
            awaitLockWaiters(lock, first.datasetId, 1);

            long start = System.nanoTime();
            given().spec(jsonSpec)
                    .queryParam("name", "Åkesson")
            .when()
                    .get(second.datasetUrl)
            .then()
                    .statusCode(200)
                    .body("results", hasSize(1));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(WAIT_TIMEOUT_SECONDS));

            start = System.nanoTime();
            Response excess = given().accept("application/json").get(second.datasetUrl + "/export");
            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .isGreaterThanOrEqualTo(Duration.ofMillis(WAIT_TIMEOUT_SECONDS * 1000 - 100));
            // 503 if the export connection is requested before the response status is sent, otherwise the export is
            // truncated, since the status cannot be changed anymore (API-6.05)
            assertThat(excess.statusCode() == 503 || !excess.asString().endsWith("]"))
                    .as("excess export fails, status %d", excess.statusCode()).isTrue();

            lock.rollback();
            Response exported = export.get(30, TimeUnit.SECONDS);
            assertThat(exported.statusCode()).isEqualTo(200);
            assertThat(exported.jsonPath().getList("")).hasSize(5);
        }
        given().accept("application/json").get(second.datasetUrl + "/export").then().statusCode(200);
        assertThat(given().spec(jsonSpec).get("/status").jsonPath().getInt("connections.database.export.inUse")).isZero();
    }

    /**
     * Locks the data table of a dataset exclusively until the returned connection is rolled back or closed.
     */
    private Connection lockDataTable(String datasetId) throws SQLException {
        Connection conn = ENV.openDatabaseConnection();
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LOCK TABLE " + dataTable(datasetId) + " IN ACCESS EXCLUSIVE MODE");
        }
        return conn;
    }

    /**
     * Waits until the given number of sessions wait for a lock on the data table of a dataset.
     */
    private static void awaitLockWaiters(Connection conn, String datasetId, int count) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).until(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM pg_locks WHERE NOT granted AND relation = ?::regclass")) {
                ps.setString(1, dataTable(datasetId));
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() && rs.getInt(1) == count;
                }
            }
        });
    }

    private static String dataTable(String datasetId) {
        return "data_" + datasetId.replace("-", "");
    }

}
