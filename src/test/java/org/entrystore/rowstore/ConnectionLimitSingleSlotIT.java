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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.restassured.response.Response;
import org.entrystore.rowstore.ConnectionLimitTestSupport.SlowDataset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.entrystore.rowstore.ConnectionLimitTestSupport.awaitLockWaiters;
import static org.entrystore.rowstore.ConnectionLimitTestSupport.awaitSleepers;
import static org.entrystore.rowstore.ConnectionLimitTestSupport.lockDataTable;
import static org.entrystore.rowstore.ConnectionLimitTestSupport.storedStatus;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

/**
 * Integration tests verifying that requests and ETL jobs hold at most one connection at a time (ARCH-7.04): with a
 * single connection per budget (query, export, load) and a wait timeout of 5 s, a nested connection request would
 * wait for the timeout. Every request must therefore complete well within the wait timeout.
 * Specification: specs/tests/12-connection-limits.md
 */
@DisplayName("Connection Limit Single Slot Tests")
class ConnectionLimitSingleSlotIT extends ConfigurableTestBase {

    private static final long MAX_REQUEST_MILLIS = 4000;

    private static final ConfigurableTestEnvironment ENV = ConfigurableTestEnvironment.builder()
            .maxQueryConnections(1)
            .maxExportConnections(1)
            .maxEtlProcesses(1)
            .connectionWaitTimeout(5)
            .buildEnvironment();

    private final List<String> datasetUrls = new ArrayList<>();

    private DatasetUrls dataset;

    @Override
    protected String getExtensionBaseUrl() {
        ENV.start();
        return ENV.getBaseUrl();
    }

    @BeforeAll
    void createDataset() {
        dataset = create();
    }

    @AfterAll
    void cleanup() {
        datasetUrls.forEach(url -> given().delete(url));
        ENV.stop();
    }

    @Test
    @DisplayName("TC-CONN-014: Appends and replaces work with one connection per budget")
    void appendAndReplace_work() {
        DatasetUrls loaded = create();
        byte[] csv = loadTestData("dataset1_utf8.csv");

        given().spec(csvSpec).body(csv).post(loaded.datasetUrl).then().statusCode(202);
        waitForDatasetAvailable(loaded.infoUrl);
        assertRowCount(loaded, 10);

        given().spec(csvSpec).body(csv).put(loaded.datasetUrl).then().statusCode(202);
        waitForDatasetAvailable(loaded.infoUrl);
        assertRowCount(loaded, 5);
    }

    @Test
    @DisplayName("TC-CONN-015: Alias PUT and POST complete within the wait timeout with one connection per budget")
    void aliasPutAndPost_work() {
        given().contentType("application/json").body("[\"singleslotone\"]")
        .when()
                .put(dataset.datasetUrl + "/aliases")
        .then()
                .statusCode(204)
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);

        given().contentType("application/json").body("[\"singleslottwo\"]")
        .when()
                .post(dataset.datasetUrl + "/aliases")
        .then()
                .statusCode(204)
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);

        given().spec(jsonSpec)
        .when()
                .get(dataset.datasetUrl + "/aliases")
        .then()
                .statusCode(200)
                .body("", containsInAnyOrder("singleslotone", "singleslottwo"));

        // an alias of 32 hexadecimal digits is the ID of a dataset for PostgreSQL and is not available
        DatasetUrls other = create();
        given().contentType("application/json").body("[\"" + other.datasetId.replace("-", "") + "\"]")
        .when()
                .put(dataset.datasetUrl + "/aliases")
        .then()
                .statusCode(400)
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);

        // an alias of another dataset is not available
        given().contentType("application/json").body("[\"singleslotone\"]")
        .when()
                .put(other.datasetUrl + "/aliases")
        .then()
                .statusCode(400)
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);
    }

    @Test
    @DisplayName("TC-CONN-016: Column names are cleaned by an append with one connection per budget")
    void columnCleanupByAppend_works() throws Exception {
        DatasetUrls unclean = create();
        String table = "data_" + unclean.datasetId.replace("-", "");
        try (Connection conn = ENV.openDatabaseConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE " + table +
                     " SET data = (data - 'name') || jsonb_build_object(?::text, data -> 'name')")) {
            ps.setString(1, "﻿name");
            ps.executeUpdate();
        }

        given().spec(csvSpec).body(loadTestData("dataset1_utf8.csv")).post(unclean.datasetUrl).then().statusCode(202);
        waitForDatasetAvailable(unclean.infoUrl);

        given().spec(jsonSpec)
                .queryParam("name", "Åkesson")
        .when()
                .get(unclean.datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(2))
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);
    }

    @Test
    @DisplayName("TC-CONN-017: JSON and CSV exports work with one connection per budget")
    void exports_work() {
        given().accept("application/json")
        .when()
                .get(dataset.datasetUrl + "/export")
        .then()
                .statusCode(200)
                .body("", hasSize(5))
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);

        String csv = given().accept("text/csv")
        .when()
                .get(dataset.datasetUrl + "/export")
        .then()
                .statusCode(200)
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS)
                .extract().asString();
        // header and five rows
        assertThat(csv.lines().count()).isEqualTo(6);
    }

    @Test
    @DisplayName("TC-CONN-018: Health check and status complete within the wait timeout with one connection per budget")
    void healthAndStatus_work() {
        given()
        .when()
                .get("/actuator/health")
        .then()
                .statusCode(200)
                .body("status", equalTo("UP"))
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);

        given().spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .body("connections.database.query.max", equalTo(1))
                .body("connections.database.export.max", equalTo(1))
                .body("connections.database.load.max", equalTo(1))
                .body("connections.database.keySet()", hasItem("load"))
                .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);
    }

    @Test
    @DisplayName("TC-CONN-019: A load does not hold a query connection while it holds its load connection, for appends and replaces")
    void load_holdsNoQueryConnection() throws Exception {
        DatasetUrls loaded = create();
        byte[] csv = loadTestData("dataset1_utf8.csv");
        for (boolean append : new boolean[]{true, false}) {
            try (Connection lock = lockDataTable(ENV, loaded.datasetId, "ACCESS EXCLUSIVE")) {
                if (append) {
                    given().spec(csvSpec).body(csv).post(loaded.datasetUrl).then().statusCode(202);
                } else {
                    given().spec(csvSpec).body(csv).put(loaded.datasetUrl).then().statusCode(202);
                }
                // the load waits for the lock while it holds its load connection
                awaitLockWaiters(ENV, loaded.datasetId, 1);

                given().spec(jsonSpec)
                .when()
                        .get("/status")
                .then()
                        .statusCode(200)
                        .body("connections.database.load.inUse", equalTo(1))
                        .body("connections.database.query.inUse", equalTo(0));
                lock.rollback();
            }
            waitForDatasetAvailable(loaded.infoUrl);
        }
        assertRowCount(loaded, 5);
    }

    @Test
    @DisplayName("TC-CONN-020: A load writes its final status while all query connections are in use")
    void load_writesStatusWhileQueryConnectionsInUse() throws Exception {
        DatasetUrls loaded = create();
        DatasetUrls other = create();
        try (Connection lock = lockDataTable(ENV, loaded.datasetId, "ACCESS EXCLUSIVE")) {
            given().spec(csvSpec).body(loadTestData("dataset1_utf8.csv")).post(loaded.datasetUrl).then().statusCode(202);
            awaitLockWaiters(ENV, loaded.datasetId, 1);
            try (SlowDataset ignored = new SlowDataset(ENV, other.datasetId, 30)) {
                // a query of the other dataset holds the only query connection
                CompletableFuture<Response> query = CompletableFuture.supplyAsync(() -> given().spec(jsonSpec).get(other.datasetUrl));
                awaitSleepers(ENV, other.datasetId, 1);

                lock.rollback();

                // within less than the wait timeout, so the status was not written with a query connection
                await().atMost(Duration.ofMillis(MAX_REQUEST_MILLIS)).pollInterval(Duration.ofMillis(100))
                        .until(() -> storedStatus(ENV, loaded.datasetId) == ETL_STATUS_AVAILABLE);
                assertThat(query).isNotDone();
            }
        }
        assertRowCount(loaded, 10);
    }

    @Test
    @DisplayName("TC-CONN-022: Health check and status do not wait for the wait timeout if all query connections are in use")
    void healthAndStatus_doNotWaitForWaitTimeout() throws Exception {
        DatasetUrls slow = create();
        try (SlowDataset ignored = new SlowDataset(ENV, slow.datasetId, 30)) {
            CompletableFuture<Response> query = CompletableFuture.supplyAsync(() -> given().spec(jsonSpec).get(slow.datasetUrl));
            awaitSleepers(ENV, slow.datasetId, 1);

            given()
            .when()
                    .get("/actuator/health")
            .then()
                    .statusCode(200)
                    .body("status", equalTo("UP"))
                    .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);

            given().spec(jsonSpec)
            .when()
                    .get("/status")
            .then()
                    .statusCode(200)
                    .body("datasets", equalTo(-1))
                    .body("connections.database.query.inUse", equalTo(1))
                    .time(lessThan(MAX_REQUEST_MILLIS), TimeUnit.MILLISECONDS);
            assertThat(query).isNotDone();
        }
    }

    private DatasetUrls create() {
        DatasetUrls urls = createDatasetAndWait(loadTestData("dataset1_utf8.csv"));
        datasetUrls.add(urls.datasetUrl);
        return urls;
    }

    private void assertRowCount(DatasetUrls urls, int expected) {
        given().spec(jsonSpec)
        .when()
                .get(urls.infoUrl)
        .then()
                .statusCode(200)
                .body("rowcount", equalTo(expected));
    }

}
