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

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.awaitility.core.ConditionTimeoutException;
import org.entrystore.rowstore.store.impl.PgRowStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the scheduling and failure handling of ETL loads.
 * Specification: 11-etl-processing.md
 */
@DisplayName("ETL Processing Tests")
class EtlProcessingIT extends BaseIntegrationTest {

    private static final Duration OUTAGE_MAX_WAIT = Duration.ofSeconds(30);

    @Test
    @DisplayName("TC-ETL-010: A dataset stuck in processing does not block a new upload")
    void stuckProcessingDataset_doesNotBlockNewUpload() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 2));
        try {
            setStatus(urls.datasetId, ETL_STATUS_PROCESSING);

            given().spec(csvSpec).body(csv("B", 3)).put(urls.datasetUrl).then().statusCode(202);

            waitForDatasetAvailable(urls.infoUrl);
            assertThat(names(urls.datasetUrl, 0)).containsExactly("B1", "B2", "B3");
        } finally {
            setStatus(urls.datasetId, ETL_STATUS_AVAILABLE);
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-011: Rapid uploads to the same dataset are applied in submission order")
    void rapidUploads_areAppliedInSubmissionOrder() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("X", 1));
        try {
            // the replace is large enough for the appends to arrive while it is being loaded
            int replaceRows = 20000;
            given().spec(csvSpec).body(csv("A", replaceRows)).put(urls.datasetUrl).then().statusCode(202);
            given().spec(csvSpec).body(csv("B", 2)).post(urls.datasetUrl).then().statusCode(202);
            given().spec(csvSpec).body(csv("C", 2)).post(urls.datasetUrl).then().statusCode(202);

            await().atMost(MAX_WAIT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
                assertThat(resultCount(urls.datasetUrl)).isEqualTo(replaceRows + 4);
                assertThat(status(urls.infoUrl)).isEqualTo(ETL_STATUS_AVAILABLE);
            });
            assertThat(names(urls.datasetUrl, 0)).startsWith("A1", "A2", "A3");
            assertThat(names(urls.datasetUrl, replaceRows - 1)).containsExactly("A" + replaceRows, "B1", "B2", "C1", "C2");
        } finally {
            awaitLoadsFinishedAndDelete(urls);
        }
    }

    @Test
    @DisplayName("TC-ETL-012: Of two rapid replaces of the same dataset the last one wins")
    void rapidReplaces_lastOneWins() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("X", 1));
        try {
            given().spec(csvSpec).body(csv("A", 20000)).put(urls.datasetUrl).then().statusCode(202);
            given().spec(csvSpec).body(csv("B", 2)).put(urls.datasetUrl).then().statusCode(202);

            await().atMost(MAX_WAIT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
                assertThat(names(urls.datasetUrl, 0)).containsExactly("B1", "B2");
                assertThat(status(urls.infoUrl)).isEqualTo(ETL_STATUS_AVAILABLE);
            });
        } finally {
            awaitLoadsFinishedAndDelete(urls);
        }
    }

    @Test
    @DisplayName("TC-ETL-013: A database outage during a load results in error status once the database is back")
    void databaseOutageDuringLoad_resultsInErrorStatus() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 2));
        String dataTable = "data_" + urls.datasetId.replace("-", "");
        try (Connection db = openDatabaseConnection();
             Connection locker = openDatabaseConnection();
             Connection maintenance = RowStoreTestEnvironment.getInstance().openMaintenanceDatabaseConnection()) {
            String database = queryString(db, "SELECT current_database()");

            // make the replace block on its TRUNCATE so the outage hits it while it is processing
            locker.setAutoCommit(false);
            try (Statement stmt = locker.createStatement()) {
                stmt.execute("LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE");
            }
            given().spec(csvSpec).body(csv("B", 2)).put(urls.datasetUrl).then().statusCode(202);
            int loaderPid = await().atMost(MAX_WAIT).until(() -> blockedTruncatePid(db, dataTable), pid -> pid > 0);

            try {
                // simulate an outage: no new connections, and the loader's connection is lost. This relies on the
                // default test environment using the unpooled PGSimpleDataSource: idle pooled connections would
                // survive ALLOW_CONNECTIONS false and the status could be written during the "outage".
                try (Statement stmt = maintenance.createStatement()) {
                    stmt.execute("ALTER DATABASE \"" + database + "\" WITH ALLOW_CONNECTIONS false");
                    stmt.execute("SELECT pg_terminate_backend(" + loaderPid + ")");
                }
                locker.rollback();

                // an unreachable database is reported as such, not as an unknown dataset
                given().spec(jsonSpec).get(urls.infoUrl).then().statusCode(503);
                // the loader cannot set the error status while the database is unreachable
                assertThat(queryString(db, "SELECT status FROM datasets WHERE id = '" + urls.datasetId + "'"))
                        .isEqualTo(String.valueOf(ETL_STATUS_PROCESSING));
            } finally {
                try (Statement stmt = maintenance.createStatement()) {
                    stmt.execute("ALTER DATABASE \"" + database + "\" WITH ALLOW_CONNECTIONS true");
                }
            }

            await().atMost(OUTAGE_MAX_WAIT).pollInterval(POLL_INTERVAL).untilAsserted(() ->
                    assertThat(status(urls.infoUrl)).isEqualTo(ETL_STATUS_ERROR));
        } finally {
            awaitLoadsFinishedAndDelete(urls);
        }
    }

    @Test
    @DisplayName("TC-ETL-014: Datasets left in processing are reset to error at startup")
    void startupReset_setsProcessingDatasetsToError() throws SQLException {
        DatasetUrls processing = createDatasetAndWait(csv("A", 1));
        DatasetUrls accepted = createDatasetAndWait(csv("B", 1));
        try {
            setStatus(processing.datasetId, ETL_STATUS_PROCESSING);
            setStatus(accepted.datasetId, ETL_STATUS_ACCEPTED_DATA);

            // invoked by PgRowStore's constructor at startup. It resets all datasets in PROCESSING of the shared
            // test database, which relies on test classes not running concurrently; a dataset of another test that
            // is being loaded would still end up with its correct final status, which is written unconditionally.
            List<String> reset = RowStoreTestEnvironment.getInstance().getBean(PgRowStore.class).resetInterruptedLoads();

            assertThat(reset).contains(processing.datasetId).doesNotContain(accepted.datasetId);
            assertThat(status(processing.infoUrl)).isEqualTo(ETL_STATUS_ERROR);
            assertThat(status(accepted.infoUrl)).isEqualTo(ETL_STATUS_ACCEPTED_DATA);
        } finally {
            setStatus(processing.datasetId, ETL_STATUS_AVAILABLE);
            setStatus(accepted.datasetId, ETL_STATUS_AVAILABLE);
            deleteDataset(processing.datasetUrl);
            deleteDataset(accepted.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-018: Uploads via different spellings of a dataset ID and via an alias are applied in submission order")
    void uploadsViaDifferentIdSpellingsAndAlias_areAppliedInSubmissionOrder() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("X", 1));
        String alias = "etlorder" + urls.datasetId.replace("-", "").substring(0, 12);
        try {
            given().contentType(ContentType.JSON).body("[\"" + alias + "\"]")
                    .put(urls.datasetUrl + "/aliases").then().statusCode(204);
            String upperCaseUrl = urls.datasetUrl.replace(urls.datasetId, urls.datasetId.toUpperCase());
            String aliasUrl = urls.datasetUrl.replace(urls.datasetId, alias);

            int replaceRows = 20000;
            given().spec(csvSpec).body(csv("A", replaceRows)).put(urls.datasetUrl).then().statusCode(202);
            // the response keeps the requested spelling of the ID
            given().spec(csvSpec).body(csv("B", 2)).post(upperCaseUrl).then().statusCode(202)
                    .body("id", equalTo(urls.datasetId.toUpperCase()));
            given().spec(csvSpec).body(csv("C", 2)).post(aliasUrl).then().statusCode(202);

            await().atMost(MAX_WAIT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
                assertThat(resultCount(urls.datasetUrl)).isEqualTo(replaceRows + 4);
                assertThat(status(urls.infoUrl)).isEqualTo(ETL_STATUS_AVAILABLE);
            });
            assertThat(names(urls.datasetUrl, replaceRows - 1)).containsExactly("A" + replaceRows, "B1", "B2", "C1", "C2");
        } finally {
            awaitLoadsFinishedAndDelete(urls);
        }
    }

    /**
     * Deletes a dataset once it is deletable, so that a failure of the test is not masked by a failing cleanup.
     */
    private void awaitLoadsFinishedAndDelete(DatasetUrls urls) throws SQLException {
        try {
            await().atMost(OUTAGE_MAX_WAIT).pollInterval(POLL_INTERVAL).until(() -> {
                int status = given().spec(jsonSpec).get(urls.infoUrl).then().extract().jsonPath().getInt("status");
                return status == ETL_STATUS_AVAILABLE || status == ETL_STATUS_ERROR;
            });
        } catch (ConditionTimeoutException e) {
            // stuck in a status that cannot be deleted, which the test itself reports
            setStatus(urls.datasetId, ETL_STATUS_ERROR);
        }
        deleteDataset(urls.datasetUrl);
    }

    private static byte[] csv(String prefix, int rows) {
        StringBuilder sb = new StringBuilder("name,value\n");
        for (int i = 1; i <= rows; i++) {
            sb.append(prefix).append(i).append(',').append(i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> names(String datasetUrl, int offset) {
        return given().spec(jsonSpec)
                .queryParam("_offset", offset)
                .queryParam("_limit", 100)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results.name", String.class);
    }

    private static int status(String infoUrl) {
        return given().spec(jsonSpec).get(infoUrl)
                .then().statusCode(200)
                .extract().jsonPath().getInt("status");
    }

    private static long resultCount(String datasetUrl) {
        Response response = given().spec(jsonSpec).queryParam("_limit", 1).get(datasetUrl);
        response.then().statusCode(200);
        return response.jsonPath().getLong("resultCount");
    }

    private static void setStatus(String datasetId, int status) throws SQLException {
        try (Connection conn = openDatabaseConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE datasets SET status = ? WHERE id = ?::uuid")) {
            ps.setInt(1, status);
            ps.setString(2, datasetId);
            assertThat(ps.executeUpdate()).isEqualTo(1);
        }
    }

    private static String queryString(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            return rs.getString(1);
        }
    }

    /**
     * @return The PID of the backend whose TRUNCATE of the data table waits for a lock, or 0 if there is none.
     */
    private static int blockedTruncatePid(Connection conn, String dataTable) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() " +
                        "AND wait_event_type = 'Lock' AND query = ?")) {
            ps.setString(1, "TRUNCATE " + dataTable);
            List<Integer> pids = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pids.add(rs.getInt(1));
                }
            }
            return pids.isEmpty() ? 0 : pids.get(0);
        }
    }

}
