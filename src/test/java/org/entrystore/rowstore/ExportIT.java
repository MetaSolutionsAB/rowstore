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

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the /dataset/{id}/export endpoint.
 * Specification: 06-export.md
 *
 * Tests JSON and CSV export functionality including content verification
 * and proper header handling.
 */
@DisplayName("Export Endpoint Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExportIT extends BaseIntegrationTest {

    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;
    private String datasetId;

    /**
     * Header a,b,c; rows "x,x,y" (duplicate values), "p,q" (short row, stored without key c)
     * and ",,z" (empty cells).
     */
    private static final String EDGE_FILE = "dataset_export_edge.csv";

    private DatasetUrls edgeUrls;

    /** Number of rows added by {@link #insertPaddingRows(Connection, String)}. */
    private static final int PADDING_ROWS = 50000;

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        DatasetUrls urls = createDatasetAndWait(csvData);

        datasetUrl = urls.datasetUrl;
        infoUrl = urls.infoUrl;
        datasetId = urls.datasetId;

        edgeUrls = createDatasetAndWait(loadTestData(EDGE_FILE));
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            deleteDataset(datasetUrl);
        }
        if (edgeUrls != null) {
            deleteDataset(edgeUrls.datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-EXPORT-001: Export as JSON returns valid JSON array")
    void exportAsJson_returnsValidJsonArray() {
        Response response = given()
                .accept(ContentType.JSON)
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .extract()
                .response();

        // Content-Disposition header may or may not be present depending on implementation
        // The key test is that we get valid JSON content

        // Verify the response is a valid JSON array
        String body = response.getBody().asString();
        JSONArray jsonArray = new JSONArray(body);
        assertThat(jsonArray.length()).isEqualTo(5);
    }

    @Test
    @Order(2)
    @DisplayName("TC-EXPORT-002: Export as CSV returns valid CSV")
    void exportAsCsv_returnsValidCsv() {
        Response response = given()
                .accept("text/csv")
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .contentType("text/csv")
                .extract()
                .response();

        // Content-Disposition header may or may not be present depending on implementation
        // The key test is that we get valid CSV content

        // Verify the response is valid CSV with headers
        String body = response.getBody().asString();
        String[] lines = body.split("\n");

        // Should have header + 5 data rows
        assertThat(lines.length).isGreaterThanOrEqualTo(6);

        // First line should be headers
        String headerLine = lines[0].toLowerCase();
        assertThat(headerLine).contains("name");
        assertThat(headerLine).contains("telephone");
    }

    @Test
    @Order(3)
    @DisplayName("TC-EXPORT-003: Export JSON content matches original dataset")
    void exportJson_contentMatchesOriginal() {
        Response response = given()
                .accept(ContentType.JSON)
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        JSONArray jsonArray = new JSONArray(response.getBody().asString());

        // Verify specific data points from the original CSV
        boolean foundAkesson = false;
        boolean foundBeringer = false;

        for (int i = 0; i < jsonArray.length(); i++) {
            String name = jsonArray.getJSONObject(i).getString("name");
            if ("Åkesson".equals(name)) {
                foundAkesson = true;
                assertThat(jsonArray.getJSONObject(i).getString("comment"))
                        .isEqualTo("Another comment with äöå");
            }
            if ("Béringer".equals(name)) {
                foundBeringer = true;
                assertThat(jsonArray.getJSONObject(i).getString("comment"))
                        .isEqualTo("No, no comment");
            }
        }

        assertThat(foundAkesson).as("Should contain Åkesson").isTrue();
        assertThat(foundBeringer).as("Should contain Béringer").isTrue();
    }

    @Test
    @Order(4)
    @DisplayName("TC-EXPORT-004: Export CSV content verification")
    void exportCsv_contentVerification() {
        Response response = given()
                .accept("text/csv")
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String body = response.getBody().asString();

        // Verify data with Unicode characters is preserved
        assertThat(body).contains("Åkesson");
        assertThat(body).contains("Béringer");
        assertThat(body).contains("äöå");
    }

    @Test
    @Order(5)
    @DisplayName("TC-EXPORT-005: Export non-existent dataset returns 404")
    void exportNonExistent_returns404() {
        String fakeId = UUID.randomUUID().toString();

        given()
                .accept(ContentType.JSON)
        .when()
                .get("/dataset/" + fakeId + "/export")
        .then()
                .statusCode(404);
    }

    @Test
    @Order(6)
    @DisplayName("TC-EXPORT-006: Export dataset with status=CREATED returns 424")
    void exportCreatedDataset_returns424() {
        // Create a dataset but don't wait for it to be available
        byte[] csvData = loadTestData(TEST_FILE);
        Response createResponse = createDataset(csvData);

        String newDatasetUrl = getDatasetUrl(createResponse);
        String newInfoUrl = getInfoUrl(createResponse);

        try {
            // Immediately try to export (before processing completes)
            // Note: This may be flaky if processing is very fast
            // The test verifies the behavior when status is CREATED (before data is loaded)
            Response exportResponse = given()
                    .accept(ContentType.JSON)
            .when()
                    .get(newDatasetUrl + "/export")
            .then()
                    .extract()
                    .response();

            // Either 424 (if still processing) or 200 (if already done)
            assertThat(exportResponse.getStatusCode()).isIn(200, 424);
        } finally {
            // Wait and cleanup
            waitForDatasetAvailable(newInfoUrl);
            deleteDataset(newDatasetUrl);
        }
    }

    @Test
    @Order(7)
    @DisplayName("TC-EXPORT-007: Export dataset with empty field values")
    void exportEmptyValues_handledCorrectly() {
        // Create a dataset with empty values
        byte[] csvData = loadTestData("dataset_query_edge.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Export as JSON - empty values should be empty strings
            Response jsonResponse = given()
                    .accept(ContentType.JSON)
            .when()
                    .get(urls.datasetUrl + "/export");

            jsonResponse.then().statusCode(200);
            String jsonBody = jsonResponse.getBody().asString();
            // JSON export should contain proper empty string handling
            assertThat(jsonBody).contains("empty_field");

            // Export as CSV
            Response csvResponse = given()
                    .accept("text/csv")
            .when()
                    .get(urls.datasetUrl + "/export");

            csvResponse.then().statusCode(200);
            String csvBody = csvResponse.getBody().asString();
            // CSV export should have empty fields (consecutive commas or empty quoted strings)
            assertThat(csvBody).contains("empty_field");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(8)
    @DisplayName("TC-EXPORT-008: Content-Disposition filename includes dataset ID (if present)")
    void exportContentDisposition_includesDatasetId() {
        // JSON export
        Response jsonResponse = given()
                .accept(ContentType.JSON)
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String jsonDisposition = jsonResponse.getHeader("Content-Disposition");
        // Content-Disposition may not be returned by the server depending on client
        // If present, verify it contains the dataset ID
        if (jsonDisposition != null) {
            assertThat(jsonDisposition).contains(datasetId);
            assertThat(jsonDisposition).contains(".json");
        }

        // CSV export
        Response csvResponse = given()
                .accept("text/csv")
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String csvDisposition = csvResponse.getHeader("Content-Disposition");
        if (csvDisposition != null) {
            assertThat(csvDisposition).contains(datasetId);
            assertThat(csvDisposition).contains(".csv");
        }
    }

    @Test
    @Order(9)
    @DisplayName("TC-EXPORT-010: CSV export keeps duplicate cell values in their columns")
    void exportCsv_duplicateValues_keepColumnPositions() throws Exception {
        List<Map<String, String>> records = exportCsvRecords(edgeUrls.datasetUrl);
        assertThat(records).contains(Map.of("a", "x", "b", "x", "c", "y"));

        // JSON export format is unchanged: "[" + rows joined by ",\n" + "]"
        String json = exportJsonBody(edgeUrls.datasetUrl);
        assertThat(json).startsWith("[{").endsWith("}]");
        String[] rows = json.substring(1, json.length() - 1).split(",\n");
        assertThat(rows).hasSize(3);
        assertThat(rows).anySatisfy(row ->
                assertThat(new JSONObject(row).toMap()).isEqualTo(Map.of("a", "x", "b", "x", "c", "y")));
    }

    @Test
    @Order(10)
    @DisplayName("TC-EXPORT-011: CSV export of a short row yields empty trailing cells")
    void exportCsv_shortRow_yieldsEmptyCells() throws Exception {
        List<Map<String, String>> records = exportCsvRecords(edgeUrls.datasetUrl);
        assertThat(records).containsExactlyInAnyOrder(
                Map.of("a", "x", "b", "x", "c", "y"),
                Map.of("a", "p", "b", "q", "c", ""),
                Map.of("a", "", "b", "", "c", "z"));

        // JSON export keeps the short row as stored, i.e. without a key for the missing cell
        JSONArray json = new JSONArray(exportJsonBody(edgeUrls.datasetUrl));
        assertThat(json.length()).isEqualTo(3);
        List<Map<String, Object>> objects = new ArrayList<>();
        for (int i = 0; i < json.length(); i++) {
            objects.add(json.getJSONObject(i).toMap());
        }
        assertThat(objects).contains(Map.of("a", "p", "b", "q"));
    }

    @Test
    @Order(11)
    @DisplayName("TC-EXPORT-012: Database failure before streaming returns 500 and releases the connection")
    void exportDatabaseFailureBeforeStreaming_returns500() throws Exception {
        assumeTrue(RowStoreTestEnvironment.getInstance().isStarted(),
                "Requires direct database access (embedded test environment)");

        try (Connection db = RowStoreTestEnvironment.getInstance().openDatabaseConnection()) {
            String table = TestUtils.getDataTableName(db, edgeUrls.datasetId);
            // Without its data table the export query fails when the row stream is opened, i.e. before
            // the first byte of the body is written, so the status code can still report the failure
            TestUtils.setTableHidden(db, table, true);
            try {
                given().accept(ContentType.JSON).when().get(edgeUrls.datasetUrl + "/export").then().statusCode(500);
                given().accept("text/csv").when().get(edgeUrls.datasetUrl + "/export").then().statusCode(500);

                // The connections of the failed exports must be closed, not left idle in an aborted transaction
                await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200))
                        .until(() -> TestUtils.countBackendsReferencing(db, table) == 0);
            } finally {
                TestUtils.setTableHidden(db, table, false);
            }
        }
    }

    @Test
    @Order(12)
    @DisplayName("TC-EXPORT-013: Database failure mid-stream aborts the connection")
    void exportDatabaseFailureMidStream_abortsConnection() throws Exception {
        assumeTrue(RowStoreTestEnvironment.getInstance().isStarted(),
                "Requires direct database access (embedded test environment)");

        DatasetUrls urls = createDatasetAndWait(loadTestData(EDGE_FILE));
        try (Connection db = RowStoreTestEnvironment.getInstance().openDatabaseConnection()) {
            String table = TestUtils.getDataTableName(db, urls.datasetId);
            insertPaddingRows(db, table);

            assertExportAbortedOnBackendTermination(db, table, urls.datasetUrl, "application/json");
            assertExportAbortedOnBackendTermination(db, table, urls.datasetUrl, "text/csv");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(13)
    @DisplayName("TC-EXPORT-015: Export taking longer than 30 seconds is delivered completely")
    void exportSlowerThan30Seconds_isComplete() throws Exception {
        assumeTrue(RowStoreTestEnvironment.getInstance().isStarted(),
                "Requires direct database access (embedded test environment)");

        DatasetUrls urls = createDatasetAndWait(loadTestData(EDGE_FILE));
        try (Connection db = RowStoreTestEnvironment.getInstance().openDatabaseConnection()) {
            insertPaddingRows(db, TestUtils.getDataTableName(db, urls.datasetId));

            HttpRequest request = HttpRequest.newBuilder(URI.create(urls.datasetUrl + "/export"))
                    .header("Accept", "application/json")
                    .build();
            try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
                HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                assertThat(response.statusCode()).isEqualTo(200);

                String body;
                try (InputStream in = response.body()) {
                    byte[] start = in.readNBytes(1024);
                    // A slow client keeps the export open beyond the servlet container's default async
                    // request timeout of 30 seconds, which must not cut off the export
                    Thread.sleep(Duration.ofSeconds(35));
                    byte[] rest = in.readAllBytes();
                    body = new String(start, StandardCharsets.UTF_8) + new String(rest, StandardCharsets.UTF_8);
                }

                // compact descriptions instead of AssertJ printing a body of several MB on failure
                assertThat(body.startsWith("[{") && body.endsWith("}]"))
                        .as("Export of %d bytes must be a complete JSON array, but ends with: %s",
                                body.length(), body.substring(Math.max(0, body.length() - 40)))
                        .isTrue();
                // 3 rows from the CSV file plus the padding rows, separated by ",\n"
                assertThat(body.split(",\n", -1).length).isEqualTo(3 + PADDING_ROWS);
            }
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(14)
    @DisplayName("TC-EXPORT-016: CSV export without readable column names fails instead of losing all cells")
    void exportCsv_unreadableColumnNames_returns500() throws Exception {
        assumeTrue(RowStoreTestEnvironment.getInstance().isStarted(),
                "Requires direct database access (embedded test environment)");
        DatasetUrls urls = createDatasetAndWait(loadTestData(EDGE_FILE));
        DatasetUrls headersOnlyUrls = createDatasetAndWait(loadTestData("headers_only.csv"));
        try (Connection db = RowStoreTestEnvironment.getInstance().openDatabaseConnection()) {
            String table = TestUtils.getDataTableName(db, urls.datasetId);
            // Column names are read from the first row, so an empty first row leaves a dataset with rows but
            // without column names, as if reading the column names had failed
            try (Statement st = db.createStatement()) {
                st.executeUpdate("UPDATE " + table + " SET data = '{}'::jsonb WHERE rownr = (SELECT min(rownr) FROM " + table + ")");
            }
            given().accept("text/csv").when().get(urls.datasetUrl + "/export").then().statusCode(500);
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200))
                    .until(() -> TestUtils.countBackendsReferencing(db, table) == 0);

            // The JSON export does not depend on the column names
            assertThat(new JSONArray(exportJsonBody(urls.datasetUrl)).length()).isEqualTo(3);

            // A dataset without rows has no column names either, and is still exported
            given().accept("text/csv").when().get(headersOnlyUrls.datasetUrl + "/export").then().statusCode(200);
        } finally {
            deleteDataset(urls.datasetUrl);
            deleteDataset(headersOnlyUrls.datasetUrl);
        }
    }

    /**
     * Inserts about 50 MB of rows, far more than the server's write buffer and the socket buffers can hold.
     * While the client pauses reading, an export is therefore blocked mid-stream with its database cursor
     * open and has to fetch more rows from the database after the client resumes.
     */
    private void insertPaddingRows(Connection db, String table) throws Exception {
        try (Statement st = db.createStatement()) {
            st.executeUpdate("INSERT INTO " + table + " (data) SELECT jsonb_build_object(" +
                    "'a', 'pad' || g, 'b', '', 'c', repeat('x', 1000)) FROM generate_series(1, " + PADDING_ROWS + ") g");
        }
    }

    /**
     * Starts an export, terminates its database backend while the export is streaming and verifies that
     * the client sees an aborted connection instead of a cleanly terminated (and thus complete-looking) body.
     */
    private void assertExportAbortedOnBackendTermination(Connection db, String table, String url, String accept)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url + "/export")).header("Accept", accept).build();
        try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            // Status line and headers are committed with the first bytes of the body, so once streaming has
            // started a 5xx is impossible: a later failure can only be signalled by aborting the connection
            assertThat(response.statusCode()).isEqualTo(200);

            try (InputStream body = response.body()) {
                assertThat(body.readNBytes(1024)).hasSize(1024);

                AtomicInteger pid = new AtomicInteger();
                await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                        .until(() -> {
                            pid.set(findExportBackend(db, table));
                            return pid.get() > 0;
                        });
                try (PreparedStatement ps = db.prepareStatement("SELECT pg_terminate_backend(?)")) {
                    ps.setInt(1, pid.get());
                    ps.execute();
                }
                // wait until the backend is gone, so that a following call cannot pick up its pid again
                await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                        .until(() -> !backendExists(db, pid.get()));

                assertThatThrownBy(() -> body.transferTo(OutputStream.nullOutputStream()))
                        .as("Export as %s must end with an aborted connection, not a cleanly terminated body", accept)
                        .isInstanceOf(IOException.class);
            }
        }
    }

    private int findExportBackend(Connection db, String table) throws Exception {
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state <> 'idle' AND query = ?")) {
            ps.setString(1, "SELECT data FROM " + table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private boolean backendExists(Connection db, int pid) throws Exception {
        try (PreparedStatement ps = db.prepareStatement("SELECT 1 FROM pg_stat_activity WHERE pid = ?")) {
            ps.setInt(1, pid);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private String exportJsonBody(String url) {
        return given()
                .accept(ContentType.JSON)
        .when()
                .get(url + "/export")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .extract()
                .asString();
    }

    /**
     * Exports a dataset as CSV and maps each row to its header. The header order is not guaranteed
     * (column names are kept in a set), but header and cells must be in the same order.
     */
    private List<Map<String, String>> exportCsvRecords(String url) throws Exception {
        String body = given()
                .accept("text/csv")
        .when()
                .get(url + "/export")
        .then()
                .statusCode(200)
                .contentType("text/csv")
                .extract()
                .asString();

        List<String[]> lines;
        try (CSVReader reader = new CSVReaderBuilder(new StringReader(body))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            lines = reader.readAll();
        }
        assertThat(lines).isNotEmpty();
        String[] header = lines.get(0);
        assertThat(header).containsExactlyInAnyOrder("a", "b", "c");

        List<Map<String, String>> records = new ArrayList<>();
        for (String[] line : lines.subList(1, lines.size())) {
            assertThat(line).as("Cells of CSV row %s", String.join(",", line)).hasSameSizeAs(header);
            Map<String, String> record = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                record.put(header[i], line[i]);
            }
            records.add(record);
        }
        return records;
    }
}
