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
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Shared static utility methods for RowStore integration tests.
 *
 * Centralizes common operations (dataset creation, polling, cleanup) so that
 * both {@link BaseIntegrationTest} and {@link ConfigurableTestBase} can
 * delegate here without duplicating logic.
 */
public final class TestUtils {

    public static final int ETL_STATUS_CREATED = 0;
    public static final int ETL_STATUS_ACCEPTED_DATA = 1;
    public static final int ETL_STATUS_PROCESSING = 2;
    public static final int ETL_STATUS_AVAILABLE = 3;
    public static final int ETL_STATUS_ERROR = 4;

    public static final Duration POLL_INTERVAL = Duration.ofMillis(2500);
    public static final Duration MAX_WAIT = Duration.ofSeconds(15);

    private TestUtils() {}

    /**
     * Loads a test data file from the classpath.
     */
    public static byte[] loadTestData(String filename, Class<?> contextClass) {
        try (InputStream is = contextClass.getResourceAsStream("/data/" + filename)) {
            if (is == null) {
                throw new RuntimeException("Test data file not found: " + filename);
            }
            return is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test data: " + filename, e);
        }
    }

    /**
     * Loads a test data file and returns it as a String.
     */
    public static String loadTestDataAsString(String filename, Class<?> contextClass) {
        return new String(loadTestData(filename, contextClass), StandardCharsets.UTF_8);
    }

    /**
     * Creates a new dataset from CSV data.
     */
    public static Response createDataset(byte[] csvData, RequestSpecification csvSpec) {
        return given()
                .spec(csvSpec)
                .body(csvData)
                .when()
                .post("/datasets");
    }

    /**
     * Extracts the dataset URL from a creation response.
     */
    public static String getDatasetUrl(Response response) {
        return response.jsonPath().getString("url");
    }

    /**
     * Extracts the info URL from a creation response.
     */
    public static String getInfoUrl(Response response) {
        return response.jsonPath().getString("info");
    }

    /**
     * Waits for a dataset to reach the AVAILABLE status.
     */
    public static void waitForDatasetAvailable(String infoUrl, RequestSpecification jsonSpec) {
        waitForStatus(infoUrl, ETL_STATUS_AVAILABLE, jsonSpec);
    }

    /**
     * Waits for a dataset to reach the ERROR status.
     */
    public static void waitForDatasetError(String infoUrl, RequestSpecification jsonSpec) {
        waitForStatus(infoUrl, ETL_STATUS_ERROR, jsonSpec);
    }

    /**
     * Waits for a dataset to reach a specific status.
     */
    public static void waitForStatus(String infoUrl, int expectedStatus, RequestSpecification jsonSpec) {
        await()
                .atMost(MAX_WAIT)
                .pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        given()
                                .spec(jsonSpec)
                                .get(infoUrl)
                                .then()
                                .body("status", equalTo(expectedStatus))
                );
    }

    /**
     * Creates a new dataset and waits for it to become available.
     */
    public static DatasetUrls createDatasetAndWait(byte[] csvData,
                                                    RequestSpecification csvSpec,
                                                    RequestSpecification jsonSpec) {
        Response response = createDataset(csvData, csvSpec);

        response.then()
                .statusCode(202)
                .contentType(ContentType.JSON);

        String datasetUrl = getDatasetUrl(response);
        String infoUrl = getInfoUrl(response);
        String datasetId = response.jsonPath().getString("id");

        waitForDatasetAvailable(infoUrl, jsonSpec);

        return new DatasetUrls(datasetUrl, infoUrl, datasetId);
    }

    /**
     * Deletes a dataset and verifies it returns 204 status.
     */
    public static void deleteDataset(String datasetUrl) {
        given()
                .delete(datasetUrl)
                .then()
                .statusCode(204);
    }

    /**
     * Deletes a dataset, verifying both the 204 status and that the dataset
     * is no longer accessible (returns 404).
     */
    public static void deleteDatasetAndVerify(String datasetUrl, RequestSpecification jsonSpec) {
        given()
                .delete(datasetUrl)
                .then()
                .statusCode(204);

        given()
                .spec(jsonSpec)
                .get(datasetUrl + "/info")
                .then()
                .statusCode(404);
    }

    /**
     * Verifies that a JSON response contains expected fields.
     */
    public static void assertJsonContainsFields(Response response, String... expectedFields) {
        for (String field : expectedFields) {
            Object value = response.jsonPath().get(field);
            assertThat(value)
                    .as("Response should contain field: " + field)
                    .isNotNull();
        }
    }

    /**
     * Gets the current dataset count from the status endpoint.
     */
    public static int getDatasetCount(RequestSpecification jsonSpec) {
        return given()
                .spec(jsonSpec)
                .get("/status")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getInt("datasets");
    }

    /**
     * Gets the list of all datasets.
     */
    @SuppressWarnings("unchecked")
    public static List<String> getDatasetsList(RequestSpecification jsonSpec) {
        return given()
                .spec(jsonSpec)
                .get("/datasets")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getList("");
    }

    /**
     * Looks up the name of a dataset's data table directly in the database.
     */
    public static String getDataTableName(Connection db, String datasetId) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("SELECT data_table FROM datasets WHERE id = ?::uuid")) {
            ps.setString(1, datasetId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("Dataset %s should exist in the database", datasetId).isTrue();
                return rs.getString(1).trim();
            }
        }
    }

    /**
     * Renames a table to simulate a database failure on access: {@code hidden=true} makes the
     * table unavailable under its name, {@code hidden=false} restores it.
     */
    public static void setTableHidden(Connection db, String table, boolean hidden) throws SQLException {
        String from = hidden ? table : table + "_hidden";
        String to = hidden ? table + "_hidden" : table;
        try (Statement st = db.createStatement()) {
            st.execute("ALTER TABLE " + from + " RENAME TO " + to);
        }
    }

    /**
     * Counts database backends (other than the calling one) whose current or last
     * query references the given table. Used to detect leaked connections.
     */
    public static int countBackendsReferencing(Connection db, String table) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE ?")) {
            ps.setString(1, "%" + table + "%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    /**
     * Sends a request with the request line exactly as given, e.g., with characters in the query string that HTTP
     * clients would encode or reject (REST Assured and java.net.URI encode or refuse "^", "|", "[" or "%zz").
     * Uses HTTP/1.0, so that the response is not chunked and ends when the connection is closed.
     *
     * @param baseUrl The base URL of the server, e.g., "http://localhost:8282/".
     * @param pathAndQuery The raw path and query string, e.g., "/dataset/x?Name=^M".
     * @param headers Header lines, e.g., "Content-Type: text/csv".
     * @param body The request body, or null.
     */
    public static RawResponse rawRequest(String baseUrl, String method, String pathAndQuery, List<String> headers,
                                         byte[] body) {
        URI base = URI.create(baseUrl);
        int port = base.getPort() > 0 ? base.getPort() : 80;
        try (Socket socket = new Socket(base.getHost(), port)) {
            socket.setSoTimeout(30000);
            StringBuilder head = new StringBuilder();
            head.append(method).append(' ').append(pathAndQuery).append(" HTTP/1.0\r\n");
            head.append("Host: ").append(base.getHost()).append(':').append(port).append("\r\n");
            for (String header : headers) {
                head.append(header).append("\r\n");
            }
            if (body != null) {
                head.append("Content-Length: ").append(body.length).append("\r\n");
            }
            head.append("\r\n");
            OutputStream out = socket.getOutputStream();
            out.write(head.toString().getBytes(StandardCharsets.ISO_8859_1));
            if (body != null) {
                out.write(body);
            }
            out.flush();

            byte[] response = socket.getInputStream().readAllBytes();
            String text = new String(response, StandardCharsets.UTF_8);
            int headerEnd = text.indexOf("\r\n\r\n");
            String[] lines = text.substring(0, headerEnd).split("\r\n");
            int status = Integer.parseInt(lines[0].split(" ")[1]);
            Map<String, String> responseHeaders = new LinkedHashMap<>();
            for (int i = 1; i < lines.length; i++) {
                int colon = lines[i].indexOf(':');
                responseHeaders.put(lines[i].substring(0, colon).trim().toLowerCase(), lines[i].substring(colon + 1).trim());
            }
            return new RawResponse(status, responseHeaders, text.substring(headerEnd + 4));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static RawResponse rawGet(String baseUrl, String pathAndQuery, String... headers) {
        return rawRequest(baseUrl, "GET", pathAndQuery, List.of(headers), null);
    }

    /**
     * A response of {@link #rawRequest(String, String, String, List, byte[])}, with header names in lower case.
     */
    public record RawResponse(int status, Map<String, String> headers, String body) {

        public String header(String name) {
            return headers.get(name.toLowerCase());
        }

        public JsonPath json() {
            return new JsonPath(body);
        }

    }

    /**
     * Container class for dataset URLs returned after creation.
     */
    public static class DatasetUrls {
        public final String datasetUrl;
        public final String infoUrl;
        public final String datasetId;

        public DatasetUrls(String datasetUrl, String infoUrl, String datasetId) {
            this.datasetUrl = datasetUrl;
            this.infoUrl = infoUrl;
            this.datasetId = datasetId;
        }
    }
}
